;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; 
;;    A set of commands that allows you to change card attributes like filters,                                ;;
;;    aggregations and special functions through Metabot.                                                      ;;
;;    Use functions from metabot_extra (bot)                                                                   ;;
;;    by: Jessika Darambaris                                                                                   ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(ns metabase.metabot_extra
  (:refer-clojure :exclude [list +])
  (:require [aleph.http :as aleph]
            [cheshire.core :as json]
            [clojure
             [edn :as edn]
             [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [manifold
             [deferred :as d]
             [stream :as s]]
            [metabase
             [pulse :as pulse]
             [util :as u]
             [events :as events]]
            [metabase.api.common :refer [*current-user-permissions-set* read-check]]      
            [metabase.api.common :refer [*current-user-permissions-set* write-check]]        
            [metabase.integrations.slack :as slack]
            [metabase.models
             [card :refer [Card]]
             [collection :refer [Collection]]
             [collection-revision :refer [CollectionRevision]]
             [table :refer [Table]]
             [field :refer [Field]]
             [interface :as mi]
             [permissions :refer [Permissions]]
             [permissions-group :as perms-group]
             [setting :as setting :refer [defsetting]]]
            [metabase.util.urls :as urls]
            [throttle.core :as throttle]
            [toucan.db :as db]
            [toucan.hydrate :refer [hydrate]]))


;---------------------------------------------- metabot info (aux functions) -------------------------------------------------------;

;; extract field names are present in the clause "grouped by" from question to show in command "info"
(defn- extract-breakouts [card]
  ;; json format  {:dataset_query {:keys values :query {:breakout card-breakouts}}}
  (let [{{{card-breakouts :breakout} :query} :dataset_query} card] 
    (apply str (interpose ", " (for [card-field (get-in card-breakouts [])] 
      (let [card-field-id (get-in card-field[1])] ;; it's necessary because of this format [["field-id" id],["field-id" id]]
        (cond
          (integer? card-field-id) 
            ;; search field name to show on Slack 
            (let [{field-name :display_name} (db/select-one [Field :display_name], :id card-field-id)] 
              (str field-name))))))))) 

;; extract table name from the question 
(defn- extract-table [card]
  ;; json format {:dataset_query {:keys values :query {:source_table card-table-id}}}
  (if-let [{{{card-table-id :source_table} :query} :dataset_query} card]
    (cond
      (integer? card-table-id)
      ;; search table name to show on Slack 
        (let [{table-name :display_name} (db/select-one [Table :display_name], :id card-table-id)]
          (str table-name)))))

;; extract graph visualization type from the question
(defn- extract-display [card]
  (let [{card-display :display} card]
    (subs (str card-display) 1))) ;; extract : of :type. Ex: :scalar -> scalar

;; extract aggregations (special functions) from question
(defn- extract-aggregations [card]
  ;; json format {:dataset_query {:keys values :query {:aggregation card-aggregation}}}    
  (let [{{{card-aggregation :aggregation} :query} :dataset_query} card]
    (let [func (get-in (get-in card-aggregation [0])[0])]
      ;; visualization depends agreggation type
      (case func  
            ;; count -> count of rows table name
            "count" (str "count of " (first (str/split (str/lower-case (extract-table card)) #"\d+")))
            "sum" (str "is sum")
            (nil?) (str "There is no aggregation here.")))))

;; string info mount to show on Slack
(defn format-info-card [card]
  (str "Here's the card infos: "
      "\n Card table: "       (extract-table card)
      "\n Card display: "     (extract-display card)
      "\n Card aggregations: "(extract-aggregations card)
      "\n Card breakouts: "   (cond
                                (empty? (extract-breakouts card)) (str "There is no field here.")
                                (string? (extract-breakouts card)) (extract-breakouts card))))

;-------------------------------------- metabot add-group-by (aux functions) ---------------------------------------------------;
;; metabot choose the new question name according to query features
(defn- new-name [new-card]
  (str (extract-aggregations new-card) " grouped by " (extract-breakouts new-card))) 

;; update into json card the new breakout and return new card
(defn update-breakout [card, field-id]
  (let [{{{card-breakouts :breakout} :query} :dataset_query} card]
    (let [card-field (get-in card-breakouts [])]
      (let [new-breakout (conj card-field (edn/read-string (format "[\"field-id\" %d]" field-id)))] 
        (update-in card [:dataset_query :query] assoc :breakout new-breakout)))))

(defn update-name 
  ([card, card-name]
    (cond 
      (empty? card-name)  (assoc-in card [:name] (new-name card))              
      (string? card-name) (assoc-in card [:name] (str card-name)))))

; ------------------------------------- common functions -----------------------------------------------------------------------;
;; return metabot collection id - select or create row with name of "metabot" within collection table  
;; questions created by metabot are stored in metabot collection 
(defn find-metabot-collection-id []
   (if-let [{collection_id :id} (db/select-one [Collection :id :name], :%lower.name [:like (str "metabot")])]
      (do   
        ;; history permissions collection 
        (db/insert! CollectionRevision 
               :before (json/generate-string {:3 {(keyword (str collection_id)) "write"}})  ;; group metabot (default number) 
               :after  (json/generate-string {:3 {(keyword (str collection_id)) "write"}})  ;; group metabot (default number)
               :user_id 1) ;; TO DO: create user metabot
        (int collection_id))
      (let [new-collection (db/insert! Collection 
          :name "metabot"
          :description "questions created by metabot"
          :color "#A989C5")]
         ;;Important: Give metabot permissions for your collection 
         (let [{collection_id :id} (hydrate new-collection :creator :can_write)] 
            (db/insert! Permissions 
              :object (format "/collection/%d/" collection_id)
              :group_id 3)) ;; group metabot (default number)
            (find-metabot-collection-id))))

;; insert new card 
(defn insert-card 
  ([{:keys [dataset_query description display name visualization_settings result_metadata]}]
  {name                   su/NonBlankString
   description            (s/maybe su/NonBlankString)
   display                su/NonBlankString
   visualization_settings su/Map
   result_metadata        (s/maybe results-metadata/ResultsMetadata)}

  (let [new-card (db/insert! Card
       :creator_id             1 ;; TO DO: create user metabot
       :dataset_query          dataset_query
       :description            description
       :display                display
       :name                   name
       :visualization_settings "{}"
       :collection_id          (find-metabot-collection-id)
       :result_metadata        result_metadata)]

      (events/publish-event! :card-create new-card)
      (hydrate new-card :creator :dashboard_count :labels :can_write :collection)))) 