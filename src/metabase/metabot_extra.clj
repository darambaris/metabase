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
            "count" (str "count of " (first (re-find #"(/S+)" (extract-table card))))
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

;; update into json card the new breakout and return new card
(defn update-breakout [card, field-id]
  (let [{{{card-breakouts :breakout} :query} :dataset_query} card]
    (let [card-field (get-in card-breakouts [])]
      (let [new-breakout (conj card-field (edn/read-string (format "[\"field-id\" %d]" field-id)))] 
        (update-in card [:dataset_query :query] assoc :breakout new-breakout)))))


; ------------------------------------- common functions -----------------------------------------------------------------------;


;; insert new card 
(defn insert-card
  ([{:keys [dataset_query description display name visualization_settings collection_id result_metadata]}]
;; {name                   su/NonBlankString
;; description            (s/maybe su/NonBlankString)
; display                su/NonBlankString
;collection_id          (s/maybe su/IntGreaterThanZero)
; visualization_settings su/Map
; result_metadata        (s/maybe results-metadata/ResultsMetadata)}

  (let [new-card (db/insert! Card
       :creator_id             1
       :dataset_query          dataset_query
       :description            description
       :display                display
       :name                   name
       :visualization_settings "{}"
       :collection_id          collection_id
       :result_metadata        result_metadata)]

      (events/publish-event! :card-create new-card)
      (hydrate new-card :creator :dashboard_count :labels :can_write :collection)))) 



;;(format-info-card {:dataset_query {:database 2, :type "query", :query {:source_table 37, :filter ["AND" [">" ["field-id" 2273] 100]], :aggregation [["count"]], :breakout [["field-id" 2293]]}}})



