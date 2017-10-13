(ns metabase.metabot
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
             [util :as u]]
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
            [toucan.db :as db]))

(defsetting metabot-enabled
  "Enable MetaBot, which lets you search for and view your saved questions directly via Slack."
  :type    :boolean
  :default false)


;;; ------------------------------------------------------------ Perms Checking ------------------------------------------------------------

(defn- metabot-permissions
  "Return the set of permissions granted to the MetaBot."
  []
  (db/select-field :object Permissions, :group_id (u/get-id (perms-group/metabot))))

(defn do-with-metabot-permissions [f]
  (binding [*current-user-permissions-set* (delay (metabot-permissions))]
    (f)))

(defmacro ^:private with-metabot-permissions
  "Execute BODY with MetaBot's permissions bound to `*current-user-permissions-set*`."
  {:style/indent 0}
  [& body]
  `(do-with-metabot-permissions (fn [] ~@body)))


;;; # ------------------------------------------------------------ Metabot Command Handlers ------------------------------------------------------------

(def ^:private ^:dynamic *channel-id* nil)

(defn- keys-description
  ([message m]
   (str message " " (keys-description m)))
  ([m]
   (str/join ", " (sort (for [[k varr] m
                              :when    (not (:unlisted (meta varr)))]
                          (str \` (name k) \`))))))

(defn- dispatch-fn [verb tag]
  (let [fn-map (into {} (for [[symb varr] (ns-interns *ns*)
                              :let        [dispatch-token (get (meta varr) tag)]
                              :when       dispatch-token]
                          {(if (true? dispatch-token)
                             (keyword symb)
                             dispatch-token) varr}))]
    (fn dispatch*
      ([]
       (keys-description (format "Here's what I can %s:" verb) fn-map))
      ([what & args]
       (if-let [f (fn-map (keyword what))]
         (apply f args)
         (format "I don't know how to %s `%s`.\n%s"
                 verb
                 (if (instance? clojure.lang.Named what)
                   (name what)
                   what)
                 (dispatch*)))))))

(defn- format-exception
  "Format a `Throwable` the way we'd like for posting it on slack."
  [^Throwable e]
  (str "Uh oh! :cry:\n>" (.getMessage e)))

(defmacro ^:private do-async {:style/indent 0} [& body]
  `(future (try ~@body
                (catch Throwable e#
                  (log/error (u/format-color '~'red (u/filtered-stacktrace e#)))
                  (slack/post-chat-message! *channel-id* (format-exception e#))))))

;; format vector var "cards" to use in command list 
(defn format-cards ;;defn- => private function with arguments [cards]
  "Format a sequence of Cards as a nice multiline list for use in responses." ;;description
  [cards] ;;declare vector 
  ;; apply str (interpose "\n" => convert vector positions to string separated by "\n"
  (apply str (interpose "\n" (for [{id :id, card-name :name} cards]
                               (format "%d.  <%s|\"%s\">" id (urls/card-url id) card-name)))))
  ;; create 2 keys {id and card-name} format %d -> id and %s card-name


(defn ^:metabot list
  "Implementation of the `metabot list cards` command."
  [& _]
  (let [cards (with-metabot-permissions
                (filterv mi/can-read? (db/select [Card :id :name :dataset_query], {:order-by [[:id :desc]], :limit 20})))]
    (str "Here's your " (count cards) " most recent cards:\n" (format-cards cards))))

(defn- card-with-name [card-name]
  (first (u/prog1 (db/select [Card :id :name], :%lower.name [:like (str \% (str/lower-case card-name) \%)])
           (when (> (count <>) 1)
             (throw (Exception. (str "Could you be a little more specific? I found these cards with names that matched:\n"
                                     (format-cards <>))))))))

(defn id-or-name->card [card-id-or-name]
  (cond
    (integer? card-id-or-name)     (db/select-one [Card :id :name], :id card-id-or-name)
    (or (string? card-id-or-name)
        (symbol? card-id-or-name)) (card-with-name card-id-or-name)
    :else                          (throw (Exception. (format "I don't know what Card `%s` is. Give me a Card ID or name." card-id-or-name)))))


(defn ^:metabot show
  "Implementation of the `metabot show card <name-or-id>` command."
  ([]
   "Show which card? Give me a part of a card name or its ID and I can show it to you. If you don't know which card you want, try `metabot list`.")
  ([card-id-or-name] 
   (if-let [{card-id :id} (id-or-name->card card-id-or-name)]
     (do
       (with-metabot-permissions
         (read-check Card card-id))
       (do-async (let [attachments (pulse/create-and-upload-slack-attachments! [(pulse/execute-card card-id, :context :metabot)])]
                   (slack/post-chat-message! *channel-id*
                                             nil
                                             attachments)))
       "Ok, just a second...")
     (throw (Exception. "Not Found"))))
  ;; If the card name comes without spaces, e.g. (show 'my 'wacky 'card) turn it into a string an recur: (show "my wacky card")
  ([word & more]
   (show (str/join " " (cons word more)))))

;; ---------------------------------------- metabot display --------------------------------- ;;
(defn ^:metabot display
  "This function changes the visualization of a question on Metabase."
  ([]
    "Use: \n
     \"metabot display types\" - to see the graphic types available \n
     \"metabot display ID\" - to see the graphic type of the choosed question")
  ([card-id-or-list]
    (cond
      (string? card-id-or-list) (format "The graphic types available are: \n Scalar, Table, Line, Bar, Row Chart")
      (integer? card-id-or-list) 
          (let [{card-name :name, display :display} (db/select-one [Card :id :name :display], :id card-id-or-list)] 
            (format "The visualization of card with name %s is %s" card-name display)
          )))
  ([type, card-id-or-list]
  (if-let [{card-id :id} (id-or-name->card card-id-or-list)]
      (db/update! Card card-id :display (keyword type))
  (throw (Exception. "Not Found")))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; 
;;    A set of features that allows you to change card attributes like filters, 
;;    aggregations and special functions through Metabot.
;;    by: Jessika Darambaris 
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- extract-display
  [card]
  (let [{card-display :display} card]
    (format "%s" card-display))
)

(defn- extract-aggregations
  [card]
  (let [{{{card-aggregation :aggregation} :query} :dataset_query} card]
    (format "%s" (get-in (get-in card-aggregation [0])[0]))))


(defn- extract-breakouts
  [card]
  (let [{{{card-breakouts :breakout} :query} :dataset_query} card]
    (apply str (interpose ", " (for [card-field (get-in card-breakouts [])]
      (let [card-field-id (get-in card-field[1])]
        (cond
          (integer? card-field-id) 
            (let [{field-name :display_name} (db/select-one [Field :display_name], :id card-field-id)]
              (format "%s" field-name))))))))) 


(defn- extract-table
  [card]
  (if-let [{{{card-table-id :source_table} :query} :dataset_query} card]
    (cond
      (integer? card-table-id) (let [{table-name :display_name} (db/select-one [Table :display_name], :id card-table-id)]
                                  (format "%s" table-name)))))



(defn- field-with-name [field-name]
  (first (u/prog1 (db/select [Field :id :name], :%lower.name [:like (str \% (str/lower-case field-name) \%)])
           (when (> (count <>) 1)
             (throw (Exception. (str "Could you be a little more specific? I found these fields with names that matched:\n")))))))

(defn ^:metabot info 
  "This function allows see card infos like aggregations, filters and breakouts"
  ([]
    ("Show which info card? Give me a part of a card name or its ID and I can show it to you")
  )
  ([card-id-or-name]
    (if-let [{card-id :id} (id-or-name->card card-id-or-name)]
      (do (with-metabot-permissions
        (read-check Card card-id))
        (let [card (db/select-one [Card :id :name :display :result_metadata :dataset_query], :id card-id)]
          (str "Here's the card infos: "
                "\n Card table: " (extract-table card)
                "\n Card display: "(extract-display card)
                "\n Card aggregations: "(extract-aggregations card)
                "\n Card breakouts: "(extract-breakouts card)))))))


  (defn ^:metabot add group-by
    ([card-id-or-name, field-name]
      (if-let [{card-id :id} (id-or-name->card card-id-or-name)]
        (do (with-metabot-permissions
          (write-check Card card-id))
            (let [card (db/select-one [Card :id :name :display :result_metadata :dataset_query], :id card-id)]
              (if-let [{field-id :id} (field-with-name field-name)]
                  ;; TO DO: insert new register on database 
                )            
      )
  ))))

(defn update-teste 
  ([card, field-id]
  (let [{{{card-breakouts :breakout} :query} :dataset_query} card]
    (let [card-field (get-in card-breakouts [])]
      (let [new-breakout (conj card-field "[\"field-id\" 3]")] ;; GET field-id
        (let [new-card (update-in card [:dataset_query :query] assoc :breakout new-breakout)]
            (str new-card)))))))



(defn- extract_filters [result_metadata, dataset_query]
  (str dataset_query))
;;  (for [{display :display_name, special :special_type} result_metadata]
;;    (cond
 ;;     (nil? special)
  ;;      (format "" display)
  ;;    (string? special) (str special)))) 

(defn ^:metabot unlisted
  "Implementation of the `metabot show card <name-or-id>` command."
  ([]
   "Uh Oh! Don't we a question you would like to ask?")
  ([card-id-or-name]
   (if-let [{card-id :id} (id-or-name->card card-id-or-name)]
     (let [{card-name :name, display :display, result_metadata :result_metadata, dataset_query :dataset_query} 
      (db/select-one [Card :id :name :display :result_metadata :dataset_query], :id card-id-or-name)]
        (extract_filters result_metadata dataset_query)))))

(defn meme:up-and-to-the-right
  "Implementation of the `metabot meme up-and-to-the-right <title>` command."
  {:meme :up-and-to-the-right}
    [& _]
  ":chart_with_upwards_trend:")

(def ^:metabot ^:unlisted meme
  "Dispatch function for the `metabot meme` family of commands."
  (dispatch-fn "meme" :meme))


(declare apply-metabot-fn)

(defn ^:metabot help
  "Implementation of the `metabot help` command."
  [& _]
  (apply-metabot-fn))


(def ^:private kanye-quotes
  (delay (log/debug "Loading kanye quotes...")
         (when-let [data (slurp (io/reader (io/resource "kanye-quotes.edn")))]
           (edn/read-string data))))

(defn ^:metabot ^:unlisted kanye
  "Implementation of the `metabot kanye` command."
  [& _]
  (str ":kanye:\n> " (rand-nth @kanye-quotes)))


;;; # ------------------------------------------------------------ Metabot Command Dispatch ------------------------------------------------------------

(def ^:private apply-metabot-fn
  (dispatch-fn "understand" :metabot))

(defn- eval-command-str [s]
  (when (string? s)
    ;; if someone just typed "metabot" (no command) act like they typed "metabot help"
    (let [s (if (seq s)
              s
              "help")]
      (log/debug "Evaluating Metabot command:" s)
      (when-let [tokens (seq (edn/read-string (str "(" (-> s
                                                           (str/replace "“" "\"") ; replace smart quotes
                                                           (str/replace "”" "\"")) ")")))]
        (apply apply-metabot-fn tokens)))))


;;; # ------------------------------------------------------------ Metabot Input Handling ------------------------------------------------------------

(defn- message->command-str
  "Get the command portion of a message *event* directed at Metabot.

     (message->command-str {:text \"metabot list\"}) -> \"list\""
  [{:keys [text]}]
    (when (seq text)
      (str/lower-case (second (re-matches #".*(?i)mea?ta?boa?t\s*(.*)$" text))))) ; handle typos like metaboat or meatbot

(defn- respond-to-message! [message response]
  (when response
    (let [response (if (coll? response) (str "```\n" (u/pprint-to-str response) "```")
                       (str response))]
      (when (seq response)
        (slack/post-chat-message! (:channel message) response)))))

(defn- handle-slack-message [message]
  (respond-to-message! message (eval-command-str (message->command-str message))))

(defn- human-message?
  "Was this Slack WebSocket event one about a *human* sending a message?"
  [{event-type :type, subtype :subtype}]
  (and (= event-type "message")
       (not (contains? #{"bot_message" "message_changed" "message_deleted"} subtype))))

(defn- event-timestamp-ms
  "Get the UNIX timestamp of a Slack WebSocket event, in milliseconds."
  [{:keys [ts], :or {ts "0"}}]
  (* (Double/parseDouble ts) 1000))


(defonce ^:private websocket (atom nil))

(defn- handle-slack-event [socket start-time event]
  (when-not (= socket @websocket)
    (log/debug "Go home websocket, you're drunk.")
    (s/close! socket)
    (throw (Exception.)))

  (when-let [event (json/parse-string event keyword)]
    ;; Only respond to events where a *human* sends a message that have happened *after* the MetaBot launches
    (when (and (human-message? event)
               (> (event-timestamp-ms event) start-time))
      (binding [*channel-id* (:channel event)]
        (do (future (try
                      (handle-slack-message event)
                      (catch Throwable t
                        (slack/post-chat-message! *channel-id* (format-exception t)))))
            nil)))))


;;; # ------------------------------------------------------------ Websocket Connection Stuff ------------------------------------------------------------

(defn- connect-websocket! []
  (when-let [websocket-url (slack/websocket-url)]
    (let [socket @(aleph/websocket-client websocket-url)]
      (reset! websocket socket)
      (d/catch (s/consume (partial handle-slack-event socket (System/currentTimeMillis))
                          socket)
          (fn [error]
            (log/error "Error launching metabot:" error))))))

(defn- disconnect-websocket! []
  (when-let [socket @websocket]
    (reset! websocket nil)
    (when-not (s/closed? socket)
      (s/close! socket))))

;;; Websocket monitor

;; Keep track of the Thread ID of the current monitor thread. Monitor threads should check this ID
;; and if it is no longer equal to theirs they should die
(defonce ^:private websocket-monitor-thread-id (atom nil))

;; we'll use a THROTTLER to implement exponential backoff for recconenction attempts, since THROTTLERS are designed with for this sort of thing
;; e.g. after the first failed connection we'll wait 2 seconds, then each that amount increases by the `:delay-exponent` of 1.3
;; so our reconnection schedule will look something like:
;; number of consecutive failed attempts | seconds before next try (rounded up to nearest multiple of 2 seconds)
;; --------------------------------------+----------------------------------------------------------------------
;;                                    0  |   2
;;                                    1  |   4
;;                                    2  |   4
;;                                    3  |   6
;;                                    4  |   8
;;                                    5  |  14
;;                                    6  |  30
;; we'll throttle this based on values of the `slack-token` setting; that way if someone changes its value they won't have to wait
;; whatever the exponential delay is before the connection is retried
(def ^:private reconnection-attempt-throttler
  (throttle/make-throttler nil :attempts-threshold 1, :initial-delay-ms 200 , :delay-exponent 1.3))

(defn- should-attempt-to-reconnect? ^Boolean []
  (boolean (u/ignore-exceptions
             (throttle/check reconnection-attempt-throttler (slack/slack-token))
             true)))

(defn- start-websocket-monitor! []
  (future
    (reset! websocket-monitor-thread-id (.getId (Thread/currentThread)))
    ;; Every 2 seconds check to see if websocket connection is [still] open, [re-]open it if not
    (loop []
      (while (not (should-attempt-to-reconnect?))
        (Thread/sleep 200))
      (when (= (.getId (Thread/currentThread)) @websocket-monitor-thread-id)
        (try
          (when (or (not  @websocket)
                    (s/closed? @websocket))
            (log/debug "MetaBot WebSocket is closed. Reconnecting now.")
            (connect-websocket!))
          (catch Throwable e
            (log/error "Error connecting websocket:" (.getMessage e))))
        (recur)))))

(defn start-metabot!
  "Start the MetaBot! :robot_face:

   This will spin up a background thread that opens and maintains a Slack WebSocket connection."
  []
  (when (and (slack/slack-token)
             (metabot-enabled))
    (log/info "Starting MetaBot WebSocket monitor thread...")
    (start-websocket-monitor!)))

(defn stop-metabot!
  "Stop the MetaBot! :robot_face:

   This will stop the background thread that responsible for the Slack WebSocket connection."
  []
  (log/info "Stopping MetaBot...  🤖")
  (reset! websocket-monitor-thread-id nil)
  (disconnect-websocket!))

(defn restart-metabot!
  "Restart the MetaBot listening process.
   Used on settings changed"
  []
  (when @websocket-monitor-thread-id
    (log/info "MetaBot already running. Killing the previous WebSocket listener first.")
    (stop-metabot!))
  (start-metabot!))

