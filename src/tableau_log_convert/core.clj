(ns tableau-log-convert.core
  (:require [clojure.data.json :as json]
            [clojure.data.csv :as csv]
            [clojure.java.jdbc :as jdbc]

            [clj-time.format :as tf]
            [clj-time.coerce :as tc])
  (:gen-class))


(def TEST-ROW "{\"ts\":\"2015-09-24T17:29:09.051\",\"pid\":34372,\"tid\":\"637c\",\"sev\":\"info\",\"req\":\"-\",\"sess\":\"-\",\"site\":\"-\",\"user\":\"-\",\"k\":\"open-log\",\"v\":{\"path\":\"E:\\\\Tableau_Server\\\\data\\\\tabsvc\\\\vizqlserver\\\\Logs\\\\vizqlserver_1_2015_09_24_17_29_09.txt\"}} ")
(def TEST-ROW-2 "{\"ts\":\"2015-09-24T17:29:09.051\",\"pid\":34372,\"tid\":\"637c\",\"sev\":\"info\",\"req\":\"-\",\"sess\":\"-\",\"site\":\"-\",\"user\":\"-\",\"k\":\"startup-info\",\"v\":{\"tableau-version\":\"9000.15.0820.1222,x64\",\"cwd\":\"E:\\\\Tableau_Server\\\\9.0\\\\repository\\\\jre\\\\bin\",\"start-time\":\"2015-09-24T21:29:09.051\",\"os\":\"Microsoft Windows Server 2008 R2 Enterprise (Build 7601) Service Pack 1 (CSDBuild 1130)\",\"domain\":\"gis.corp.ge.com\",\"hostname\":\"CHPWPGTABP01NW\",\"process-id\":\"34372 (0x8644)\"}}")


(def SEPARATOR "__")


;; Helper to turn a nested object into a linear structure
;(defn- linearize-row
;  ([row] (linearize-row row nil))
;  ([row base-key]
;   (->> row
;        (mapcat (fn [[k v]]
;                  (let [new-key (if (nil? base-key) k (str base-key SEPARATOR k))]
;                    (cond
;                      (map? v) (linearize-row v new-key)
;                      :else [[new-key v]])))))))

(defn parse-row
  "Parses a JSON object and returns a linearized representation of it for CSV purposes"
  [row]
  (->> (json/read-str row)))
;
;(def DB {:classname   "org.postgresql.Driver"
;         :subprotocol "postgresql"
;         :subname     "//localhost:5432/tableau_logs"
;         :user        "postgres"
;         :password    "abc12def"})


(def ^:private LOG-KEYS ["pid" "tid" "sev" "req" "sess" "site"])
(def ^:private log-ts-formatter (tf/formatters :date-hour-minute-second-ms))

(defn- select-log-keys [row log-name server-num]
  (assoc (select-keys row LOG-KEYS)
    :ts (tc/to-sql-time (tf/parse log-ts-formatter (row "ts")))
    :user_name (row "user")

    :log_name log-name
    :server_num (Integer. server-num)))

(defn insert-log-row [dbspec log-name server-num log-rows]
  (let [inserted-rows (apply jdbc/insert! dbspec :logs (map #(select-log-keys % log-name server-num) log-rows))
        inserted-ids (map :pk_key inserted-rows)]

    (->> (map (fn [id log-row] [id (log-row "v")]) inserted-ids log-rows)

         (mapcat
           (fn [[id attrs]]
             (if (string? attrs)
               ;; If the value is a string, assoc it with
               [{:pk_key id
                 :key    "$content"
                 :value  attrs}]
               ;; otherwise map each pair
               (mapv (fn [[k v]] {:pk_key id
                                 :key    k
                                 :value  (if (string? v) v (pr-str v))})
                    attrs))))

         (apply jdbc/insert! dbspec :keyv ))))


(defn read-tableau-log [dbspec log-file]
  (let [[combined log-name server-num] (re-find #"^([a-zA-Z-]+)_([0-9]+)_" (org.apache.commons.io.FilenameUtils/getBaseName log-file))
        log-entries (with-open [rdr (clojure.java.io/reader log-file)]
                      (doall
                        (map parse-row (line-seq rdr))))
        log-count (count log-entries)]
    (println  "-> Finished loading -- extracting from" log-count " rows.")
    (doseq [log-entry-block (partition-all 1000 log-entries)]
      (println "Inserting " (count log-entry-block) " / " log-count " rows")
      (time
        (insert-log-row dbspec log-name server-num log-entry-block)))
    ))



(defn -main [ dbspec & args]
  (doseq [log-file args]
    (println "Processing: " log-file)
    (read-tableau-log dbspec log-file)))