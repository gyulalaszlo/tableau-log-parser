(ns tableau-log-convert.core
  (:require [clojure.data.json :as json]
            [clojure.data.csv :as csv])
  (:gen-class))


(def TEST-ROW "{\"ts\":\"2015-09-24T17:29:09.051\",\"pid\":34372,\"tid\":\"637c\",\"sev\":\"info\",\"req\":\"-\",\"sess\":\"-\",\"site\":\"-\",\"user\":\"-\",\"k\":\"open-log\",\"v\":{\"path\":\"E:\\\\Tableau_Server\\\\data\\\\tabsvc\\\\vizqlserver\\\\Logs\\\\vizqlserver_1_2015_09_24_17_29_09.txt\"}} ")
(def TEST-ROW-2 "{\"ts\":\"2015-09-24T17:29:09.051\",\"pid\":34372,\"tid\":\"637c\",\"sev\":\"info\",\"req\":\"-\",\"sess\":\"-\",\"site\":\"-\",\"user\":\"-\",\"k\":\"startup-info\",\"v\":{\"tableau-version\":\"9000.15.0820.1222,x64\",\"cwd\":\"E:\\\\Tableau_Server\\\\9.0\\\\repository\\\\jre\\\\bin\",\"start-time\":\"2015-09-24T21:29:09.051\",\"os\":\"Microsoft Windows Server 2008 R2 Enterprise (Build 7601) Service Pack 1 (CSDBuild 1130)\",\"domain\":\"gis.corp.ge.com\",\"hostname\":\"CHPWPGTABP01NW\",\"process-id\":\"34372 (0x8644)\"}}")


(def SEPARATOR "__")


;; Helper to turn a nested object into a linear structure
(defn- linearize-row
  ([row] (linearize-row row nil))
  ([row base-key]
   (->> row
        (mapcat (fn [[k v]]
                  (let [new-key (if (nil? base-key) k (str base-key SEPARATOR k))]
                    (cond
                      (map? v) (linearize-row v new-key)
                      :else [[new-key v]])))))))

(defn parse-row
  "Parses a JSON object and returns a linearized representation of it for CSV purposes"
  [row]
  (->> (json/read-str row)
       (linearize-row)
       (into {})))



(defn read-tableau-log [log-file]
  (let [log-entries (with-open [rdr (clojure.java.io/reader log-file)]
                      (doall
                        (map parse-row (line-seq rdr))))
        log-keys (->> (mapcat keys log-entries)
                      (set)
                      (vec)
                      (sort))
        log-rows (mapv (fn [le] (mapv #(get le %) log-keys)) log-entries)]
    (with-open [out-file (clojure.java.io/writer (str log-file ".csv"))]
      (csv/write-csv out-file [log-keys])
      (csv/write-csv out-file log-rows))
    ))



(defn -main [f & args]
  (println "Usage: <command> <FILE>")
  (println "Processing: " f)
  (read-tableau-log f))