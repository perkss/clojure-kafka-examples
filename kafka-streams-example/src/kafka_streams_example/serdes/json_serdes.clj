(ns kafka-streams-example.serdes.json-serdes
  (:require [clojure.data.json :as json])
  (:import java.nio.charset.StandardCharsets
           [org.apache.kafka.common.serialization Deserializer Serdes Serializer]))

(defn bytes-to-string
  [data]
  (String. data StandardCharsets/UTF_8))

(defn string-to-bytes
  [data]
  (.getBytes ^String data StandardCharsets/UTF_8))

(def json-serializer
  (reify
    Serializer
    (close [_])
    (configure [_ _ _])
    (serialize [_ _ data]
      (when data
        (-> (json/write-str data)
            (string-to-bytes))))))

(def json-deserializer
  (reify
    Deserializer
    (close [_])
    (configure [_ _ _])
    (deserialize [_ _topic data]
      (when data
        (-> (bytes-to-string data)
            (json/read-str :key-fn keyword))))))

(def json-serde
  (Serdes/serdeFrom json-serializer json-deserializer))