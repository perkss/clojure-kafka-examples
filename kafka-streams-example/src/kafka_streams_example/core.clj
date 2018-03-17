(ns kafka-streams-example.core
  (:import  
           (org.apache.kafka.streams StreamsConfig KafkaStreams)
           (org.apache.kafka.common.serialization Serde Serdes Serializer)
           (org.apache.kafka.streams.kstream KStreamBuilder ValueMapper))
  (:gen-class))

(defn -main
  [& args]

  (.addShutdownHook (Runtime/getRuntime) (Thread. #(println "Shutting down")))
  
  (println "Starting kafka stream application")

  (def properties
    {StreamsConfig/APPLICATION_ID_CONFIG, "word-count-processing-application"
     StreamsConfig/BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
     StreamsConfig/KEY_SERDE_CLASS_CONFIG, (.getName (.getClass (Serdes/String)))
     StreamsConfig/VALUE_SERDE_CLASS_CONFIG, (.getName (.getClass (Serdes/String)))})

  ;; Create instance of StreamConfig
  (def config
    (StreamsConfig. properties))

  (def builder
    (KStreamBuilder.))

  (def input-topic
    (into-array String ["plaintext-input"]))

  (->
   (.stream builder input-topic) ;; Create the source node of the stream
 ;;  (.mapValues (reify ValueMapper (apply [_ v] (str v)) ))
   (.mapValues (reify ValueMapper (apply [_ v] (clojure.string/upper-case v)))) ;; map the strings to uppercase
   (.print))

  (def streams
    (KafkaStreams. builder config))
(prn "starting")
 (.start streams)
 (Thread/sleep (* 60000 10))
 (prn "stopping"))
