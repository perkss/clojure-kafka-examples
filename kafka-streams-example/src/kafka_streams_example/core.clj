(ns kafka-streams-example.core
  (:require [clojure.tools.logging :as log])
  (:import
   (org.apache.kafka.streams StreamsConfig KafkaStreams StreamsBuilder)
   (org.apache.kafka.common.serialization Serde Serdes Serializer)
   (org.apache.kafka.streams.kstream ValueMapper))
  (:gen-class))

(defn to-uppercase-topology []
  (let [builder (StreamsBuilder.)
        words (.stream builder "plaintext-input")]
    (.. words
        (mapValues (reify
                     ValueMapper
                     (apply [_ v]
                       (clojure.string/upper-case v))))
        (to "uppercase"))
    builder))

(defn -main
  [& args]

  (.addShutdownHook (Runtime/getRuntime) (Thread. #(log/info "Shutting down")))

  (log/info "Starting kafka stream application")

  (def properties
    {StreamsConfig/APPLICATION_ID_CONFIG, "uppercase-processing-application"
     StreamsConfig/BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
     StreamsConfig/KEY_SERDE_CLASS_CONFIG, (.getName (.getClass (Serdes/String)))
     StreamsConfig/VALUE_SERDE_CLASS_CONFIG, (.getName (.getClass (Serdes/String)))})

  (def config
    (StreamsConfig. properties))

  (def builder
    (StreamsBuilder.))

  (def input-topic "plaintext-input")
  (def output-topic "uppercase")

  (->
   (.stream builder input-topic) ;; Create the source node of the stream
   (.mapValues (reify
                 ValueMapper
                 (apply [_ v]
                   (clojure.string/upper-case v)))) ;; map the strings to uppercase
   (.to output-topic)) ;; Send the repsonse onto an output topic

  (def streams
    (KafkaStreams. (.build (to-uppercase-topology)) config))

  (log/info "starting stream with topology to upper case")
  (.start streams)
  (Thread/sleep (* 60000 10))
  (log/info "stopping"))
