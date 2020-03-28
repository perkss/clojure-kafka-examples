(ns kafka-streams-example.core
  (:require [clojure.tools.logging :as log])
  (:import
    (org.apache.kafka.common.serialization Serdes)
    (org.apache.kafka.streams StreamsConfig KafkaStreams StreamsBuilder)
    (org.apache.kafka.streams.kstream ValueMapper))
  (:gen-class))

(defn to-uppercase-topology [input-topic output-topic]
  (let [builder (StreamsBuilder.)]
    (->
      (.stream builder input-topic)                         ;; Create the source node of the stream
      (.mapValues (reify
                    ValueMapper
                    (apply [_ v]
                      (clojure.string/upper-case v))))      ;; map the strings to uppercase
      (.to output-topic))
    builder))

(defn -main
  [& args]

  (.addShutdownHook (Runtime/getRuntime) (Thread. #(log/info "Shutting down")))

  (log/info "Starting kafka stream application")

  (let [properties {StreamsConfig/APPLICATION_ID_CONFIG,            "uppercase-processing-application"
                    StreamsConfig/BOOTSTRAP_SERVERS_CONFIG,         "broker:9092"
                    StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG,   (.getName (.getClass (Serdes/String)))
                    StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG, (.getName (.getClass (Serdes/String)))}
        input-topic "plaintext-input"
        output-topic "uppercase"
        streams (KafkaStreams. (.build (to-uppercase-topology input-topic output-topic)) properties)]
    (log/info "starting stream with topology to upper case")
    (.start streams)
    (Thread/sleep (* 60000 10))
    (log/info "stopping")))