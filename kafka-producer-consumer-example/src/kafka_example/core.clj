(ns kafka-example.core
  (:require [clojure.tools.logging :as log])
  (:import  (java.util Properties)
            (org.apache.kafka.clients.consumer ConsumerConfig KafkaConsumer)
            (org.apache.kafka.common.serialization StringSerializer StringDeserializer)
            (org.apache.kafka.clients.producer KafkaProducer ProducerRecord))
  (:gen-class))

(defn- build-consumer
  "Create the consumer instance to consume
from the provided kafka topic name"
  [consumer-topic bootstrap-server]
  (let [consumer-props
        {"bootstrap.servers", bootstrap-server
         "group.id",          "My-Group"
         "key.deserializer",  StringDeserializer
         "value.deserializer", StringDeserializer
         "auto.offset.reset", "earliest"
         "enable.auto.commit", "true"}]

    (doto (KafkaConsumer. consumer-props)
      (.subscribe [consumer-topic]))))

(defn- build-producer
  "Create the kafka producer to send on messages received"
  [bootstrap-server]
  (let [producer-props {"value.serializer" StringSerializer
                        "key.serializer" StringSerializer
                        "bootstrap.servers" bootstrap-server}]
    (KafkaProducer. producer-props)))

(defn -main
  [& args]

  (.addShutdownHook (Runtime/getRuntime) (Thread. #(log/info "Shutting down")))

  (def topic "example-topic")
  (def producer-topic "example-produced-topic")
  (def bootstrap-server "localhost:9092")

  (def consumer (build-consumer topic bootstrap-server))

  (def producer (build-producer bootstrap-server))

  (log/info "Starting the kafka example app. With topic consuming topic" topic
           "and sending to topic" producer-topic)
  (while true

    (let [records (.poll consumer 100)]
      (doseq [record records]
        (log/info "Sending on value" (str "Value: " (.value record)))
        (.send producer (ProducerRecord. producer-topic (str "Value: " (.value record))))))

    (.commitAsync consumer)))
