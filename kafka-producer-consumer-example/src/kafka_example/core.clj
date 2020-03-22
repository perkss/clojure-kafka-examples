(ns kafka-example.core
  (:gen-class)
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]])
  (:import [org.apache.kafka.clients.admin AdminClient AdminClientConfig NewTopic]
           org.apache.kafka.clients.consumer.KafkaConsumer
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.common.serialization StringDeserializer StringSerializer]))

(defn create-topics!
  "Create the topic "
  [bootstrap-server topics partitions replication]
  (let [config {AdminClientConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-server}
        adminClient (AdminClient/create config)
        new-topics (map #(NewTopic. % partitions replication) topics)]
    (.createTopics adminClient new-topics)))

(defn build-consumer
  "Create the consumer instance to consume
from the provided kafka topic name"
  [consumer-topic bootstrap-server]
  (let [consumer-props
        {"bootstrap.servers", bootstrap-server
         "group.id",          "example"
         "key.deserializer",  StringDeserializer
         "value.deserializer", StringDeserializer
         "auto.offset.reset", "earliest"
         "enable.auto.commit", "true"}]

    (doto (KafkaConsumer. consumer-props)
      (.subscribe [consumer-topic]))))

(defn build-producer ^KafkaProducer
  ;"Create the kafka producer to send on messages received"
  [bootstrap-server]
  (let [producer-props {"value.serializer" StringSerializer
                        "key.serializer" StringSerializer
                        "bootstrap.servers" bootstrap-server}]
    (KafkaProducer. producer-props)))

(defn run-application
  "Create the simple read and write topology with Kafka"
  [bootstrap-server]
  (let [consumer-topic "example-consumer-topic"
        producer-topic "example-produced-topic"
        bootstrap-server (env :bootstrap-server bootstrap-server)
        consumer (build-consumer consumer-topic bootstrap-server)
        producer (build-producer bootstrap-server)]
    (log/infof "Creating the topics %s" [producer-topic consumer-topic])
    (create-topics! bootstrap-server [producer-topic consumer-topic] 1 1)
    (log/infof "Starting the kafka example app. With topic consuming topic %s and producing to %s"
               consumer-topic producer-topic)
    (while true
      (let [records (.poll consumer 100)]
        (doseq [record records]
          (log/info "Sending on value" (str "Processed Value: " (.value record)))
          (.send producer (ProducerRecord. producer-topic (str "Processed Value: " (.value record))))))
      (.commitAsync consumer))))

(defn -main
  [& args]
  (.addShutdownHook (Runtime/getRuntime) (Thread. #(log/info "Shutting down")))
  (run-application "localhost:9092"))