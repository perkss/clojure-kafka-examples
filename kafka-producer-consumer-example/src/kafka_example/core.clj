(ns kafka-example.core
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]])
  (:import  (java.util Properties)
            (org.apache.kafka.clients.consumer ConsumerConfig KafkaConsumer)
            (org.apache.kafka.common.serialization StringSerializer StringDeserializer)
            (org.apache.kafka.clients.producer KafkaProducer ProducerRecord)
            (org.apache.kafka.clients.admin AdminClient AdminClientConfig NewTopic))
  (:gen-class))

(defn create-topics!
  "Create the topic "
  [bootstrap-server topics]
  (let [config {AdminClientConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-server}
        adminClient (AdminClient/create config)]
    (.createTopics adminClient topics)))

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

  (def consumer-topic "example-consumer-topic")
  (def producer-topic "example-produced-topic")
  (def bootstrap-server (env :bootstrap-server "localhost:9092"))
  (def zookeeper-hosts (env :zookeeper-hosts "localhost:2181"))

  ;; Create the example topics
  (log/infof "Creating the topics %s" [producer-topic consumer-topic])
  (create-topics! bootstrap-server [(NewTopic. producer-topic 1 1)
                                    (NewTopic. consumer-topic 1 1)])

  (def consumer (build-consumer consumer-topic bootstrap-server))

  (def producer (build-producer bootstrap-server))

  (log/infof "Starting the kafka example app. With topic consuming topic %s and producing to %s"
           consumer-topic producer-topic)
  (while true

    (let [records (.poll consumer 100)]
      (doseq [record records]
        (log/info "Sending on value" (str "Value: " (.value record)))
        (.send producer (ProducerRecord. producer-topic (str "Value: " (.value record))))))

    (.commitAsync consumer)))
