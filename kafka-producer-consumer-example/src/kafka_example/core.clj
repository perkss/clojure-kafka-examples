(ns kafka-example.core
  (:gen-class)
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]])
  (:import [org.apache.kafka.clients.admin AdminClient AdminClientConfig NewTopic]
           org.apache.kafka.clients.consumer.KafkaConsumer
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.common.serialization StringDeserializer StringSerializer]
           (org.apache.kafka.common TopicPartition)
           (java.time Duration)))

(defn create-topics!
  "Create the topic "
  [bootstrap-server topics ^Integer partitions ^Short replication]
  (let [config {AdminClientConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-server}
        adminClient (AdminClient/create config)
        new-topics (map (fn [^String topic-name] (NewTopic. topic-name partitions replication)) topics)]
    (.createTopics adminClient new-topics)))

(defn- pending-messages
  [end-offsets consumer]
  (some true? (doall
                (map
                  (fn [topic-partition]
                    (let [position (.position consumer (key topic-partition))
                          value (val topic-partition)]
                      (< position value)))
                  end-offsets))))

(defn search-topic-by-key
  "Searches through Kafka topic and returns those matching the key"
  [^KafkaConsumer consumer topic search-key]
  (let [topic-partitions (->> (.partitionsFor consumer topic)
                              (map #(TopicPartition. (.topic %) (.partition %))))
        _ (.assign consumer topic-partitions)
        _ (.seekToBeginning consumer (.assignment consumer))
        end-offsets (.endOffsets consumer (.assignment consumer))
        found-records (transient [])]
    (log/infof "end offsets %s" end-offsets)
    (log/infof "Pending messages? %s" (pending-messages end-offsets consumer))
    (while (pending-messages end-offsets consumer)
      (log/infof "Pending messages? %s" (pending-messages end-offsets consumer))
      (let [records (.poll consumer (Duration/ofMillis 1000))
            matched-search-key (filter #(= (.key %) search-key) records)]
        (conj! found-records matched-search-key)
        (doseq [record matched-search-key]
          (log/infof "Found Matching Key %s Value %s" (.key record) (.value record)))))
    (persistent! found-records)))

(defn build-consumer
  "Create the consumer instance to consume
from the provided kafka topic name"
  [consumer-topic bootstrap-server]
  (let [consumer-props
        {"bootstrap.servers",  bootstrap-server
         "group.id",           "example"
         "key.deserializer",   StringDeserializer
         "value.deserializer", StringDeserializer
         "auto.offset.reset",  "earliest"
         "enable.auto.commit", "true"}]
    (KafkaConsumer. consumer-props)))

(defn consumer-subscribe
  [consumer topic]
  (.subscribe consumer [topic]))

(defn build-producer ^KafkaProducer
  ;"Create the kafka producer to send on messages received"
  [bootstrap-server]
  (let [producer-props {"value.serializer"  StringSerializer
                        "key.serializer"    StringSerializer
                        "bootstrap.servers" bootstrap-server}]
    (KafkaProducer. producer-props)))

(defn run-application
  "Create the simple read and write topology with Kafka"
  [bootstrap-server]
  (let [consumer-topic "example-consumer-topic"
        producer-topic "example-produced-topic"
        bootstrap-server (env :bootstrap-server bootstrap-server)
        replay-consumer (build-consumer consumer-topic bootstrap-server)
        consumer (build-consumer consumer-topic bootstrap-server)
        producer (build-producer bootstrap-server)]
    (log/infof "Creating the topics %s" [producer-topic consumer-topic])
    (create-topics! bootstrap-server [producer-topic consumer-topic] 1 1)
    (log/infof "Starting the kafka example app. With topic consuming topic %s and producing to %s"
               consumer-topic producer-topic)
    (search-topic-by-key replay-consumer consumer-topic "1")
    (consumer-subscribe consumer consumer-topic)
    (while true
      (let [records (.poll consumer (Duration/ofMillis 100))]
        (doseq [record records]
          (log/info "Sending on value" (str "Processed Value: " (.value record)))
          (.send producer (ProducerRecord. producer-topic "a" (str "Processed Value: " (.value record))))))
      (.commitAsync consumer))))

(defn -main
  [& args]
  (.addShutdownHook (Runtime/getRuntime) (Thread. #(log/info "Shutting down")))
  (run-application "localhost:9092"))