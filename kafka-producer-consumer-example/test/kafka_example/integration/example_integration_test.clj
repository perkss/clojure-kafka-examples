(ns kafka-example.integration.example-integration-test
  (:require [clojure.test :refer :all]
            [kafka-example.core :refer :all])
  (:import (org.testcontainers.containers KafkaContainer)
           (org.apache.kafka.clients.producer ProducerRecord)))

(deftest example-kafka-integration-test
  (testing "Fire up test containers Kafka and then send and consume message"
    (let
      [kafka-container (KafkaContainer. "5.4.1")
       _ (.start kafka-container)
       bootstrap-server (.getBootstrapServers kafka-container)
       test-producer (build-producer bootstrap-server)
       _ (future (run-application bootstrap-server))        ; execute application in separate thread
       producer-topic "example-consumer-topic"
       test-consumer (build-consumer "example-produced-topic" bootstrap-server)
       input-data "hello"
       sent-result (.get (.send test-producer (ProducerRecord. producer-topic input-data)))
       records (.poll test-consumer 10000)]
      (is (= producer-topic (.topic sent-result)))
      (doseq [record records]
        (is (= (format "Processed Value: %s" input-data) (.value record)))))))