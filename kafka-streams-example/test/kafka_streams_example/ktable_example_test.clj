(ns kafka-streams-example.ktable-example-test
  (:require [kafka-streams-example.ktable-example :as sut]
            [clojure.test :refer [deftest testing is]]
            [kafka-streams-example.test-support :as support])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams TopologyTestDriver]
           org.apache.kafka.streams.test.ConsumerRecordFactory))

(deftest kafka-streams-ktable-example-test
  (testing "Kafka Streams with KTABLE"
    (let [topology (.build (sut/build-join-topology))
          topology-test-driver (TopologyTestDriver. topology (support/properties "user-clicks-application"))
          serializer  (.serializer (. Serdes String))
          deserializer (.deserializer (. Serdes String))
          factory (ConsumerRecordFactory. serializer serializer)
          user-clicks-topic "user-clicks-topic"
          user-regions-topic "user-regions-topic"
          output-topic "clicks-per-region-topic"
          input-clicks "2"
          input-regions "England"]
      (.pipeInput topology-test-driver (.create factory user-regions-topic "alice" input-regions))
      (.pipeInput topology-test-driver (.create factory user-clicks-topic "alice" input-clicks))
      (.pipeInput topology-test-driver (.create factory user-clicks-topic "alice" input-clicks))
      (let [output (.readOutput topology-test-driver output-topic deserializer deserializer)
            output-2 (.readOutput topology-test-driver output-topic deserializer deserializer)]
        (is (= "England" (.key output)))
        (is (= "2" (.value output)))
        (is (= "England" (.key output-2)))
        (is (= "4" (.value output-2)))))))
