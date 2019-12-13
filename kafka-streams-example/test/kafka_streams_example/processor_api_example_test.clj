(ns kafka-streams-example.processor-api-example-test
  (:require [clojure.test :refer [deftest is testing]]
            [kafka-streams-example.processor-api-example :as sut]
            [kafka-streams-example.test-support :as support])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams TopologyTestDriver]
           org.apache.kafka.streams.test.ConsumerRecordFactory))

(deftest word-count-test
  (testing "A word count processor API test"
    (let [topology (sut/word-processor-topology)
          topology-test-driver (TopologyTestDriver. topology (support/properties "word-count-application"))
          serializer (.serializer (. Serdes String))
          deserializer (.deserializer (. Serdes String))
          factory (ConsumerRecordFactory. serializer serializer)
          input-topic "source-topic"
          output-topic "sink-topic"]

      (.pipeInput topology-test-driver (.create factory input-topic "word-entry" "perkss blog rocks clojure rocks kafka rocks"))

      (let [output (.readOutput topology-test-driver output-topic deserializer deserializer)]
        (is (= "blog" (.key output)))
        (is (= "1" (.value output))))

      (let [output (.readOutput topology-test-driver output-topic deserializer deserializer)]
        (is (= "clojure" (.key output)))
        (is (= "1" (.value output))))

      (let [output (.readOutput topology-test-driver output-topic deserializer deserializer)]
        (is (= "kafka" (.key output)))
        (is (= "1" (.value output))))

      (let [output (.readOutput topology-test-driver output-topic deserializer deserializer)]
        (is (= "perkss" (.key output)))
        (is (= "1" (.value output))))

      (let [output (.readOutput topology-test-driver output-topic deserializer deserializer)]
        (is (= "rocks" (.key output)))
        (is (= "3" (.value output))))

      (.close topology-test-driver))))
