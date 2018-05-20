(ns kafka-streams-example.ktable-example-test
  (:require [kafka-streams-example.ktable-example :as sut]
            [clojure.test :refer :all])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams StreamsConfig TopologyTestDriver]
           org.apache.kafka.streams.test.ConsumerRecordFactory
           org.apache.kafka.test.TestUtils))

(def properties
  (let [properties (java.util.Properties.)]
    (.put properties StreamsConfig/APPLICATION_ID_CONFIG "uppercase-processing-application")
    (.put properties StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "dummy:9092")
    (.put properties StreamsConfig/KEY_SERDE_CLASS_CONFIG (.getName (.getClass (Serdes/String))))
    (.put properties StreamsConfig/VALUE_SERDE_CLASS_CONFIG (.getName (.getClass (Serdes/String))))
    (.put properties StreamsConfig/COMMIT_INTERVAL_MS_CONFIG  (* 10 1000))
    (.put properties StreamsConfig/STATE_DIR_CONFIG (.getAbsolutePath (. TestUtils tempDirectory)))
    properties))


;; TODO multiple data check the clicks are summing
(deftest kafka-streams-ktable-example-test
  (testing "Kafka Streams with KTABLE"
    (let [topology (.build (sut/build-join-topology))
          topology-test-driver (TopologyTestDriver. topology properties)
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
