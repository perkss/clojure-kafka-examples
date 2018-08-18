(ns kafka-streams-example.core-test
  (:require [clojure.test :refer :all]
            [kafka-streams-example.core :as sut])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams StreamsConfig TopologyTestDriver]
           org.apache.kafka.streams.test ConsumerRecordFactory OutputVerifier))

(def properties
  (let [properties (java.util.Properties.)]
    (.put properties StreamsConfig/APPLICATION_ID_CONFIG "uppercase-processing-application")
    (.put properties StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "dummy:9092")
    (.put properties StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG (.getName (.getClass (Serdes/String))))
    (.put properties StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG (.getName (.getClass (Serdes/String))))
    properties))

(deftest kafka-streams-to-uppercase-test
  (testing "Kafka Stream example one to test the uppercase topology"
    (let [input-topic "plaintext-input"
          output-topic "uppercase"
          topology (.build (sut/to-uppercase-topology input-topic output-topic))
          topology-test-driver (TopologyTestDriver. topology properties)
          serializer  (.serializer (. Serdes String))
          deserializer (.deserializer (. Serdes String))
          factory (ConsumerRecordFactory. serializer serializer)
          input "Hello my first stream testing to uppercase"
          expected "HELLO MY FIRST STREAM TESTING TO UPPERCASE"]
      (.pipeInput topology-test-driver (.create factory input-topic "key" input))
      (is (= expected (.value (.readOutput topology-test-driver output-topic  deserializer deserializer)))))))
