(ns kafka-streams-example.processor-api-example-test
  (:require [clojure.test :refer [deftest is testing]]
            [kafka-streams-example.processor-api-example :as sut])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams StreamsConfig TopologyTestDriver]
           org.apache.kafka.test.TestUtils
           org.apache.kafka.streams.test.ConsumerRecordFactory))

(def properties
  (let [properties (java.util.Properties.)]
    (.put properties StreamsConfig/APPLICATION_ID_CONFIG (str "word-count-application" (rand)))
    (.put properties StreamsConfig/PROCESSING_GUARANTEE_CONFIG StreamsConfig/EXACTLY_ONCE)
    (.put properties StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "dummy:9092")
    (.put properties StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG (.getName (.getClass (Serdes/String))))
    (.put properties StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG (.getName (.getClass (Serdes/String))))
    (.put properties StreamsConfig/COMMIT_INTERVAL_MS_CONFIG  (* 10 1000))
    (.put properties StreamsConfig/STATE_DIR_CONFIG (.getAbsolutePath (TestUtils/tempDirectory)))
    properties))


(deftest word-count-test
  (testing "A word count processor API test"
    (let [topology (sut/word-processor-topology)
          topology-test-driver (TopologyTestDriver. topology properties)
          serializer (.serializer (. Serdes String))
          deserializer (.deserializer (. Serdes String))
          factory (ConsumerRecordFactory. serializer serializer)
          input-topic "source-topic"
          output-topic "sink-topic"]
      (.pipeInput topology-test-driver (.create factory input-topic "word-entry" "stuart"))

      (let [output (.readOutput topology-test-driver output-topic deserializer deserializer)]
        (is (= "stuart" (.key  output)))
        (is (= "1" (.value output))))

      (.close topology-test-driver))))
