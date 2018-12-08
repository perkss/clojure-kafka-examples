(ns kafka-streams-example.processor-api-rekey-test
  (:require
    [clj-uuid :as uuid]
    [clojure.test :refer [deftest is testing]]
    [kafka-streams-example.serdes.json-serdes :refer [json-serde]]
    [kafka-streams-example.processor-api-rekey :as sut])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams StreamsConfig TopologyTestDriver Topology]
           org.apache.kafka.test.TestUtils
           org.apache.kafka.streams.test.ConsumerRecordFactory))

(def properties
  (let [properties (java.util.Properties.)]
    (.put properties StreamsConfig/APPLICATION_ID_CONFIG (str "word-count-application" (rand)))
    (.put properties StreamsConfig/PROCESSING_GUARANTEE_CONFIG StreamsConfig/EXACTLY_ONCE)
    (.put properties StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "dummy:9092")
    (.put properties StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG (.getName (.getClass (Serdes/String))))
    (.put properties StreamsConfig/COMMIT_INTERVAL_MS_CONFIG (* 10 1000))
    (.put properties StreamsConfig/STATE_DIR_CONFIG (.getAbsolutePath (TestUtils/tempDirectory)))
    properties))

(deftest trade-rekey-to-id-test
  (testing "A test that rekeys the trade messages to trade-id from id"
    (let [topology (sut/trade-rekey-topology (Topology.))
          topology-test-driver (TopologyTestDriver. topology properties)
          key-serializer (.serializer (. Serdes String))
          key-deserializer (.deserializer (. Serdes String))
          value-serializer (.serializer json-serde)
          value-deserializer (.deserializer json-serde)
          factory (ConsumerRecordFactory. key-serializer value-serializer)
          input-topic "trade-input-topic"
          output-topic "trades-by-trade-id"
          trade-msg {:id       (str (uuid/v1))
                     :trade-id (str (uuid/v1))
                     :buyer    "perkss"
                     :seller   "stuart"}
          trade-msg-key (:id trade-msg)
          new-trade-msg-key (:trade-id trade-msg)]

      ;; Send in with Key ID
      (.pipeInput topology-test-driver (.create factory input-topic trade-msg-key trade-msg))

      ;; The new key is the trade-id
      (let [output (.readOutput topology-test-driver output-topic key-deserializer value-deserializer)]
        (is (= new-trade-msg-key (.key output)))
        (is (= trade-msg (.value output))))

      (.close topology-test-driver))))