(ns kafka-streams-example.processor-api-rekey-test
  (:require
    [clj-uuid :as uuid]
    [clojure.test :refer [deftest is testing]]
    [kafka-streams-example.serdes.json-serdes :refer [json-serde]]
    [kafka-streams-example.processor-api-rekey :as sut]
    [kafka-streams-example.test-support :as support])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams TopologyTestDriver Topology]))

(deftest trade-rekey-to-id-test
  (testing "A test that rekeys the trade messages to trade-id from id"
    (let [topology (sut/trade-rekey-topology (Topology.))
          topology-test-driver (TopologyTestDriver. topology (support/properties "word-count-application"))
          key-serializer (.serializer (. Serdes String))
          key-deserializer (.deserializer (. Serdes String))
          value-serializer (.serializer json-serde)
          value-deserializer (.deserializer json-serde)
          input-topic (.createInputTopic topology-test-driver "trade-input-topic" key-serializer value-serializer)
          output-topic (.createOutputTopic topology-test-driver "trades-by-trade-id" key-deserializer value-deserializer)
          trade-msg {:id       (str (uuid/v4))
                     :trade-id (str (uuid/v4))
                     :buyer    "perkss"
                     :seller   "stuart"}
          trade-msg-key (:id trade-msg)
          new-trade-msg-key (:trade-id trade-msg)]

      ;; Send in with Key ID
      (.pipeInput input-topic trade-msg-key trade-msg)

      ;; The new key is the trade-id
      (let [output (.readKeyValue output-topic)]
        (is (= new-trade-msg-key (.key output)))
        (is (= trade-msg (.value output))))

      (.close topology-test-driver))))