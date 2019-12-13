(ns kafka-streams-example.core-test
  (:require [clojure.test :refer :all]
            [kafka-streams-example.core :as sut]
            [kafka-streams-example.test-support :as support])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams TopologyTestDriver]
           org.apache.kafka.streams.test.ConsumerRecordFactory))

(deftest kafka-streams-to-uppercase-test
  (testing "Kafka Stream example one to test the uppercase topology"
    (let [input-topic "plaintext-input"
          output-topic "uppercase"
          topology (.build (sut/to-uppercase-topology input-topic output-topic))
          topology-test-driver (TopologyTestDriver. topology (support/properties "uppercase-topology"))
          serializer  (.serializer (. Serdes String))
          deserializer (.deserializer (. Serdes String))
          factory (ConsumerRecordFactory. serializer serializer)
          input "Hello my first stream testing to uppercase"
          expected "HELLO MY FIRST STREAM TESTING TO UPPERCASE"]
      (.pipeInput topology-test-driver (.create factory input-topic "key" input))
      (is (= expected (.value (.readOutput topology-test-driver output-topic  deserializer deserializer)))))))
