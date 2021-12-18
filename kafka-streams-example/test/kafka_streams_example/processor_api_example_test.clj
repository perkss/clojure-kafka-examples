(ns kafka-streams-example.processor-api-example-test
  (:require [clojure.test :refer [deftest is testing]]
            [kafka-streams-example.processor-api-example :as sut]
            [kafka-streams-example.test-support :as support])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams TopologyTestDriver]))

(deftest word-count-test
  (testing "A word count processor API test"
    (let [topology (sut/word-processor-topology)
          topology-test-driver (TopologyTestDriver. topology (support/properties "word-count-application"))
          serializer (.serializer (. Serdes String))
          deserializer (.deserializer (. Serdes String))
          input-topic (.createInputTopic topology-test-driver "source-topic" serializer serializer)
          output-topic (.createOutputTopic topology-test-driver "sink-topic" deserializer deserializer)]

      (.pipeInput input-topic "word-entry" "perkss blog rocks clojure rocks kafka rocks")

      (let [output (.readKeyValue output-topic)]
        (is (= "blog" (.key output)))
        (is (= "1" (.value output))))

      (let [output (.readKeyValue output-topic)]
        (is (= "clojure" (.key output)))
        (is (= "1" (.value output))))

      (let [output (.readKeyValue output-topic)]
        (is (= "kafka" (.key output)))
        (is (= "1" (.value output))))

      (let [output (.readKeyValue output-topic)]
        (is (= "perkss" (.key output)))
        (is (= "1" (.value output))))

      (let [output (.readKeyValue output-topic)]
        (is (= "rocks" (.key output)))
        (is (= "3" (.value output))))

      (.close topology-test-driver))))
