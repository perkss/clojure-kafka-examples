(ns kafka-streams-example.ktable-example-test
  (:require [kafka-streams-example.ktable-example :as sut]
            [clojure.test :refer [deftest testing is]]
            [kafka-streams-example.test-support :as support])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams TopologyTestDriver Topology]))

(deftest kafka-streams-ktable-example-test
  (testing "Kafka Streams with KTABLE"
    (let [^Topology topology (.build (sut/build-join-topology))
          topology-test-driver (TopologyTestDriver. topology (support/properties "user-clicks-application"))
          serializer (.serializer (. Serdes String))
          deserializer (.deserializer (. Serdes String))
          user-clicks-topic (.createInputTopic topology-test-driver "user-clicks-topic" serializer serializer)
          user-regions-topic (.createInputTopic topology-test-driver "user-regions-topic" serializer serializer)
          output-topic (.createOutputTopic topology-test-driver "clicks-per-region-topic" deserializer deserializer)
          input-clicks "2"
          input-regions "England"]
      (.pipeInput user-regions-topic "alice" input-regions)
      (.pipeInput user-clicks-topic "alice" input-clicks)
      (.pipeInput user-clicks-topic "alice" input-clicks)
      (let [output (.readKeyValue output-topic)
            output-2 (.readKeyValue output-topic)]
        (is (= "England" (.key output)))
        (is (= "2" (.value output)))
        (is (= "England" (.key output-2)))
        (is (= "4" (.value output-2)))))))
