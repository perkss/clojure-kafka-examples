(ns kafka-streams-example.kstream_aggregate_test
  (:require [kafka-streams-example.kstream-aggregate :as sut]
            [clojure.test :refer [deftest testing is]]
            [kafka-streams-example.test-support :as support])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams TopologyTestDriver]
           org.apache.kafka.streams.test.ConsumerRecordFactory))

(deftest kafka-streams-example-streaming-aggregate-test
  (testing "Aggregate a stream of works by counting the number of times the starting letter has been seen in total")
  (let [topology (.build (sut/build-aggregate-topology))
        topology-test-driver (TopologyTestDriver. topology (support/properties "aggregate-topology"))
        serializer  (.serializer (Serdes/String))
        value-deserializer (.deserializer (Serdes/Long))
        factory (ConsumerRecordFactory. serializer serializer)
        input-topic "input-topic"
        output-topic "output-topic"]

    (.pipeInput topology-test-driver (.create factory input-topic "1" "stream"))

    (let [output (.readOutput topology-test-driver output-topic (.deserializer (Serdes/String)) value-deserializer)]
      (is (= "s" (.key output)))
      (is (= 6 (.value output))))

    (.pipeInput topology-test-driver (.create factory input-topic "2" "all"))

    (let [output (.readOutput topology-test-driver output-topic (.deserializer (Serdes/String)) value-deserializer)]
      (is (= "a" (.key output)))
      (is (= 3 (.value output))))

    (.pipeInput topology-test-driver (.create factory input-topic "3" "the"))

    (let [output (.readOutput topology-test-driver output-topic (.deserializer (Serdes/String)) value-deserializer)]
      (is (= "t" (.key output)))
      (is (= 3 (.value output))))

    (.pipeInput topology-test-driver (.create factory input-topic "4" "things"))

    ;; Aggregates the count for items beginning with t
    (let [output (.readOutput topology-test-driver output-topic (.deserializer (Serdes/String)) value-deserializer)]
      (is (= "t" (.key output)))
      (is (= 9 (.value output))))

    (.close topology-test-driver)))