(ns kafka-streams-example.kstream-reduce-test
  (:require [clojure.test :refer :all])
  (:require [kafka-streams-example.kstream-reduce :refer [build-group-by-reduce-topology]]
            [kafka-streams-example.test-support :as support])
  (:import (org.apache.kafka.streams TopologyTestDriver KeyValue)
           (org.apache.kafka.common.serialization StringSerializer StringDeserializer LongSerializer LongDeserializer)))

(deftest build-group-by-reduce-topology-test
  (testing "Group by a key then reduce the values into single string")
  (let [input-topic-name "input-topic"
        output-topic-name "output-topic"
        topology (.build (build-group-by-reduce-topology input-topic-name output-topic-name))
        topology-test-driver (TopologyTestDriver. topology (support/properties "reduce-topology"))
        input-topic (.createInputTopic topology-test-driver input-topic-name (LongSerializer.) (StringSerializer.))
        output-topic (.createOutputTopic topology-test-driver output-topic-name (LongDeserializer.) (StringDeserializer.))
        input-records [(KeyValue. 456 "stream")
                       (KeyValue. 123 "hello")
                       (KeyValue. 123 "world")
                       (KeyValue. 456 "all")
                       (KeyValue. 123 "kafka")
                       (KeyValue. 456 "the")
                       (KeyValue. 456 "things")
                       (KeyValue. 123 "streams")]
        expected-output {456 "stream all the things"
                         123 "hello world kafka streams"}]
    (.pipeKeyValueList input-topic input-records)
    (is (= expected-output (.readKeyValuesToMap output-topic)))))
