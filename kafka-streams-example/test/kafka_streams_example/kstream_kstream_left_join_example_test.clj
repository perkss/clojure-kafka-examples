(ns kafka-streams-example.kstream-kstream-left-join-example-test
  (:require [kafka-streams-example.kstream-kstream-left-join-example :as sut]
            [clojure.test :refer [deftest testing is]]
            [kafka-streams-example.test-support :as support])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams TopologyTestDriver]
           org.apache.kafka.streams.test.ConsumerRecordFactory))

(def properties (support/properties "click-impressions-application"))

;; https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Join+Semantics
(deftest kafka-streams-example-streaming-left-join-test
  (testing "Joining two KafkaStreams with a joining window with input from each side")
  (let [topology (.build (sut/builder-streaming-join-topology))
        topology-test-driver (TopologyTestDriver. topology properties)
        serializer  (.serializer (. Serdes String))
        deserializer (.deserializer (. Serdes String))
        factory (ConsumerRecordFactory. serializer serializer)
        ad-impressions-topic "adImpressions"
        ad-clicks-topic "adClicks"
        output-topic "output-topic"]

    (.pipeInput topology-test-driver (.create factory ad-impressions-topic "newspaper-advertisement" "football-advert"))

    ;; Outputs the record first as it received left side first
    (let [output (.readOutput topology-test-driver output-topic deserializer deserializer)]
      (is (= "newspaper-advertisement" (.key output)))
      (is (= "football-advert/" (.value output))))

    ;; send the right side and it then joins
    (.pipeInput topology-test-driver (.create factory ad-clicks-topic "newspaper-advertisement" "1"))
    (let [output (.readOutput topology-test-driver output-topic deserializer deserializer)]
      (is (= "newspaper-advertisement" (.key output)))
      (is (= "football-advert/1" (.value output))))
    (.close topology-test-driver)))

(deftest kafka-streams-example-streaming-left-join-only-left-side-present-test
  (testing "Joining two KafkaStreams with a joining window with input from the left side
           as it is a left join that single data will flow through so will just be shown")
  (let [topology (.build (sut/builder-streaming-join-topology))
        topology-test-driver (TopologyTestDriver. topology properties)
        serializer  (.serializer (. Serdes String))
        deserializer (.deserializer (. Serdes String))
        factory (ConsumerRecordFactory. serializer serializer)
        ad-impressions-topic "adImpressions"
        output-topic "output-topic"]
    (.pipeInput topology-test-driver (.create factory ad-impressions-topic "newspaper-advertisement" "football-advert"))

    (let [output (.readOutput topology-test-driver output-topic deserializer deserializer)]
      (is (= "newspaper-advertisement" (.key output)))
      (is (= "football-advert/" (.value output))))
    (.close topology-test-driver)))

(deftest kafka-streams-example-streaming-left-join-only-right-side-present-test
  (testing "Joining two KafkaStreams with a joining window with input from the right side
           as it is a left join the right single data will not flow through")
  (let [topology (.build (sut/builder-streaming-join-topology))
        topology-test-driver (TopologyTestDriver. topology properties)
        serializer  (.serializer (. Serdes String))
        deserializer (.deserializer (. Serdes String))
        factory (ConsumerRecordFactory. serializer serializer)
        ad-clicks-topic "adClicks"
        output-topic "output-topic"]
    (.pipeInput topology-test-driver (.create factory ad-clicks-topic "newspaper-advertisement" "1"))

    (let [output (.readOutput topology-test-driver output-topic deserializer deserializer)]
      (is (= nil output)))
    (.close topology-test-driver)))
