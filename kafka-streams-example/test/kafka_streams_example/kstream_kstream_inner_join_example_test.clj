(ns kafka-streams-example.kstream-kstream-inner-join-example-test
  (:require [kafka-streams-example.kstream-kstream-inner-join-example :as sut]
            [clojure.test :refer [deftest testing is]]
            [kafka-streams-example.test-support :as support])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams TopologyTestDriver]
           org.apache.kafka.streams.test.ConsumerRecordFactory))

;; https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Join+Semantics
(def properties (support/properties "click-impressions-application"))

(deftest kafka-streams-example-streaming-inner-join-test
  (testing "Joining two KafkaStreams with a joining window with input from each side")
  (let [topology (.build (sut/builder-streaming-join-topology))
        topology-test-driver (TopologyTestDriver. topology properties)
        serializer  (.serializer (. Serdes String))
        deserializer (.deserializer (. Serdes String))
        factory (ConsumerRecordFactory. serializer serializer)
        ad-impressions-topic "adImpressions"
        ad-clicks-topic "adClicks"
        output-topic "output-topic"]
    (.pipeInput topology-test-driver (.create factory ad-clicks-topic "newspaper-advertisement" "1"))
    (.pipeInput topology-test-driver (.create factory ad-impressions-topic "newspaper-advertisement" "football-advert"))

    (let [output (.readOutput topology-test-driver output-topic deserializer deserializer)]
      (is (= "newspaper-advertisement" (.key output)))
      (is (= "football-advert/1" (.value output))))
    (.close topology-test-driver)))

(deftest kafka-streams-example-streaming-inner-join-only-left-side-present-test
  (testing "Joining two KafkaStreams with a joining window with input from the left side
           as it is a inner join it will wait for both sides so will return nothing")
  (let [topology (.build (sut/builder-streaming-join-topology))
        topology-test-driver (TopologyTestDriver. topology properties)
        serializer  (.serializer (. Serdes String))
        deserializer (.deserializer (. Serdes String))
        factory (ConsumerRecordFactory. serializer serializer)
        ad-impressions-topic "adImpressions"
        output-topic "output-topic"]
    (.pipeInput topology-test-driver (.create factory ad-impressions-topic "newspaper-advertisement" "football-advert"))

    (let [output (.readOutput topology-test-driver output-topic deserializer deserializer)]
      (is (= nil output)))
    (.close topology-test-driver)))

(deftest kafka-streams-example-streaming-inner-join-only-right-side-present-test
  (testing "Joining two KafkaStreams with a joining window with input from the right side
           as it is a innerjoin will return nothing as waits for both sides")
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
