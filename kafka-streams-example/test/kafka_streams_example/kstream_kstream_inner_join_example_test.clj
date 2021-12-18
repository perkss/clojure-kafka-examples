(ns kafka-streams-example.kstream-kstream-inner-join-example-test
  (:require [kafka-streams-example.kstream-kstream-inner-join-example :as sut]
            [clojure.test :refer [deftest testing is]]
            [kafka-streams-example.test-support :as support])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams TopologyTestDriver Topology]
           (java.util Properties NoSuchElementException)))

;; https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Join+Semantics
(def ^Properties properties (support/properties "click-impressions-application"))

(deftest kafka-streams-example-streaming-inner-join-test
  (testing "Joining two KafkaStreams with a joining window with input from each side")
  (let [^Topology topology (.build (sut/builder-streaming-join-topology))
        topology-test-driver (TopologyTestDriver. topology properties)
        serializer (.serializer (. Serdes String))
        deserializer (.deserializer (. Serdes String))
        ad-clicks-topic (.createInputTopic topology-test-driver "adClicks" serializer serializer)
        ad-impressions-topic (.createInputTopic topology-test-driver "adImpressions" serializer serializer)
        output-topic (.createOutputTopic topology-test-driver "output-topic" deserializer deserializer)]
    (.pipeInput ad-clicks-topic "newspaper-advertisement" "1")
    (.pipeInput ad-impressions-topic "newspaper-advertisement" "football-advert")
    (let [output (.readKeyValue output-topic)]
      (is (= "newspaper-advertisement" (.key output)))
      (is (= "football-advert/1" (.value output))))
    (.close topology-test-driver)))

(deftest kafka-streams-example-streaming-inner-join-only-left-side-present-test
  (testing "Joining two KafkaStreams with a joining window with input from the left side
           as it is a inner join it will wait for both sides so will return nothing")
  (let [^Topology topology (.build (sut/builder-streaming-join-topology))
        topology-test-driver (TopologyTestDriver. topology properties)
        serializer (.serializer (. Serdes String))
        deserializer (.deserializer (. Serdes String))
        ad-impressions-topic (.createInputTopic topology-test-driver "adImpressions" serializer serializer)
        output-topic (.createOutputTopic topology-test-driver "output-topic" deserializer deserializer)]
    (.pipeInput ad-impressions-topic "newspaper-advertisement" "football-advert")

    (let [output (try
                   (.readKeyValue output-topic)
                   (catch Exception e e))]
      (is (= NoSuchElementException (type output))))
    (.close topology-test-driver)))

(deftest kafka-streams-example-streaming-inner-join-only-right-side-present-test
  (testing "Joining two KafkaStreams with a joining window with input from the right side
           as it is a innerjoin will return nothing as waits for both sides")
  (let [^Topology topology (.build (sut/builder-streaming-join-topology))
        topology-test-driver (TopologyTestDriver. topology properties)
        serializer (.serializer (. Serdes String))
        deserializer (.deserializer (. Serdes String))
        ad-clicks-topic (.createInputTopic topology-test-driver "adClicks" serializer serializer)
        output-topic (.createOutputTopic topology-test-driver "output-topic" deserializer deserializer)]
    (.pipeInput ad-clicks-topic "newspaper-advertisement" "1")

    (let [output (try
                   (.readKeyValue output-topic)
                   (catch Exception e e))]
      (is (= NoSuchElementException (type output))))
    (.close topology-test-driver)))
