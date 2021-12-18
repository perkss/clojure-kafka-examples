(ns kafka-streams-example.kstream-kstream-outer-join-example-test
  (:require [kafka-streams-example.kstream-kstream-outer-join-example :as sut]
            [clojure.test :refer [deftest is testing]]
            [kafka-streams-example.test-support :as support])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams TopologyTestDriver Topology]
           (java.util Properties)))

(def ^Properties properties (support/properties "click-impressions-application"))

;; https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Join+Semantics
(deftest kafka-streams-example-streaming-outer-join-test
  (testing "Joining two KafkaStreams with a joining window with input from each side
            as it is an outer join the first result will be released on its own and
            then as a pair.")
  (let [^Topology topology (.build (sut/builder-streaming-join-topology))
        topology-test-driver (TopologyTestDriver. topology properties)
        serializer (.serializer (. Serdes String))
        deserializer (.deserializer (. Serdes String))
        ad-clicks-topic (.createInputTopic topology-test-driver "adClicks" serializer serializer)
        ad-impressions-topic (.createInputTopic topology-test-driver "adImpressions" serializer serializer)
        output-topic (.createOutputTopic topology-test-driver "output-topic" deserializer deserializer)]
    (.pipeInput ad-clicks-topic "newspaper-advertisement" "1")
    (.pipeInput ad-impressions-topic "newspaper-advertisement" "football-advert")

    ;; Will release first input immediately as outer join
    (let [output (.readKeyValue output-topic)]
      (is (= "newspaper-advertisement" (.key output)))
      (is (= "/1" (.value output))))

    (let [output (.readKeyValue output-topic)]
      (is (= "newspaper-advertisement" (.key output)))
      (is (= "football-advert/1" (.value output))))
    (.close topology-test-driver)))

(deftest kafka-streams-example-streaming-outer-join-only-left-side-present-test
  (testing "Joining two KafkaStreams with a joining window with input from the left side
           as it is a outer join that single data will flow through")
  (let [^Topology topology (.build (sut/builder-streaming-join-topology))
        topology-test-driver (TopologyTestDriver. topology properties)
        serializer (.serializer (. Serdes String))
        deserializer (.deserializer (. Serdes String))
        ad-impressions-topic (.createInputTopic topology-test-driver "adImpressions" serializer serializer)
        output-topic (.createOutputTopic topology-test-driver "output-topic" deserializer deserializer)
        ]
    (.pipeInput ad-impressions-topic "newspaper-advertisement" "football-advert")

    (let [output (.readKeyValue output-topic)]
      (is (= "newspaper-advertisement" (.key output)))
      (is (= "football-advert/" (.value output))))
    (.close topology-test-driver)))

(deftest kafka-streams-example-streaming-outer-join-only-right-side-present-test
  (testing "Joining two KafkaStreams with a joining window with input from the right side
           as it is a outer-join join the right single data will just flow through")
  (let [^Topology topology (.build (sut/builder-streaming-join-topology))
        topology-test-driver (TopologyTestDriver. topology properties)
        serializer (.serializer (. Serdes String))
        deserializer (.deserializer (. Serdes String))
        ad-clicks-topic (.createInputTopic topology-test-driver "adClicks" serializer serializer)
        output-topic (.createOutputTopic topology-test-driver "output-topic" deserializer deserializer)
        ]
    (.pipeInput ad-clicks-topic "newspaper-advertisement" "1")

    (let [output (.readKeyValue output-topic)]
      (is (= "newspaper-advertisement" (.key output)))
      (is (= "/1" (.value output))))
    (.close topology-test-driver)))
