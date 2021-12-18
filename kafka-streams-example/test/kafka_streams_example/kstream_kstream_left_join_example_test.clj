(ns kafka-streams-example.kstream-kstream-left-join-example-test
  (:require [kafka-streams-example.kstream-kstream-left-join-example :as sut]
            [clojure.test :refer [deftest testing is]]
            [kafka-streams-example.test-support :as support])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams TopologyTestDriver]
           (java.util NoSuchElementException)))

(def properties (support/properties "click-impressions-application"))

;; https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Join+Semantics
(deftest kafka-streams-example-streaming-left-join-test
  (testing "Joining two KafkaStreams with a joining window with input from each side")
  (let [topology (.build (sut/builder-streaming-join-topology))
        topology-test-driver (TopologyTestDriver. topology properties)
        serializer (.serializer (. Serdes String))
        deserializer (.deserializer (. Serdes String))
        ad-clicks-topic (.createInputTopic topology-test-driver "adClicks" serializer serializer)
        ad-impressions-topic (.createInputTopic topology-test-driver "adImpressions" serializer serializer)
        output-topic (.createOutputTopic topology-test-driver "output-topic" deserializer deserializer)]

    (.pipeInput ad-impressions-topic "newspaper-advertisement" "football-advert")

    ;; Outputs the record first as it received left side first
    (let [output (.readKeyValue output-topic)]
      (is (= "newspaper-advertisement" (.key output)))
      (is (= "football-advert/" (.value output))))

    ;; send the right side and it then joins
    (.pipeInput ad-clicks-topic "newspaper-advertisement" "1")
    (let [output (.readKeyValue output-topic)]
      (is (= "newspaper-advertisement" (.key output)))
      (is (= "football-advert/1" (.value output))))
    (.close topology-test-driver)))

(deftest kafka-streams-example-streaming-left-join-only-left-side-present-test
  (testing "Joining two KafkaStreams with a joining window with input from the left side
           as it is a left join that single data will flow through so will just be shown")
  (let [topology (.build (sut/builder-streaming-join-topology))
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

(deftest kafka-streams-example-streaming-left-join-only-right-side-present-test
  (testing "Joining two KafkaStreams with a joining window with input from the right side
           as it is a left join the right single data will not flow through")
  (let [topology (.build (sut/builder-streaming-join-topology))
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
