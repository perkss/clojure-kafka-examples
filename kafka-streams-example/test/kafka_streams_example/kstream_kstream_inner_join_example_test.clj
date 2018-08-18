(ns kafka-streams-example.kstream-kstream-inner-join-example-test
  (:require [kafka-streams-example.kstream-kstream-inner-join-example :as sut]
            [clojure.test :refer :all])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams StreamsConfig TopologyTestDriver]
           org.apache.kafka.streams.test.ConsumerRecordFactory
           org.apache.kafka.test.TestUtils))

;; https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Join+Semantics

(def properties
  (let [properties (java.util.Properties.)]
    (.put properties StreamsConfig/APPLICATION_ID_CONFIG (str "click-impressions-application" (rand)))
    (.put properties StreamsConfig/PROCESSING_GUARANTEE_CONFIG StreamsConfig/EXACTLY_ONCE)
    (.put properties StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "dummy:9092")
    (.put properties StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG (.getName (.getClass (Serdes/String))))
    (.put properties StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG (.getName (.getClass (Serdes/String))))
    (.put properties StreamsConfig/COMMIT_INTERVAL_MS_CONFIG  (* 10 1000))
    (.put properties StreamsConfig/STATE_DIR_CONFIG (.getAbsolutePath (TestUtils/tempDirectory)))
    properties))

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
        ad-clicks-topic "adClicks"
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
        ad-impressions-topic "adImpressions"
        ad-clicks-topic "adClicks"
        output-topic "output-topic"]
    (.pipeInput topology-test-driver (.create factory ad-clicks-topic "newspaper-advertisement" "1"))

    (let [output (.readOutput topology-test-driver output-topic deserializer deserializer)]
      (is (= nil output)))
    (.close topology-test-driver)))
