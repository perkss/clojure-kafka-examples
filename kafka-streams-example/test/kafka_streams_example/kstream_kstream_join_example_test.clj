(ns kafka-streams-example.kstream-kstream-join-example-test
  (:require [kafka-streams-example.kstream-kstream-join-example :as sut]
            [clojure.test :refer :all])
  (:import org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams StreamsConfig TopologyTestDriver]
           org.apache.kafka.streams.test.ConsumerRecordFactory
           org.apache.kafka.test.TestUtils))

(def properties
  (let [properties (java.util.Properties.)]
    (.put properties StreamsConfig/APPLICATION_ID_CONFIG "alerts-application")
    (.put properties StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "dummy:9092")
    (.put properties StreamsConfig/KEY_SERDE_CLASS_CONFIG (.getName (.getClass (Serdes/String))))
    (.put properties StreamsConfig/VALUE_SERDE_CLASS_CONFIG (.getName (.getClass (Serdes/String))))
    (.put properties StreamsConfig/COMMIT_INTERVAL_MS_CONFIG  (* 10 1000))
    (.put properties StreamsConfig/STATE_DIR_CONFIG (.getAbsolutePath (. TestUtils tempDirectory)))
    properties))

(deftest kafka-streams-example-streaming-join-test
  (testing "Joining two KafkaStreams with a joining window")
  (let [topology (.build (sut/builder-streaming-join-topology))
        topology-test-driver (TopologyTestDriver. topology properties)
        serializer  (.serializer (. Serdes String))
        deserializer (.deserializer (. Serdes String))
        factory (ConsumerRecordFactory. serializer serializer)
        ad-impressions-topic "adImpressions"
        ad-clicks-topic "adClicks"
        output-topic "output-topic"]
    (.pipeInput topology-test-driver (.create factory ad-clicks-topic "newspaper-advertisement" "1"))
     (.pipeInput topology-test-driver (.create factory ad-impressions-topic "newspaper-advertisement" "shown"))

    (let [output (.readOutput topology-test-driver output-topic deserializer deserializer)]
      (is (= "newspaper-advertisement" (.key output)))
      (is (= "shown/1" (.value output))))))
