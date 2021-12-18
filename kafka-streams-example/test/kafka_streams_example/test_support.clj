(ns kafka-streams-example.test-support
  (:import (java.util Properties)
           (org.apache.kafka.streams StreamsConfig)
           (org.apache.kafka.common.serialization Serdes)
           (org.apache.kafka.test TestUtils)))

(defn ^Properties properties [application-name]
  (let [properties (Properties.)]
    (.put properties StreamsConfig/APPLICATION_ID_CONFIG application-name)
    (.put properties StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "dummy:9092")
    (.put properties StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG (.getName (.getClass (Serdes/String))))
    (.put properties StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG (.getName (.getClass (Serdes/String))))
    (.put properties StreamsConfig/COMMIT_INTERVAL_MS_CONFIG (* 10 1000))
    (.put properties StreamsConfig/STATE_DIR_CONFIG (.getAbsolutePath (. TestUtils tempDirectory)))
    properties))