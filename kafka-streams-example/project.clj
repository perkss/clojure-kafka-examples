(defproject kafka-streams-example "0.1.0-SNAPSHOT"
  :description "Kafka Streams Example"
  :url "https://perkss.github.io/#/clojure/KafkaClojure#text-body"
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.clojure/data.json "0.2.6"]
                 [org.apache.kafka/kafka-streams "3.6.1"]
                 [org.apache.kafka/kafka-clients "3.6.1"]
                 [org.apache.kafka/kafka-streams-test-utils "3.6.1"]
                 [org.apache.kafka/kafka-clients "3.6.1" :classifier "test"]
                 [io.confluent/kafka-streams-avro-serde "7.3.3"]
                 [org.clojure/tools.logging "1.2.4"]
                 [org.slf4j/slf4j-log4j12 "2.0.10"]
                 [org.apache.logging.log4j/log4j-core "2.22.1"]
                 [danlentz/clj-uuid "0.1.9"]]

  :repositories [["confluent"  {:url "https://packages.confluent.io/maven/"}]]
  :plugins [[lein-cljfmt "0.6.1"]]
  :main ^:skip-aot kafka-streams-example.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
