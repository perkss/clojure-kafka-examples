(defproject kafka-example "0.1.0-SNAPSHOT"
  :description "Example Kafka Producer and Consumer using Plain Java Interop"
  :url "https://perkss.github.io/#/clojure/KafkaClojure#text-body"
  :dependencies [[environ "1.1.0"]
                 [org.clojure/clojure "1.11.1"]
                 [org.apache.kafka/kafka-clients "3.8.0"]
                 [org.apache.kafka/kafka_2.12 "3.8.0"]
                 [org.clojure/tools.logging "1.2.4"]
                 [org.slf4j/slf4j-log4j12 "2.0.10"]
                 [org.apache.logging.log4j/log4j-core "2.22.1"]
                 [org.testcontainers/testcontainers "1.19.3"]
                 [org.testcontainers/kafka "1.19.3"]]
  :main ^:skip-aot kafka-example.core
  :target-path "target/%s"
  :plugins [[lein-cljfmt "0.6.0"]]
  :profiles {:uberjar {:aot :all}})
