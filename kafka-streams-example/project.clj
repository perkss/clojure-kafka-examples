(defproject kafka-streams-example "0.1.0-SNAPSHOT"
  :description "Kafka Streams Example"
  :url "https://perkss.github.io/#/clojure/KafkaClojure#text-body"
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/data.json "0.2.6"]
                 [org.apache.kafka/kafka-streams "2.3.1"]
                 [org.apache.kafka/kafka-clients "2.3.1"]
                 [org.apache.kafka/kafka-streams-test-utils "2.3.1"]
                 [org.apache.kafka/kafka-clients "2.3.1" :classifier "test"]
                 [io.confluent/kafka-streams-avro-serde "5.0.0"]
                 [org.clojure/tools.logging "0.4.0"]
                 [org.slf4j/slf4j-log4j12 "1.7.1"]
                 [log4j/log4j "1.2.17" :exclusions [javax.mail/mail
                                                    javax.jms/jms
                                                    com.sun.jmdk/jmxtools
                                                    com.sun.jmx/jmxri]]
                 [danlentz/clj-uuid "0.1.7"]]

  :repositories [["confluent"  {:url "https://packages.confluent.io/maven/"}]]
  :plugins [[lein-cljfmt "0.6.1"]]
  :main ^:skip-aot kafka-streams-example.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
