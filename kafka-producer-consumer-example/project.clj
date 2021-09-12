(defproject kafka-example "0.1.0-SNAPSHOT"
  :description "Example Kafka Producer and Consumer using Plain Java Interop"
  :url "https://perkss.github.io/#/clojure/KafkaClojure#text-body"
  :dependencies [[environ "1.1.0"]
                 [org.clojure/clojure "1.10.1"]
                 [org.apache.kafka/kafka-clients "2.8.0"]
                 [org.apache.kafka/kafka_2.12 "2.8.0"]
                 [org.clojure/tools.logging "0.4.0"]
                 [org.slf4j/slf4j-log4j12 "1.7.1"]
                 [log4j/log4j "1.2.17" :exclusions [javax.mail/mail
                                                    javax.jms/jms
                                                    com.sun.jmdk/jmxtools
                                                    com.sun.jmx/jmxri]]
                 [org.testcontainers/testcontainers "1.15.3"]
                 [org.testcontainers/kafka "1.15.3"]]
  :main ^:skip-aot kafka-example.core
  :target-path "target/%s"
  :plugins [[lein-cljfmt "0.6.0"]]
  :profiles {:uberjar {:aot :all}})
