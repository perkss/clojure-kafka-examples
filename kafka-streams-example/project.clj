(defproject kafka-streams-example "0.1.0-SNAPSHOT"
  :description "Kafka Streams Example"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.apache.kafka/kafka-streams "1.1.0"]
                 [org.apache.kafka/kafka-clients "1.1.0"]
                 [org.apache.kafka/kafka-streams-test-utils "1.1.0"]
                 [org.apache.kafka/kafka-clients "1.1.0" :classifier "test"]
                 [org.clojure/tools.logging "0.4.0"]
                 [org.slf4j/slf4j-log4j12 "1.7.1"]
                 [log4j/log4j "1.2.17" :exclusions [javax.mail/mail
                                                    javax.jms/jms
                                                    com.sun.jmdk/jmxtools
                                                    com.sun.jmx/jmxri]]]

  :plugins [[lein-environ "1.0.1"]]
  :main ^:skip-aot kafka-streams-example.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}}
  :env {:squiggly {:checkers [:eastwood]
                   :eastwood-exclude-linters [:unlimited-use]}})
