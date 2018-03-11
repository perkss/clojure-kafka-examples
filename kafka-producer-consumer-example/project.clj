(defproject kafka-example "0.1.0-SNAPSHOT"
  :description "Example Kafka Producer and Consumer using Plain Java Interop"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.apache.kafka/kafka-clients "1.0.1"]]
  :main ^:skip-aot kafka-example.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
