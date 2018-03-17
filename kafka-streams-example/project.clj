(defproject kafka-streams-example "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.apache.kafka/kafka-streams "1.0.1"]]
  :main ^:skip-aot kafka-streams-example.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
