(ns kafka-streams-example.kstream-reduce
  (:import (org.apache.kafka.streams StreamsBuilder)
           (org.apache.kafka.streams.kstream Consumed Grouped Reducer Produced)
           (org.apache.kafka.common.serialization Serdes)))

; https://github.com/confluentinc/kafka-streams-examples/blob/5.4.1-post/src/test/java/io/confluent/examples/streams/ReduceTest.java
(defn build-group-by-reduce-topology
  [input-topic output-topic]
  (let [builder (StreamsBuilder.)
        input (.stream builder input-topic (Consumed/with (Serdes/Long) (Serdes/String)))
        concatenated (-> input
                         (.groupByKey (Grouped/with (Serdes/Long) (Serdes/String)))
                         (.reduce (reify Reducer
                                    (apply [_ v1 v2]
                                      (str v1 " " v2)))))
        _ (-> concatenated
              (.toStream)
              (.to output-topic (Produced/with (Serdes/Long) (Serdes/String))))]
    builder))
