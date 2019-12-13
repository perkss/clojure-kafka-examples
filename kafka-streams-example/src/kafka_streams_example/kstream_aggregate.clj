(ns kafka-streams-example.kstream-aggregate
  (:require [kafka-streams-example.utils :as kstream-utils])
  (:import [org.apache.kafka.streams.kstream Aggregator Initializer KeyValueMapper KStream Materialized Produced KTable]
           org.apache.kafka.streams.StreamsBuilder
           (org.apache.kafka.common.serialization Serdes)))

; https://github.com/confluentinc/kafka-streams-examples/blob/aefdc2fa3fe820709b069685021728e7775b1788/src/test/java/io/confluent/examples/streams/AggregateTest.java

;KTable<Windowed<String>, Long> timeWindowedAggregatedStream = groupedStream.windowedBy(TimeUnit.MINUTES.toMillis(5))
;.aggregate(
;           () -> 0L, /* initializer */
;        (aggKey, newValue, aggValue) -> aggValue + newValue, /* adder */
;        Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("time-windowed-aggregated-stream-store") /* state store name */
;        .withValueSerde(Serdes.Long())); /* serde for aggregate value */
(defn ^KTable impressions-clicks-topology
  [^KStream input]
  (-> input
      (.groupBy (reify KeyValueMapper
                  (apply [_ k v]
                    ((fn [_ v]
                       (.substring v 0 1)) k v))))
      (.aggregate
        (reify Initializer
          (apply [_] 0))
        (reify Aggregator
          (apply [_ _ new-value agg-value]
            (long (+ (.length new-value) agg-value))))
        (Materialized/with (Serdes/String) (Serdes/Long)))))

(defn build-aggregate-topology
  []
  (let [builder (StreamsBuilder.)
        input-topic "input-topic"
        output-topic "output-topic"
        input (kstream-utils/build-stream builder input-topic)]

    (-> (impressions-clicks-topology input)
        (.toStream)
        (.to output-topic (Produced/with (Serdes/String) (Serdes/Long))))
    builder))