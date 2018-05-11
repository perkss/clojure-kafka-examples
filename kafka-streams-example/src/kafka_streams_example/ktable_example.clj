(ns kafka-streams-example.ktable-example
  (:import (org.apache.kafka.streams StreamsBuilder KeyValue)
           (org.apache.kafka.streams.kstream KStream KTable ValueJoiner KGroupedStream KeyValueMapper Reducer)))

(defn ^KStream user-click-stream
  [builder input-topic]
  (.stream builder input-topic))

(defn ^KTable user-region-table
  [builder input-topic]
  (.table builder input-topic))

(defn clicks-per-region
  [^KStream user-clicks-stream ^KTable user-regions-table output-topic]
  (-> user-clicks-stream
      ;; Joins on the Key which is the name
      (.leftJoin user-regions-table
                 (reify ValueJoiner
                   (apply [_ left right]
                     ((fn [clicks region]
                        {:region region :clicks clicks})
                      left right))))
      (.map (reify KeyValueMapper
              (apply [_ k v]
                ((fn [user clicks-with-regions]
                   (let [value (KeyValue.
                                (:region clicks-with-regions)
                                (:clicks clicks-with-regions))]
                     value)) k v))))
      (.groupByKey)
      (.reduce (reify Reducer
                 (apply [_ left right]
                   ((fn [first-clicks second-clicks]
                      (+ first-clicks second-clicks)) left right))))))

(defn build-join-topology
  []
  (let [builder (StreamsBuilder.)
        input-topic-clicks "user-clicks-topic"
        input-topic-regions "user-regions-topic"
        output-topic "clicks-per-region-topic"
        user-clicks (user-click-stream builder input-topic-clicks)
        user-regions (user-region-table builder input-topic-regions)]
    (-> (clicks-per-region user-clicks user-regions output-topic)
        (.toStream)
        (.to output-topic))
    builder))
