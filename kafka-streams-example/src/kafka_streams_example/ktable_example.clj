(ns kafka-streams-example.ktable-example
  (:require [kafka-streams-example.utils :as kstream-utils])
  (:import (org.apache.kafka.streams StreamsBuilder KeyValue)
           (org.apache.kafka.streams.kstream KStream KTable ValueJoiner KeyValueMapper Reducer)))

(defn clicks-per-region
  [^KStream user-clicks-stream ^KTable user-regions-table]
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
                ((fn [_ clicks-with-regions]
                   (let [value (KeyValue.
                                (:region clicks-with-regions)
                                (:clicks clicks-with-regions))]
                     value)) k v))))
      (.groupByKey)
      (.reduce (reify Reducer
                 (apply [_ left right]
                   ((fn [first-clicks second-clicks]
                      (str (+ (Integer. first-clicks) (Integer. second-clicks)))) left right))))))

(defn build-join-topology
  []
  (let [builder (StreamsBuilder.)
        input-topic-clicks "user-clicks-topic"
        input-topic-regions "user-regions-topic"
        output-topic "clicks-per-region-topic"
        user-clicks (kstream-utils/build-stream builder input-topic-clicks)
        user-regions (kstream-utils/build-table builder input-topic-regions)]
    (-> (clicks-per-region user-clicks user-regions)
        (.toStream)
        (.to output-topic))
    builder))
