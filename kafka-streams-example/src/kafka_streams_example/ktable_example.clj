(ns kafka-streams-example.ktable-example
  (:import (org.apache.kafka.streams StreamsBuilder)
           (org.apache.kafka.streams.kstream KStream KTable ValueJoiner KGroupedStream
                                             Reducer)))


;; add threading macro
(defn ^KStream user-click-stream
  [builder input-topic]
  (let [click-stream (.stream builder input-topic)]
    click-stream))

(defn ^KTable user-region-table
  [builder input-topic]
  (let [user-regions-table (.table builder input-topic)]
    user-regions-table))

;; where do we build
(defn clicks-per-region
  [^KStream user-clicks-stream ^KTable user-regions-table output-topic]
  (-> user-clicks-stream
      ;; Joins on the Key which is the name
      (.leftJoin user-regions-table
                 (reify ValueJoiner
                   (apply [_ left right]
                     ((fn [clicks region]
                        (str {:region region :clicks clicks})

                        )
                      left right))))
     ;; (.map )
      ;; Check the destructuring here
     ;; (.groupByKey)
      ;;(.reduceByKey (fn [{:keys [region clicks]}] (+ clicks region)))
      (.to output-topic)))

(defn build-join-topology
  []
  (let [builder (StreamsBuilder.)
        input-topic-clicks "user-clicks-topic"
        input-topic-regions "user-regions-topic"
        output-topic "clicks-per-region-topic"
        user-clicks (user-click-stream builder input-topic-clicks)
        user-regions (user-region-table builder input-topic-regions)]
    (clicks-per-region user-clicks user-regions output-topic)
    builder))
