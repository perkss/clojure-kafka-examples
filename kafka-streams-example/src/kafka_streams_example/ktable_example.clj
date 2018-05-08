(ns kafka-streams-example.ktable-example
  (:import (org.apache.kafka.streams StreamsBuilder)
           (org.apache.kafka.streams.kstream KStream KTable)))

;; add threading macro
(defn ^KStream user-click-stream
  [input-topic]
  (let [builder (StreamsBuilder.)
        click-stream (.stream builder input-topic)]
    click-stream))

(defn ^KTable user-region-table
  [input-topic]
  (let [builder (StreamsBuilder.)
        user-regions-table (.table builder input-topic)]
    user-regions-table))

;; where do we build
(defn clicks-per-region
  [user-clicks-stream user-regions-table output-topic]
  (-> user-clicks-stream

      (.leftJoin user-regions-table
                 (fn [clicks region] {:region region :clicks clicks}))
      ;; Check the destructuring here
      ;; (.reduceByKey (fn [{:keys [region clicks]}] (+  clicks region)))
      ;; (.to output-topic)
      ))

(defn build-join-topology
  []
  (let [input-topic-clicks "user-clicks-topic"
        input-topic-regions "user-regions-topic"
        output-topic "clicks-per-region-topic"
        user-clicks (user-click-stream input-topic-clicks)
        user-regions (user-region-table input-topic-regions)]
    (clicks-per-region user-clicks user-regions output-topic)))
