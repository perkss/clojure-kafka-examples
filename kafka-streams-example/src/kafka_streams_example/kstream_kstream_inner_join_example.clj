(ns kafka-streams-example.kstream-kstream-inner-join-example
  (:import (org.apache.kafka.streams StreamsBuilder)
           (org.apache.kafka.streams.kstream KStream ValueJoiner JoinWindows)))


;; The click and impressions example topology where user clicks are joined to
;; impressions with a inner join.

(defn ^KStream build-stream
  [^StreamsBuilder builder input-topic]
  (.stream builder input-topic))

(defn impressions-clicks-topology
  [^KStream impressions ^KStream clicks]
  (-> impressions
      (.join clicks
                  (reify ValueJoiner
                    (apply [_ left right]
                      ((fn [impression-value click-value]
                         (str impression-value "/" click-value))
                       left right)))
                  (. JoinWindows of 5000))))

(defn builder-streaming-join-topology
  []
  (let [builder (StreamsBuilder.)
        ad-impressions-topic "adImpressions"
        ad-clicks-topic "adClicks"
        output-topic "output-topic"
        impressions (build-stream builder ad-impressions-topic)
        clicks (build-stream builder ad-clicks-topic)]

    (-> (impressions-clicks-topology impressions clicks)
        (.to output-topic))
    builder))
