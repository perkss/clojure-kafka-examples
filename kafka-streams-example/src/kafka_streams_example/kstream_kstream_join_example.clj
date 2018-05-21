(ns kafka-streams-example.kstream-kstream-join-example
  (:import (org.apache.kafka.streams StreamsBuilder)
           (org.apache.kafka.streams.kstream KStream ValueJoiner JoinWindows)))

(defn ^KStream build-stream
  [^StreamsBuilder builder input-topic]
  (.stream builder input-topic))

(defn impressions-clicks-topology
  [^KStream alerts ^KStream incidents]
  (-> alerts
      (.leftJoin incidents
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
        alerts (build-stream builder ad-impressions-topic)
        incidents (build-stream builder ad-clicks-topic)]

    (-> (impressions-clicks-topology alerts incidents)
        (.to output-topic))
    builder))
