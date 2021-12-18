(ns kafka-streams-example.kstream-kstream-left-join-example
  (:require [clojure.tools.logging :as log])
  (:require [kafka-streams-example.utils :as kstream-utils])
  (:import [org.apache.kafka.streams.kstream JoinWindows KStream ValueJoiner]
           org.apache.kafka.streams.StreamsBuilder
           (java.time Duration)))

(defn impressions-clicks-topology
  [^KStream impressions ^KStream clicks]
  (-> impressions
      (.leftJoin clicks
                 (reify ValueJoiner
                   (apply [_ left right]
                     ((fn [impression-value click-value]
                        (log/infof "Received values impression value: %s click: %s" impression-value click-value)
                        (str impression-value "/" click-value))
                      left right)))
                 (. JoinWindows of (Duration/ofMillis (* 50 60 10000))))))

(defn builder-streaming-join-topology
  []
  (let [builder (StreamsBuilder.)
        ad-impressions-topic "adImpressions"
        ad-clicks-topic "adClicks"
        output-topic "output-topic"
        impressions (kstream-utils/build-stream builder ad-impressions-topic)
        clicks (kstream-utils/build-stream builder ad-clicks-topic)]

    (-> (impressions-clicks-topology impressions clicks)
        (kstream-utils/peek-stream)
        (.to output-topic))
    builder))
