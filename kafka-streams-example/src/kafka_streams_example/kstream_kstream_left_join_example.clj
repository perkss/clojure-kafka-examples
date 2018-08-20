(ns kafka-streams-example.kstream-kstream-left-join-example
  (:require [clojure.tools.logging :as log])
  (:import [org.apache.kafka.streams.kstream JoinWindows KStream ValueJoiner ForeachAction]
           org.apache.kafka.streams.StreamsBuilder))

(defn peek-stream
  [stream]
  (.peek stream
         (reify ForeachAction
           (apply [_ k v]
             (log/infof "Key: %s, Data: %s" k v)))))

(defn build-stream
  [builder input-topic]
  (.stream builder input-topic))

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
                 (. JoinWindows of (* 50 60 10000)))))

(defn builder-streaming-join-topology
  []
  (let [builder (StreamsBuilder.)
        ad-impressions-topic "adImpressions"
        ad-clicks-topic "adClicks"
        output-topic "output-topic"
        impressions (build-stream builder ad-impressions-topic)
        clicks (build-stream builder ad-clicks-topic)]

    (-> (impressions-clicks-topology impressions clicks)
        (peek-stream)
        (.to output-topic))
    builder))
