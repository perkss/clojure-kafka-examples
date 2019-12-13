(ns kafka-streams-example.utils
  (:require [clojure.tools.logging :as log])
  (:import (org.apache.kafka.streams.kstream KStream KTable ForeachAction)))

(defn ^KStream build-stream
  [builder input-topic]
  (.stream builder input-topic))

(defn ^KTable build-table
  [builder input-topic]
  (.table builder input-topic))

(defn peek-stream
  [stream]
  (.peek stream
         (reify ForeachAction
           (apply [_ k v]
             (log/infof "Key: %s, Data: %s" k v)))))