(ns kafka-streams-example.processor-api-rekey
  (:require [clojure.tools.logging :as log]
            [kafka-streams-example.serdes.json-serdes :refer [json-serde]])
  (:import (org.apache.kafka.streams.processor Processor
                                               ProcessorSupplier)
           org.apache.kafka.common.serialization.Serdes))

(defn re-key-stream-processor
  "A generic processor that rekeys messages"
  [re-key-fn sink]
  (let [context (atom nil)]
    (reify
      Processor
      (close [_])
      (init [_ processor-context]
        (reset! context processor-context))
      (process [_ _ message]
        (re-key-fn @context sink message)))))

(defn re-key-stream-processor-supplier
  [re-key-fn sink]
  (reify
    ProcessorSupplier
    (get [_] (re-key-stream-processor re-key-fn sink))))

(defn re-key-trades
  "Function provided to rekey"
  [context sink {:keys [trade-id] :as message}]
  (log/infof "Sending on rekey trade Key: %s Message %s" trade-id message)
  (.forward context trade-id message sink))

(defn trade-rekey-topology
  "Topology that takes a trade and rekeys it by the provided function"
  [builder]
  (-> builder
      (.addSource "TradeInputs"
                  (.deserializer (Serdes/String))
                  (.deserializer json-serde)
                  (into-array String ["trade-input-topic"]))
      (.addProcessor "ReKeyTrades"
                     (re-key-stream-processor-supplier re-key-trades "TradeSinkByTradeId")
                     (into-array String ["TradeInputs"]))
      (.addSink "TradeSinkByTradeId"
                "trades-by-trade-id"
                (.serializer (Serdes/String))
                (.serializer json-serde)
                (into-array String ["ReKeyTrades"]))))