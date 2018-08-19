(ns kafka-streams-example.processor-api-example
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log])
  (:import (org.apache.kafka.streams.processor Processor ProcessorContext
                                               ProcessorSupplier StateStoreSupplier)
           (org.apache.kafka.streams Topology)
           org.apache.kafka.common.serialization.Serdes
           (org.apache.kafka.streams.state Stores)))
;; inspired by https://docs.confluent.io/current/streams/developer-guide/processor-api.html

(defn add-word-count
  [word kvstore]
  (log/info "Calling add word count")
  (let [old-value (.get @kvstore word)]
    (if (nil? old-value)
      (do
        (log/infof "Entering initial value for %s " word)
        (.put @kvstore word (str 1)))
      (do
        (log/infof "Entering a updated value for word %s with old value %s" word old-value)
        ;; TODO fix horrible casting here
        (.put @kvstore word (str (inc (int old-value))))))))

(defn word-processor
  "Note the first argument is this so is ignored"
  [store-name]
  (let [store (atom {})
        context (atom nil)]
    (reify
      Processor
      (close [_])
      (init [this processor-context]
        (reset! context processor-context)
        (reset! store (.getStateStore @context store-name)) ;; does this assign the actual state store?
        )
      (process [_ key line]
        (log/infof "Process has been called with %s %s" key line)
        (let [words
              (-> line
                  (str/lower-case)
                  (str/split #" "))]
          (log/infof "Words is: %s" words)
          #_(map #(add-word-count % store) words)
          (add-word-count (first words) store)

          (log/infof "Current word value for %s word is :%s" (first words) (.get @store (first words)))
          ;; change so each word is forwarded each time - these map functions are not working
          (map (fn [word]
                 (log/info "Fowarding on")
                 (let [word-count (.get @store word)]
                   (.forward context word (str word-count))))
               words)

          ;; lets forward the first word
          (.forward @context (first words) (.get @store (first words))))
        ))))

(defn word-processor-supplier
  [store-name]
  (reify
    ProcessorSupplier
    (get [_] (word-processor store-name))))

(defn word-processor-topology
  []
  (log/info "Word Processor API Streaming Topology")
  (let [builder (Topology.)
        store-name "Counts"
        store (Stores/keyValueStoreBuilder
               (Stores/persistentKeyValueStore store-name)
               (Serdes/String)
               (Serdes/String))]
    (-> builder
        (.addSource "Source" (into-array String ["source-topic"]))
        (.addProcessor "Process"
                       (word-processor-supplier store-name)
                       (into-array String ["Source"]))
        ;; state store missing is this actually used? As the let has it?
        (.addStateStore store (into-array String ["Process"]))
        (.addSink "Sink" "sink-topic" (into-array String ["Process"]))
        )))
