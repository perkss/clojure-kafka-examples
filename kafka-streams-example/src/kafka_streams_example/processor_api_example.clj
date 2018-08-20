(ns kafka-streams-example.processor-api-example
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log])
  (:import (org.apache.kafka.streams.processor Processor ProcessorContext
                                               ProcessorSupplier StateStoreSupplier
                                               PunctuationType Punctuator)
           (org.apache.kafka.streams Topology)
           org.apache.kafka.common.serialization.Serdes
           (org.apache.kafka.streams.state Stores)))

;; inspired by https://kafka.apache.org/11/documentation/streams/developer-guide/processor-api.html

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
        (.put @kvstore word (str (inc (Integer. old-value))))))))

(defn punctuator-forward-message
  [timestamp kvstore context]
  (reify
    Punctuator
    (punctuate [_ timestamp]
      (let [iter (iterator-seq (.all @kvstore))]
        (log/infof "Iter %s" (pr-str iter))
        (dorun (map #(.forward @context (.key %) (str (.value %))) iter))
        (.commit @context)))))

(defn word-processor
  "The first argument to reify method if this. Impleemt the Processor Java API"
  [store-name]
  (let [store (atom {})
        context (atom nil)
        timestamp 10]
    (reify
      Processor
      (close [_])
      (init [this processor-context]
        (reset! context processor-context)
        (reset! store (.getStateStore @context store-name)) ;; Assign the state store to the atom

        (.schedule @context
                   timestamp
                   PunctuationType/STREAM_TIME
                   (punctuator-forward-message timestamp store context)))
      (process [_ key line]
        (log/infof "Process has been called with %s %s" key line)
        (let [words
              (-> (str line)
                  (str/lower-case)
                  (str/split #" "))]

          (log/infof "Words is: %s" words)
          ;; Update the word count in the state store
          (dorun (map #(add-word-count % store) words))

          (log/infof "Current word value for %s word is :%s"
                     (first words)
                     (.get @store (first words))))))))

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
        ;; Remove the add state store and you will see my contribution to Kafka:
        ;; https://issues.apache.org/jira/browse/KAFKA-6659
        (.addStateStore store (into-array String ["Process"]))
        (.addSink "Sink" "sink-topic" (into-array String ["Process"])))))
