(ns kafka-streams-example.kstream-avro-join
  (:require [clojure.tools.logging :as log])
  (:import (org.apache.kafka.streams StreamsBuilder)
           (org.apache.kafka.streams.kstream KStream ValueJoiner JoinWindows ForeachAction Consumed Joined Produced)
           (org.apache.kafka.common.serialization Serdes)
           (org.apache.avro Schema Schema$Field Schema$Type)
           (java.util ArrayList)
           (org.apache.avro.generic GenericRecordBuilder)))

(defn peek-stream
  [stream]
  (.peek stream
         (reify ForeachAction
           (apply [_ k v]
             (log/infof "Key: %s, Data: %s" k v)))))

(defn build-repayment-processed-schema
  []
  (let [fields (doto (ArrayList.)
                 (.add (Schema$Field. "id" (Schema/create (Schema$Type/STRING)) "id field" ""))
                 (.add (Schema$Field. "repayment_amount" (Schema/create (Schema$Type/INT)) "id field" 0))
                 (.add (Schema$Field. "transaction_amount" (Schema/create (Schema$Type/INT)) "id field" 0))
                 (.add (Schema$Field. "account" (Schema/create (Schema$Type/INT)) "id field" 0)))]
    (doto (Schema/createRecord "RepaymentProcessedRecord"
                               "The repayment processed schema record"
                               "kafka.streams.example"
                               false
                               fields))))

(defn build-repayment-processed-record
  [repayment transaction ^Schema schema]
  (let [^GenericRecordBuilder builder (GenericRecordBuilder. schema)]
    (.set builder "id" (.get repayment "id"))
    (.set builder "repayment_amount" (.get repayment "amount"))
    (.set builder "transaction_amount" (.get transaction "amount"))
    (.set builder "account" (.get repayment "account"))
    (.build builder)))

(defn ^KStream build-stream
  [^StreamsBuilder builder ^String input-topic
   key-serializer value-serializer]
  (.stream builder input-topic (Consumed/with key-serializer value-serializer)))

(defn join-repayment-transaction-topology
  [^KStream repayments ^KStream transactions value-serializer]
  (-> repayments
      (.join transactions
             (reify ValueJoiner
               (apply [_ left right]
                 ((fn [repayment-value transaction-value]
                    (build-repayment-processed-record repayment-value transaction-value (build-repayment-processed-schema)))
                  left right)))
             (. JoinWindows of 5000)
             (. Joined with (. Serdes String)
                value-serializer
                value-serializer))))

(defn repayment-transaction-topology
  [key-serializer value-serializer]
  (let [builder (StreamsBuilder.)
        repayment-topic "repayment"
        transaction-topic "transaction"
        processed-repayment-topic "processed-repayment"
        repayment-stream (build-stream builder repayment-topic key-serializer value-serializer)
        transaction-stream (build-stream builder transaction-topic key-serializer value-serializer)]

    (-> (join-repayment-transaction-topology repayment-stream transaction-stream value-serializer)
        (peek-stream)
        (.to processed-repayment-topic (Produced/with key-serializer value-serializer)))
    builder))
