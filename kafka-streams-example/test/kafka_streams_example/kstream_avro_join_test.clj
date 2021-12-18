(ns kafka-streams-example.kstream-avro-join-test
  (:require [clojure.test :refer :all])
  (:require [kafka-streams-example.kstream-avro-join :as sut]
            [kafka-streams-example.test-support :as support])
  (:import (org.apache.kafka.streams TopologyTestDriver Topology KeyValue)
           (io.confluent.kafka.schemaregistry.client MockSchemaRegistryClient)
           (io.confluent.kafka.streams.serdes.avro GenericAvroSerde)
           (org.apache.kafka.common.serialization Serdes)
           (org.apache.avro Schema$Field Schema Schema$Type)
           (java.util ArrayList)
           (org.apache.avro.generic GenericRecordBuilder)))

(defn build-transaction-schema
  []
  (let [fields (doto (ArrayList.)
                 (.add (Schema$Field. "id" (Schema/create (Schema$Type/STRING)) "id field" ""))
                 (.add (Schema$Field. "amount" (Schema/create (Schema$Type/INT)) "id field" 0)))]
    (doto (Schema/createRecord "TransactionRecord"
                               "The transaction schema record"
                               "kafka.streams.example"
                               false
                               fields))))

(defn build-repayment-schema
  []
  (let [fields (doto (ArrayList.)
                 (.add (Schema$Field. "id" (Schema/create (Schema$Type/STRING)) "id field" ""))
                 (.add (Schema$Field. "amount" (Schema/create (Schema$Type/INT)) "id field" 0))
                 (.add (Schema$Field. "account" (Schema/create (Schema$Type/INT)) "id field" 0)))]
    (doto (Schema/createRecord "RepaymentRecord"
                               "The repayment schema record"
                               "kafka.streams.example"
                               false
                               fields))))

(defn build-repayment-record
  [data ^Schema schema]
  (let [^GenericRecordBuilder builder (GenericRecordBuilder. schema)]
    (.set builder "id" (:id data))
    (.set builder "amount" (:amount data))
    (.set builder "account" (:account data))
    (.build builder)))

(defn build-transaction-record
  [data ^Schema schema]
  (let [^GenericRecordBuilder builder (GenericRecordBuilder. schema)]
    (.set builder "id" (:id data))
    (.set builder "amount" (:amount data))
    (.build builder)))

(deftest join-repayment-transaction-topology-test
  (testing "Joining a repayment and transaction"
    (let [repayment-id "1234"
          transaction-id "1234"
          amount 10
          repayment {:id      repayment-id
                     :amount  amount
                     :account 10241241}
          transaction {:id     transaction-id
                       :amount amount}

          schema-registry (MockSchemaRegistryClient.)
          serdes-config {"schema.registry.url" "http://localhost"}
          key-serdes (. Serdes String)
          value-serdes (doto
                         (GenericAvroSerde. schema-registry)
                         (.configure serdes-config false))
          key-serializer (.serializer key-serdes)
          key-deserializer (.deserializer key-serdes)
          value-serializer (.serializer value-serdes)
          value-deserializer (.deserializer value-serdes)
          ^Topology topology (.build (sut/repayment-transaction-topology
                                       key-serdes
                                       value-serdes))
          topology-test-driver (TopologyTestDriver. topology (support/properties "repayments-application"))
          repayment-topic (.createInputTopic topology-test-driver "repayment" key-serializer value-serializer)
          transaction-topic (.createInputTopic topology-test-driver "transaction" key-serializer value-serializer)
          processed-repayment-topic (.createOutputTopic topology-test-driver "processed-repayment" key-deserializer value-deserializer)
          ]

      (.pipeInput transaction-topic transaction-id (build-transaction-record transaction (build-transaction-schema)))
      (.pipeInput repayment-topic repayment-id (build-repayment-record repayment (build-repayment-schema)))

      (let [^KeyValue output (.readKeyValue processed-repayment-topic)]
        (is (= repayment-id (.key output)))
        (is (= 10241241 (.get (.value output) "account")))
        (is (= repayment-id (.toString (.get (.value output) "id"))))
        (is (= 10 (.get (.value output) "repayment_amount")))
        (is (= 10 (.get (.value output) "transaction_amount"))))
      (.close topology-test-driver))))
