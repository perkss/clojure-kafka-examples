(ns kafka-streams-example.kstream-avro-join-test
  (:require [clojure.test :refer :all])
  (:require [kafka-streams-example.kstream-avro-join :as sut])
  (:import (org.apache.kafka.streams StreamsConfig TopologyTestDriver)
           (org.apache.kafka.test TestUtils)
           (org.apache.kafka.streams.test ConsumerRecordFactory)
           (io.confluent.kafka.schemaregistry.client MockSchemaRegistryClient)
           (io.confluent.kafka.streams.serdes.avro GenericAvroSerde)
           (org.apache.kafka.common.serialization Serdes)
           (org.apache.avro Schema$Field Schema Schema$Type)
           (java.util ArrayList Properties)
           (org.apache.avro.generic GenericRecordBuilder)))

(def properties
  (let [properties (Properties.)]
    (.put properties StreamsConfig/APPLICATION_ID_CONFIG (str "repayments-application" (rand)))
    (.put properties StreamsConfig/PROCESSING_GUARANTEE_CONFIG StreamsConfig/EXACTLY_ONCE)
    (.put properties StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG (.getName (.getClass (Serdes/String))))
    (.put properties StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG (.getName (.getClass (Serdes/String))))
    (.put properties StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "dummy:9092")
    (.put properties "schema.registry.url" "http://localhost")
    (.put properties StreamsConfig/COMMIT_INTERVAL_MS_CONFIG (* 10 1000))
    (.put properties StreamsConfig/STATE_DIR_CONFIG (.getAbsolutePath (TestUtils/tempDirectory)))
    properties))

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
          factory (ConsumerRecordFactory. (.serializer key-serdes)
                                          (.serializer value-serdes))
          topology (.build (sut/repayment-transaction-topology
                            key-serdes
                            value-serdes))
          topology-test-driver (TopologyTestDriver. topology properties)
          repayment-topic "repayment"
          transaction-topic "transaction"
          processed-repayment-topic "processed-repayment"]

      (.pipeInput topology-test-driver (.create factory
                                                transaction-topic
                                                transaction-id
                                                (build-transaction-record transaction (build-transaction-schema))))
      (.pipeInput topology-test-driver (.create factory
                                                repayment-topic
                                                repayment-id
                                                (build-repayment-record repayment (build-repayment-schema))))

      (let [output (.readOutput topology-test-driver processed-repayment-topic
                                (.deserializer key-serdes)
                                (.deserializer value-serdes))]
        (is (= repayment-id (.key output)))
        (is (= 10241241 (.get (.value output) "account")))
        (is (= repayment-id (.toString (.get (.value output) "id"))))
        (is (= 10 (.get (.value output) "repayment_amount")))
        (is (= 10 (.get (.value output) "transaction_amount"))))
      (.close topology-test-driver))))
