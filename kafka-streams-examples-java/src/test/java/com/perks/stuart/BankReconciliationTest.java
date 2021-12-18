package com.perks.stuart;

import io.confluent.common.utils.TestUtils;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BankReconciliationTest {

    private final Map configuration = Map.of(
            StreamsConfig.APPLICATION_ID_CONFIG, "reconciliation-topology",
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy-test-kafka",
            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName(),
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName(),
            StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath(),
            "schema.registry.url", "http://localhost");

    private Schema buildRepaymentSchema() {
        List<Schema.Field> fields = new ArrayList<>();
        fields.add(new Schema.Field("id", Schema.create(Schema.Type.STRING), "id field", ""));
        fields.add(new Schema.Field("amount", Schema.create(Schema.Type.INT), "transaction amount field", 0));
        fields.add(new Schema.Field("account", Schema.create(Schema.Type.INT), "account amount field", 0));

        return Schema.createRecord("RepaymentRecord",
                "The original repayent message schema record",
                "kafka.streams.example",
                false,
                fields);
    }

    private GenericData.Record buildRepaymentRecord(String repaymentId, int amount) {
        GenericRecordBuilder builder = new GenericRecordBuilder(buildRepaymentSchema());
        builder.set("id", repaymentId);
        builder.set("amount", amount);
        builder.set("account", 1242511);
        return builder.build();
    }

    private Schema buildTransactionSchema() {
        List<Schema.Field> fields = new ArrayList<>();
        fields.add(new Schema.Field("id", Schema.create(Schema.Type.STRING), "id field", ""));
        fields.add(new Schema.Field("amount", Schema.create(Schema.Type.INT), "transaction amount field", 0));

        return Schema.createRecord("TransactionRecord",
                "The original transaction message schema record",
                "kafka.streams.example",
                false,
                fields);
    }

    private GenericData.Record buildTransactionRecord(String transactionId, int amount) {
        GenericRecordBuilder builder = new GenericRecordBuilder(buildTransactionSchema());
        builder.set("id", transactionId);
        builder.set("amount", amount);
        return builder.build();
    }

    @Test
    void topology() {

        var schemaRegistry = new MockSchemaRegistryClient();
        var repaymentId = "1234A";
        var transactionId = "1234A";

        var amount = 10;

        Serde<String> keySerde = Serdes.String();
        GenericAvroSerde genericAvroSerde = new GenericAvroSerde(schemaRegistry);
        genericAvroSerde.configure(
                Map.of("schema.registry.url", "http://localhost"), false);

        BankReconciliation bankReconciliation = new BankReconciliation();

        var properties = new Properties();
        properties.putAll(configuration);

        TopologyTestDriver topologyTestDriver = new TopologyTestDriver(
                bankReconciliation.topology(keySerde, genericAvroSerde).build(),
                properties);
        var transactionTopic = topologyTestDriver.createInputTopic("transaction", keySerde.serializer(), genericAvroSerde.serializer());
        var repaymentTopic = topologyTestDriver.createInputTopic("repayment", keySerde.serializer(), genericAvroSerde.serializer());

        var processedRepaymentTopic = topologyTestDriver.createOutputTopic("processed-repayment", keySerde.deserializer(), genericAvroSerde.deserializer());

        transactionTopic.pipeInput(transactionId, buildTransactionRecord(transactionId, amount));

        repaymentTopic.pipeInput(repaymentId, buildRepaymentRecord(repaymentId, amount));

        KeyValue<String, GenericRecord> outputRecord = processedRepaymentTopic
                .readKeyValue();

        assertEquals(repaymentId, outputRecord.key);
        assertEquals(repaymentId, outputRecord.value.get("id").toString());
        assertEquals(amount, outputRecord.value.get("repayment_amount"));
        assertEquals(amount, outputRecord.value.get("transaction_amount"));


    }

}