package com.perks.stuart;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class BankReconciliation {

    private static final Logger logger = LoggerFactory.getLogger(BankReconciliation.class);


    public Schema buildProcessedRepaymentSchema() {
        List<Schema.Field> fields = new ArrayList<>();
        fields.add(new Schema.Field("id", Schema.create(Schema.Type.STRING), "id field", ""));
        fields.add(new Schema.Field("repayment_amount", Schema.create(Schema.Type.INT), "repayment amount field", ""));
        fields.add(new Schema.Field("transaction_amount", Schema.create(Schema.Type.INT), "transaction amount field", ""));
        fields.add(new Schema.Field("account", Schema.create(Schema.Type.INT), "account field", ""));

        return Schema.createRecord("RepaymentProcessedRecord",
                "The repayment processed schema record",
                "kafka.streams.example",
                false,
                fields);
    }

    public GenericData.Record buildProcessedRepaymentMessage(Object repayment,
                                                             Object transaction,
                                                             Schema proccessedRepaymentSchema) {

        GenericData.Record repaymentRecord = (GenericData.Record) repayment;
        GenericData.Record transactionRecord = (GenericData.Record) transaction;

        GenericRecordBuilder builder = new GenericRecordBuilder(proccessedRepaymentSchema);
        builder.set("id", repaymentRecord.get("id"));
        builder.set("repayment_amount", repaymentRecord.get("amount"));
        builder.set("transaction_amount", transactionRecord.get("amount"));
        builder.set("account", repaymentRecord.get("account"));
        return builder.build();
    }

    public StreamsBuilder topology(Serde<String> keySerdes, GenericAvroSerde valueSerdes) {

        var repaymentTopic = "repayment";
        var transactionTopic = "transaction";
        var processedRepaymentTopic = "processed-repayment";

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, GenericRecord> repaymentStream =
                builder.stream(repaymentTopic, Consumed.with(keySerdes, valueSerdes));
        KStream transactionStream =
                builder.stream(transactionTopic, Consumed.with(keySerdes, valueSerdes));

        repaymentStream
                .peek((key, value) -> logger.info("Input data Key: {}, Data: {}", key, value))
                .join(transactionStream, (repayment, transaction) ->
                                buildProcessedRepaymentMessage(repayment, transaction, buildProcessedRepaymentSchema()),
                        JoinWindows.of(1000), Joined.with(keySerdes, valueSerdes, valueSerdes))
                .peek((key, value) -> logger.info("Output Data Key: {}, Data: {}", key, value))
                .to(processedRepaymentTopic, Produced.with(keySerdes, valueSerdes));

        return builder;

    }

}
