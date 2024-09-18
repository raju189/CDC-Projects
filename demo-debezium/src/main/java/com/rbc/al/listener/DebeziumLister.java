package com.rbc.al.listener;

import com.rbc.al.model.Member;
import com.rbc.al.model.Payload;
import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.Field;
import static io.debezium.data.Envelope.FieldName.*;
import static java.util.stream.Collectors.toMap;

@Slf4j
@Component
public class DebeziumLister {

    private final Executor executor = Executors.newSingleThreadExecutor();

    private final DebeziumEngine<RecordChangeEvent<SourceRecord>>   debeziumEngine;

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${db.sync.topic:cdc_aldb_topic}")
    private String topicName;

    public DebeziumLister(Configuration customerConnectorConfiguration, KafkaTemplate<String, String> kafkaTemplate) {
        this.debeziumEngine = DebeziumEngine.create(
                ChangeEventFormat.of(Connect.class))
                .using(customerConnectorConfiguration.asProperties())
                .notifying(this::handleChangeEvent)
                .build();
        this.kafkaTemplate = kafkaTemplate;
    }

    private void handleChangeEvent(RecordChangeEvent<SourceRecord> sourceRecordRecordChangeEvent) {

        var sourceRecord = sourceRecordRecordChangeEvent.record();
        log.info("Key = {}, Value = {}", sourceRecord.key(), sourceRecord.value());
        var sourceRecordChangeValue= (Struct) sourceRecord.value();
        log.info("SourceRecordChangeValue = '{}'", sourceRecordChangeValue);
        if (sourceRecordChangeValue != null) {
            Envelope.Operation operation = Envelope.Operation.forCode((String) sourceRecordChangeValue.get(OPERATION));
            if(operation != Envelope.Operation.READ) {
                Payload payload = buildPayload(sourceRecordChangeValue, operation);
                log.info("Updated Data: {} with Operation: {}", payload, operation.name());
                kafkaTemplate.send(topicName, operation.name(), payload.toString());
            }
        }
    }

    private Payload buildPayload(Struct sourceRecordChangeValue, Envelope.Operation operation) {
        Map<String, Object> mapValues = getStringObjectMap((Struct) sourceRecordChangeValue.get(AFTER));

        Member after = new Member(
                Long.valueOf(mapValues.get("MemberID").toString()),
                mapValues.get("Name").toString(),
                mapValues.get("Address").toString(),
                mapValues.get("Email").toString()
                );

        mapValues = getStringObjectMap((Struct) sourceRecordChangeValue.get(BEFORE));

        Member before = new Member(
                Long.valueOf(mapValues.get("MemberID").toString()),
                mapValues.get("Name").toString(),
                mapValues.get("Address").toString(),
                mapValues.get("Email").toString()
        );

        return new Payload(before, after, operation.name());

    }


    private static Map<String, Object> getStringObjectMap(Struct struct) {
        Map<String, Object> payload = struct.schema().fields().stream()
                .map(Field::name)
                .filter(fieldName -> struct.get(fieldName) != null)
                .map(fieldName -> Pair.of(fieldName, struct.get(fieldName)))
                .collect(toMap(Pair::getKey, Pair::getValue));
        return payload;
    }

    @PostConstruct
    private void start() {
        this.executor.execute((debeziumEngine));
    }

    @PreDestroy
    private void stop() throws IOException {
        if(Objects.nonNull(this.debeziumEngine)){
            this.debeziumEngine.close();
        }
    }
}
