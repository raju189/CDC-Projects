package com.rbc.al.service;

import com.rbc.al.model.Payload;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaProducerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${db.sync.topic:cdc_aldb_topic}")
    private String topicName;


    public void sendRecord(Payload payload){
        kafkaTemplate.send(topicName, payload.operation(), payload);
    }
}
