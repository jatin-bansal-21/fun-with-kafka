package com.jatin.funwithkafka.internal.consumers;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @author jatin.bansal on 08/09/22
 */
@Component
@Slf4j
public class Listeners {

    @KafkaListener(topicPattern = ".*",
            groupId = "message_logger_group_id",
            containerFactory = "kafkaListenerContainerFactory",
            concurrency = "1")
    public void logMessage(ConsumerRecord<String, String> consumerRecord) {
        log.info("{}:{}", consumerRecord.key(), consumerRecord.value());
    }
}
