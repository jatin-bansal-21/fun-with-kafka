package com.jatin.funwithkafka.kafka.constants;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @author jatin.bansal
 */

@Data
@Component
@ConfigurationProperties(prefix = "kafka")
public class KafkaConstants {

    private Integer maxPollRecords;
}
