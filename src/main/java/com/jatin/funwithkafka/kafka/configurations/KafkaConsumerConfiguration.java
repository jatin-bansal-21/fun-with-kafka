package com.jatin.funwithkafka.kafka.configurations;

import com.jatin.funwithkafka.kafka.constants.KafkaConstants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonDelegatingErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.ExponentialBackOff;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;

/**
 * @author jatin.bansal on 08/09/22
 */
@Configuration
@RequiredArgsConstructor
@EnableKafka
@Slf4j
public class KafkaConsumerConfiguration {
    @Value("${spring.kafka.properties.bootstrap.servers}")
    private String bootstrapServers;
    private final KafkaConstants kafkaConstants;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public Map<String, Object> consumerConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        configs.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule   required username='CYV6IVOUE44XRGKC'   password='7TF1vQygZiGE64j0g4djWJBJQ+ZSZOm8mAA+kb6wqswhJXJmnim5MAwv6XTBQkHQ';");
        configs.put(SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kafkaConstants.getMaxPollRecords());
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return configs;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean("kafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new
                ConcurrentKafkaListenerContainerFactory<>();
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setConcurrency(2);
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setConsumerRebalanceListener(new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.info("onPartitionsRevokedListener {}", partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                log.info("onPartitionsAssignedListener {}", partitions);
            }
        });
        DefaultErrorHandler smallBackOffErrorHandler =
                new DefaultErrorHandler(createDeadLetterPublishingRecoverer(), smallExponentialBackOff());
        CommonDelegatingErrorHandler commonDelegatingErrorHandler = new CommonDelegatingErrorHandler(smallBackOffErrorHandler);
        factory.setCommonErrorHandler(commonDelegatingErrorHandler);
        return factory;
    }

    public DeadLetterPublishingRecoverer createDeadLetterPublishingRecoverer() {
        return new DeadLetterPublishingRecoverer(kafkaTemplate,
                (consumerRecord, ex) -> {
                    log.info(String.format("[moving-to-dlt-because] %s", ex.getMessage()), ex);
                    return new TopicPartition(consumerRecord.topic() + ".DLT", 0);
                });
    }

    public ExponentialBackOff smallExponentialBackOff() {
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(4);
        backOff.setInitialInterval(Duration.ofSeconds(2).toMillis());
        backOff.setMultiplier(2);
        backOff.setMaxInterval(Duration.ofSeconds(30).toMillis());
        return backOff;
    }
}
