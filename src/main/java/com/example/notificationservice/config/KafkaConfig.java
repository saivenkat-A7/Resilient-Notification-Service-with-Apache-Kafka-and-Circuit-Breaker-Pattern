package com.example.notificationservice.config;

import com.example.notificationservice.event.NotificationTopics;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Slf4j
@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic notificationEventsTopic(@Value("${app.kafka.topic.partitions:1}") int partitions,
                                            @Value("${app.kafka.topic.replicas:1}") int replicas) {
        return TopicBuilder.name(NotificationTopics.NOTIFICATION_EVENTS)
                .partitions(partitions)
                .replicas(replicas)
                .build();
    }

    /**
     * Retries a failing listener invocation with exponential back-off. When all attempts are
     * exhausted the record is logged and skipped so that one poison message cannot block the
     * partition. (Spring Boot picks up this CommonErrorHandler bean for the listener container.)
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(
            @Value("${app.kafka.consumer.retry.max-attempts:3}") int maxAttempts,
            @Value("${app.kafka.consumer.retry.initial-interval-ms:500}") long initialIntervalMs,
            @Value("${app.kafka.consumer.retry.max-interval-ms:5000}") long maxIntervalMs) {
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(maxAttempts);
        backOff.setInitialInterval(initialIntervalMs);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(maxIntervalMs);
        ConsumerRecordRecoverer recoverer = (record, ex) -> log.error(
                "Giving up on record {}-{}@{} (key={}) after {} retries: {}",
                record.topic(), record.partition(), record.offset(), record.key(), maxAttempts, ex.toString());
        return new DefaultErrorHandler(recoverer, backOff);
    }
}
