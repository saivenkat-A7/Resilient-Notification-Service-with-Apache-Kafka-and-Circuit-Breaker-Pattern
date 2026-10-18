package com.example.notificationservice.service;

import com.example.notificationservice.dto.NotificationRequest;
import com.example.notificationservice.event.NotificationEvent;
import com.example.notificationservice.event.NotificationTopics;
import com.example.notificationservice.exception.NotificationPublishException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Service
public class NotificationProducerService {

    private static final String TOPIC = NotificationTopics.NOTIFICATION_EVENTS;
    private static final long DEFAULT_SEND_TIMEOUT_MS = 15_000;

    private final KafkaTemplate<String, NotificationEvent> kafkaTemplate;
    private final long sendTimeoutMs;

    @Autowired
    public NotificationProducerService(KafkaTemplate<String, NotificationEvent> kafkaTemplate,
                                       @Value("${app.kafka.producer.send-timeout-ms:15000}") long sendTimeoutMs) {
        this.kafkaTemplate = kafkaTemplate;
        this.sendTimeoutMs = sendTimeoutMs;
    }

    public NotificationProducerService(KafkaTemplate<String, NotificationEvent> kafkaTemplate) {
        this(kafkaTemplate, DEFAULT_SEND_TIMEOUT_MS);
    }

    /**
     * Publishes the request as a {@link NotificationEvent}, keyed by userId so that all events of
     * one user land on the same partition (ordering per user).
     * <p>
     * The producer itself retries transient broker errors (see {@code spring.kafka.producer.retries}).
     * We wait for the broker acknowledgement so that a 202 is only returned once the event is
     * really stored in Kafka; if the broker stays unavailable a {@link NotificationPublishException}
     * is raised (mapped to HTTP 503) instead of silently losing the notification.
     */
    public void publishNotificationEvent(NotificationRequest request) {
        NotificationEvent event = new NotificationEvent(
                UUID.randomUUID().toString(), request.getUserId(), request.getMessage(),
                request.getNotificationType(), LocalDateTime.now());

        CompletableFuture<SendResult<String, NotificationEvent>> future;
        try {
            future = kafkaTemplate.send(TOPIC, event.getUserId(), event); // userId as key for ordering
        } catch (RuntimeException e) {
            throw new NotificationPublishException("Failed to publish event " + event.getEventId(), e);
        }
        if (future == null) {
            log.warn("KafkaTemplate returned no future for event {}", event.getEventId());
            return;
        }

        try {
            SendResult<String, NotificationEvent> result = future.get(sendTimeoutMs, TimeUnit.MILLISECONDS);
            if (result != null && result.getRecordMetadata() != null) {
                log.info("Published event {} for user {} to {}-{}@{}", event.getEventId(), event.getUserId(),
                        result.getRecordMetadata().topic(), result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.info("Published event {} for user {}", event.getEventId(), event.getUserId());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new NotificationPublishException("Interrupted while publishing event " + event.getEventId(), e);
        } catch (ExecutionException | TimeoutException e) {
            log.error("Kafka publish failed for event {}: {}", event.getEventId(), e.toString());
            throw new NotificationPublishException("Failed to publish event " + event.getEventId(), e);
        }
    }
}
