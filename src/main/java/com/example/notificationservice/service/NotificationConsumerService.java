package com.example.notificationservice.service;

import com.example.notificationservice.client.UserProfileServiceClient;
import com.example.notificationservice.dto.UserProfile;
import com.example.notificationservice.event.NotificationEvent;
import com.example.notificationservice.event.NotificationTopics;
import com.example.notificationservice.store.NotificationRecord;
import com.example.notificationservice.store.NotificationRecord.Status;
import com.example.notificationservice.store.NotificationStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Slf4j
@Service
public class NotificationConsumerService {

    private final UserProfileServiceClient userProfileServiceClient;
    private final NotificationStore store;

    public NotificationConsumerService(UserProfileServiceClient userProfileServiceClient, NotificationStore store) {
        this.userProfileServiceClient = userProfileServiceClient;
        this.store = store;
    }

    @KafkaListener(topics = NotificationTopics.NOTIFICATION_EVENTS,
            groupId = "${spring.kafka.consumer.group-id:notification_group}")
    public void consumeNotificationEvent(NotificationEvent event) {
        log.info("Consumed event {} for user: {}, type: {}", event.getEventId(), event.getUserId(),
                event.getNotificationType());

        // Idempotency: Kafka is at-least-once, so a redelivered event must not be sent twice.
        if (store.isProcessed(event.getEventId())) {
            log.warn("Event {} was already processed - skipping duplicate", event.getEventId());
            return;
        }

        // Protected by the circuit breaker; returns an empty fallback profile when the service is down.
        UserProfile profile = userProfileServiceClient.getUserProfile(event.getUserId());

        if (profile != null && profile.hasContactDetails()) {
            store.add(record(event, profile, Status.SENT));
            log.info("NOTIFICATION SENT (simulated) -> type={}, user={}, email={}, phone={}, message=\"{}\", eventId={}",
                    event.getNotificationType(), event.getUserId(), profile.getEmail(), profile.getPhoneNumber(),
                    event.getMessage(), event.getEventId());
        } else {
            store.add(record(event, profile, Status.DEFERRED));
            log.warn("NOTIFICATION DEFERRED -> user profile unavailable for user={}, type={}, message=\"{}\", eventId={}",
                    event.getUserId(), event.getNotificationType(), event.getMessage(), event.getEventId());
        }
    }

    private NotificationRecord record(NotificationEvent event, UserProfile profile, Status status) {
        return new NotificationRecord(event.getEventId(), event.getUserId(), event.getNotificationType(),
                event.getMessage(), profile == null ? null : profile.getEmail(),
                profile == null ? null : profile.getPhoneNumber(), status, Instant.now());
    }
}
