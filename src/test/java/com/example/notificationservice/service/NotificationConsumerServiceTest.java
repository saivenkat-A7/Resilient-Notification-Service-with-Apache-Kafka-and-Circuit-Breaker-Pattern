package com.example.notificationservice.service;

import com.example.notificationservice.client.UserProfileServiceClient;
import com.example.notificationservice.dto.UserProfile;
import com.example.notificationservice.event.NotificationEvent;
import com.example.notificationservice.store.NotificationRecord.Status;
import com.example.notificationservice.store.NotificationStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NotificationConsumerServiceTest {

    @Mock
    private UserProfileServiceClient client;

    private NotificationStore store;
    private NotificationConsumerService consumer;

    @BeforeEach
    void setUp() {
        store = new NotificationStore();
        consumer = new NotificationConsumerService(client, store);
    }

    private NotificationEvent event(String id) {
        return new NotificationEvent(id, "user-1", "Your order shipped", "EMAIL", LocalDateTime.now());
    }

    @Test
    void sendsNotificationWhenProfileHasContactDetails() {
        when(client.getUserProfile("user-1")).thenReturn(new UserProfile("user-1", "user-1@example.com", "+1-555"));

        consumer.consumeNotificationEvent(event("e1"));

        assertThat(store.findByUserId("user-1")).singleElement().satisfies(r -> {
            assertThat(r.status()).isEqualTo(Status.SENT);
            assertThat(r.recipientEmail()).isEqualTo("user-1@example.com");
            assertThat(r.message()).isEqualTo("Your order shipped");
        });
    }

    @Test
    void defersNotificationWhenFallbackProfileIsReturned() {
        when(client.getUserProfile("user-1")).thenReturn(new UserProfile("user-1", null, null));

        consumer.consumeNotificationEvent(event("e2"));

        assertThat(store.findByUserId("user-1")).singleElement()
                .satisfies(r -> assertThat(r.status()).isEqualTo(Status.DEFERRED));
    }

    @Test
    void ignoresDuplicateDeliveryOfTheSameEvent() {
        when(client.getUserProfile("user-1")).thenReturn(new UserProfile("user-1", "a@b.c", "1"));

        consumer.consumeNotificationEvent(event("same-id"));
        consumer.consumeNotificationEvent(event("same-id"));

        verify(client, times(1)).getUserProfile("user-1");
        assertThat(store.findAll()).hasSize(1);
    }

    @Test
    void doesNotMarkEventProcessedWhenClientThrows() {
        when(client.getUserProfile("user-1")).thenThrow(new IllegalStateException("boom"));

        try {
            consumer.consumeNotificationEvent(event("e3"));
        } catch (IllegalStateException ignored) {
            // expected: the Kafka error handler will retry
        }

        assertThat(store.isProcessed("e3")).isFalse();
    }
}
