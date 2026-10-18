package com.example.notificationservice.service;

import com.example.notificationservice.dto.NotificationRequest;
import com.example.notificationservice.event.NotificationEvent;
import com.example.notificationservice.exception.NotificationPublishException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NotificationProducerServiceTest {

    @Mock
    private KafkaTemplate<String, NotificationEvent> kafkaTemplate;

    private NotificationProducerService producerService;

    @BeforeEach
    void setUp() {
        producerService = new NotificationProducerService(kafkaTemplate, 1_000);
    }

    @Test
    void publishesEventToTopicUsingUserIdAsKey() {
        when(kafkaTemplate.send(eq("notification_events"), eq("user-1"), any(NotificationEvent.class)))
                .thenReturn(CompletableFuture.completedFuture((SendResult<String, NotificationEvent>) null));

        producerService.publishNotificationEvent(new NotificationRequest("user-1", "Hello", "EMAIL"));

        ArgumentCaptor<NotificationEvent> captor = ArgumentCaptor.forClass(NotificationEvent.class);
        verify(kafkaTemplate).send(eq("notification_events"), eq("user-1"), captor.capture());
        NotificationEvent event = captor.getValue();
        assertThat(event.getEventId()).isNotBlank();
        assertThat(event.getUserId()).isEqualTo("user-1");
        assertThat(event.getMessage()).isEqualTo("Hello");
        assertThat(event.getNotificationType()).isEqualTo("EMAIL");
        assertThat(event.getTimestamp()).isNotNull();
    }

    @Test
    void generatesUniqueEventIdPerPublish() {
        when(kafkaTemplate.send(any(String.class), any(String.class), any(NotificationEvent.class)))
                .thenReturn(CompletableFuture.completedFuture((SendResult<String, NotificationEvent>) null));

        producerService.publishNotificationEvent(new NotificationRequest("u", "m", "SMS"));
        producerService.publishNotificationEvent(new NotificationRequest("u", "m", "SMS"));

        ArgumentCaptor<NotificationEvent> captor = ArgumentCaptor.forClass(NotificationEvent.class);
        verify(kafkaTemplate, org.mockito.Mockito.times(2)).send(any(String.class), any(String.class), captor.capture());
        assertThat(captor.getAllValues().get(0).getEventId()).isNotEqualTo(captor.getAllValues().get(1).getEventId());
    }

    @Test
    void throwsPublishExceptionWhenBrokerReportsFailure() {
        CompletableFuture<SendResult<String, NotificationEvent>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("broker down"));
        when(kafkaTemplate.send(any(String.class), any(String.class), any(NotificationEvent.class))).thenReturn(failed);

        assertThatThrownBy(() -> producerService.publishNotificationEvent(new NotificationRequest("u", "m", "PUSH")))
                .isInstanceOf(NotificationPublishException.class);
    }

    @Test
    void throwsPublishExceptionWhenSendThrowsSynchronously() {
        when(kafkaTemplate.send(any(String.class), any(String.class), any(NotificationEvent.class)))
                .thenThrow(new IllegalStateException("producer closed"));

        assertThatThrownBy(() -> producerService.publishNotificationEvent(new NotificationRequest("u", "m", "PUSH")))
                .isInstanceOf(NotificationPublishException.class);
    }

    @Test
    void throwsPublishExceptionWhenAcknowledgementTimesOut() {
        when(kafkaTemplate.send(any(String.class), any(String.class), any(NotificationEvent.class)))
                .thenReturn(new CompletableFuture<>()); // never completes

        NotificationProducerService fast = new NotificationProducerService(kafkaTemplate, 50);
        assertThatThrownBy(() -> fast.publishNotificationEvent(new NotificationRequest("u", "m", "PUSH")))
                .isInstanceOf(NotificationPublishException.class);
    }
}
