package com.example.notificationservice.integration;

import com.example.notificationservice.dto.NotificationRequest;
import com.example.notificationservice.mock.MockUserProfileServiceController;
import com.example.notificationservice.store.NotificationRecord;
import com.example.notificationservice.store.NotificationRecord.Status;
import com.example.notificationservice.store.NotificationStore;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end: POST /api/notifications/send -> Kafka (embedded broker) -> consumer -> User Profile Service
 * (mock endpoint over HTTP, protected by the circuit breaker) -> "notification sent".
 */
@SpringBootTest(webEnvironment = WebEnvironment.DEFINED_PORT)
@EmbeddedKafka(partitions = 1, topics = "notification_events",
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@DirtiesContext
class KafkaFlowIntegrationTest {

    private static final int PORT = TestSupport.freePort();

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("server.port", () -> PORT);
        registry.add("user.profile.service.url", () -> "http://localhost:" + PORT + "/mock-users");
    }

    @Autowired
    private TestRestTemplate rest;
    @Autowired
    private NotificationStore store;
    @Autowired
    private MockUserProfileServiceController mock;
    @Autowired
    private CircuitBreakerRegistry registry;

    @AfterEach
    void resetState() {
        mock.reset();
        registry.circuitBreaker("userProfileService").reset();
    }

    private ResponseEntity<String> send(String userId, String message, String type) {
        return rest.postForEntity("/api/notifications/send", new NotificationRequest(userId, message, type),
                String.class);
    }

    private List<NotificationRecord> awaitRecords(String userId) {
        await().atMost(Duration.ofSeconds(30)).until(() -> !store.findByUserId(userId).isEmpty());
        return store.findByUserId(userId);
    }

    @Test
    void notificationFlowsFromRestApiThroughKafkaToConsumer() {
        String userId = "user-" + UUID.randomUUID();

        ResponseEntity<String> response = send(userId, "Your order has shipped", "EMAIL");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        List<NotificationRecord> records = awaitRecords(userId);
        assertThat(records).hasSize(1);
        assertThat(records.get(0).status()).isEqualTo(Status.SENT);
        assertThat(records.get(0).recipientEmail()).isEqualTo(userId + "@example.com");
        assertThat(records.get(0).message()).isEqualTo("Your order has shipped");
        assertThat(records.get(0).notificationType()).isEqualTo("EMAIL");
    }

    @Test
    void invalidRequestIsRejectedAndNothingIsPublished() {
        ResponseEntity<String> response = send("", "msg", "EMAIL");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void notificationIsDeferredViaFallbackWhenUserProfileServiceFails() {
        mock.configure(true, 0);
        String userId = "user-" + UUID.randomUUID();

        ResponseEntity<String> response = send(userId, "Payment received", "SMS");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED); // the API stays available
        List<NotificationRecord> records = awaitRecords(userId);
        assertThat(records.get(0).status()).isEqualTo(Status.DEFERRED);
        assertThat(records.get(0).recipientEmail()).isNull();
    }

    @Test
    void multipleEventsForTheSameUserAreAllProcessed() {
        String userId = "user-" + UUID.randomUUID();

        for (int i = 0; i < 5; i++) {
            assertThat(send(userId, "msg-" + i, "PUSH").getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        }

        await().atMost(Duration.ofSeconds(30)).until(() -> store.findByUserId(userId).size() == 5);
        assertThat(store.findByUserId(userId)).extracting(NotificationRecord::message)
                .containsExactly("msg-0", "msg-1", "msg-2", "msg-3", "msg-4"); // same key => same partition => ordered
    }
}
