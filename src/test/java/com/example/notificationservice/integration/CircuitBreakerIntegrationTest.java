package com.example.notificationservice.integration;

import com.example.notificationservice.client.UserProfileServiceClient;
import com.example.notificationservice.dto.UserProfile;
import com.example.notificationservice.mock.MockUserProfileServiceController;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreaker.State;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies the circuit breaker around the User Profile Service against the real (mock) HTTP endpoint:
 * CLOSED -> OPEN (fail fast + fallback) -> HALF_OPEN -> CLOSED, and HALF_OPEN -> OPEN again.
 * The Kafka listener is disabled here; only the client + breaker are under test.
 */
@SpringBootTest(webEnvironment = WebEnvironment.DEFINED_PORT, properties = {
        "spring.kafka.listener.auto-startup=false",
        "user.profile.service.read-timeout-ms=500",
        "resilience4j.circuitbreaker.instances.userProfileService.sliding-window-size=4",
        "resilience4j.circuitbreaker.instances.userProfileService.minimum-number-of-calls=4",
        "resilience4j.circuitbreaker.instances.userProfileService.failure-rate-threshold=50",
        "resilience4j.circuitbreaker.instances.userProfileService.wait-duration-in-open-state=1s",
        "resilience4j.circuitbreaker.instances.userProfileService.permitted-number-of-calls-in-half-open-state=2"
})
@EmbeddedKafka(partitions = 1, topics = "notification_events",
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@DirtiesContext
class CircuitBreakerIntegrationTest {

    private static final int PORT = TestSupport.freePort();

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("server.port", () -> PORT);
        registry.add("user.profile.service.url", () -> "http://localhost:" + PORT + "/mock-users");
    }

    @Autowired
    private CircuitBreakerRegistry registry;
    @Autowired
    private UserProfileServiceClient client;
    @Autowired
    private MockUserProfileServiceController mock;

    private CircuitBreaker circuitBreaker;
    private final List<String> transitions = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() {
        circuitBreaker = registry.circuitBreaker(UserProfileServiceClient.CIRCUIT_BREAKER_NAME);
        circuitBreaker.reset();
        mock.reset();
        circuitBreaker.getEventPublisher().onStateTransition(e -> transitions.add(e.getStateTransition().name()));
    }

    private void tripBreaker() {
        mock.configure(true, 0);
        for (int i = 0; i < 4; i++) {
            UserProfile profile = client.getUserProfile("fail-" + i);
            assertThat(profile.hasContactDetails()).as("fallback profile while failing").isFalse();
        }
        assertThat(circuitBreaker.getState()).isEqualTo(State.OPEN);
    }

    @Test
    void healthyServiceKeepsCircuitClosedAndReturnsRealProfile() {
        UserProfile profile = client.getUserProfile("alice");

        assertThat(profile.getEmail()).isEqualTo("alice@example.com");
        assertThat(circuitBreaker.getState()).isEqualTo(State.CLOSED);
    }

    @Test
    void failuresTripBreakerToOpenAndSubsequentCallsFailFastWithFallback() {
        tripBreaker();
        int callsBefore = mock.getInvocationCount();

        UserProfile profile = client.getUserProfile("while-open");

        assertThat(profile.getUserId()).isEqualTo("while-open");
        assertThat(profile.hasContactDetails()).isFalse();                        // fallback profile
        assertThat(mock.getInvocationCount()).isEqualTo(callsBefore);            // no network call was made
        assertThat(circuitBreaker.getMetrics().getNumberOfNotPermittedCalls()).isEqualTo(1);
        assertThat(transitions).containsExactly("CLOSED_TO_OPEN");
    }

    @Test
    void breakerRecoversThroughHalfOpenBackToClosed() {
        tripBreaker();
        mock.configure(false, 0); // the downstream service recovers

        await().atMost(Duration.ofSeconds(5)).until(() -> circuitBreaker.getState() == State.HALF_OPEN);

        assertThat(client.getUserProfile("probe-1").getEmail()).isEqualTo("probe-1@example.com");
        assertThat(client.getUserProfile("probe-2").getEmail()).isEqualTo("probe-2@example.com");

        assertThat(circuitBreaker.getState()).isEqualTo(State.CLOSED);
        assertThat(transitions).containsExactly("CLOSED_TO_OPEN", "OPEN_TO_HALF_OPEN", "HALF_OPEN_TO_CLOSED");
    }

    @Test
    void breakerReopensWhenProbeCallsFailInHalfOpen() {
        tripBreaker(); // downstream keeps failing

        await().atMost(Duration.ofSeconds(5)).until(() -> circuitBreaker.getState() == State.HALF_OPEN);
        client.getUserProfile("probe-1");
        client.getUserProfile("probe-2");

        assertThat(circuitBreaker.getState()).isEqualTo(State.OPEN);
        assertThat(transitions).containsExactly("CLOSED_TO_OPEN", "OPEN_TO_HALF_OPEN", "HALF_OPEN_TO_OPEN");
    }

    @Test
    void slowResponsesTimeOutAndTripTheBreaker() {
        mock.configure(false, 1_500); // slower than the 500 ms read timeout

        for (int i = 0; i < 4; i++) {
            assertThat(client.getUserProfile("slow-" + i).hasContactDetails()).isFalse();
        }

        assertThat(circuitBreaker.getState()).isEqualTo(State.OPEN);
    }
}
