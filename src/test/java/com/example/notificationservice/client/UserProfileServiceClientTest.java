package com.example.notificationservice.client;

import com.example.notificationservice.dto.UserProfile;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.HttpStatus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UserProfileServiceClientTest {

    private static final String URL = "http://localhost:8080/mock-users";

    @Mock
    private RestTemplate restTemplate;

    private UserProfileServiceClient client;

    @BeforeEach
    void setUp() {
        client = new UserProfileServiceClient(restTemplate, URL);
    }

    @Test
    void returnsProfileFromUserProfileService() {
        UserProfile expected = new UserProfile("u1", "u1@example.com", "+1-555-0100");
        when(restTemplate.getForObject(eq(URL + "/{userId}"), eq(UserProfile.class), eq("u1"))).thenReturn(expected);

        assertThat(client.getUserProfile("u1")).isEqualTo(expected);
    }

    @Test
    void propagatesHttpErrorSoTheCircuitBreakerCanRecordIt() {
        when(restTemplate.getForObject(eq(URL + "/{userId}"), eq(UserProfile.class), eq("u1")))
                .thenThrow(new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR));

        assertThatThrownBy(() -> client.getUserProfile("u1")).isInstanceOf(HttpServerErrorException.class);
    }

    @Test
    void propagatesTimeoutsSoTheCircuitBreakerCanRecordThem() {
        when(restTemplate.getForObject(eq(URL + "/{userId}"), eq(UserProfile.class), eq("u1")))
                .thenThrow(new ResourceAccessException("Read timed out"));

        assertThatThrownBy(() -> client.getUserProfile("u1")).isInstanceOf(ResourceAccessException.class);
    }

    @Test
    void treatsEmptyBodyAsFailure() {
        when(restTemplate.getForObject(eq(URL + "/{userId}"), eq(UserProfile.class), eq("u1"))).thenReturn(null);

        assertThatThrownBy(() -> client.getUserProfile("u1")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void fallbackReturnsEmptyProfileWhenCircuitIsOpen() {
        CircuitBreaker cb = CircuitBreaker.ofDefaults("test");
        cb.transitionToOpenState();
        CallNotPermittedException open = CallNotPermittedException.createCallNotPermittedException(cb);

        UserProfile profile = client.getUserProfileFallback("u1", open);

        assertThat(profile.getUserId()).isEqualTo("u1");
        assertThat(profile.hasContactDetails()).isFalse();
    }

    @Test
    void fallbackReturnsEmptyProfileWhenCallFailed() {
        UserProfile profile = client.getUserProfileFallback("u2", new ResourceAccessException("down"));

        assertThat(profile.getUserId()).isEqualTo("u2");
        assertThat(profile.hasContactDetails()).isFalse();
    }
}
