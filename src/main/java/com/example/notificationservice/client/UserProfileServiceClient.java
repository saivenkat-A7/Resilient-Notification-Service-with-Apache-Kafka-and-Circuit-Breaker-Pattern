package com.example.notificationservice.client;

import com.example.notificationservice.dto.UserProfile;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Slf4j
@Service
public class UserProfileServiceClient {

    public static final String CIRCUIT_BREAKER_NAME = "userProfileService";

    private final RestTemplate restTemplate;
    private final String userProfileServiceUrl;

    public UserProfileServiceClient(RestTemplate restTemplate,
                                    @Value("${user.profile.service.url}") String userProfileServiceUrl) {
        this.restTemplate = restTemplate;
        this.userProfileServiceUrl = userProfileServiceUrl;
    }

    @CircuitBreaker(name = CIRCUIT_BREAKER_NAME, fallbackMethod = "getUserProfileFallback")
    public UserProfile getUserProfile(String userId) {
        log.info("Attempting to call User Profile Service for user: {}", userId);
        UserProfile profile = restTemplate.getForObject(userProfileServiceUrl + "/{userId}", UserProfile.class, userId);
        if (profile == null) {
            throw new IllegalStateException("User Profile Service returned an empty body for user " + userId);
        }
        return profile;
    }

    /** Invoked when the call failed or the circuit is OPEN. Returns an empty profile (no contact details). */
    public UserProfile getUserProfileFallback(String userId, Throwable t) {
        if (t instanceof CallNotPermittedException) {
            log.warn("Circuit Breaker open, notification deferred (user: {}) - failing fast without a network call",
                    userId);
        } else {
            log.warn("User Profile Service call failed for user: {} ({}: {}) - notification deferred",
                    userId, t.getClass().getSimpleName(), t.getMessage());
        }
        return new UserProfile(userId, null, null);
    }
}
