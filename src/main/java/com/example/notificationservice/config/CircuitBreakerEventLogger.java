package com.example.notificationservice.config;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/** Makes circuit breaker state transitions observable through the application log. */
@Slf4j
@Component
public class CircuitBreakerEventLogger {

    public CircuitBreakerEventLogger(CircuitBreakerRegistry registry) {
        registry.getAllCircuitBreakers().forEach(this::register);
        registry.getEventPublisher().onEntryAdded(event -> register(event.getAddedEntry()));
    }

    private void register(CircuitBreaker circuitBreaker) {
        circuitBreaker.getEventPublisher()
                .onStateTransition(e -> log.warn("[CIRCUIT BREAKER] '{}' state transition: {} -> {}",
                        e.getCircuitBreakerName(),
                        e.getStateTransition().getFromState(),
                        e.getStateTransition().getToState()))
                .onCallNotPermitted(e -> log.info(
                        "[CIRCUIT BREAKER] '{}' is OPEN - call not permitted, failing fast", e.getCircuitBreakerName()))
                .onError(e -> log.debug("[CIRCUIT BREAKER] '{}' recorded failure after {} ms: {}",
                        e.getCircuitBreakerName(), e.getElapsedDuration().toMillis(), e.getThrowable().toString()))
                .onSuccess(e -> log.debug("[CIRCUIT BREAKER] '{}' recorded success after {} ms",
                        e.getCircuitBreakerName(), e.getElapsedDuration().toMillis()));
        log.info("[CIRCUIT BREAKER] '{}' registered, initial state: {}", circuitBreaker.getName(),
                circuitBreaker.getState());
    }
}
