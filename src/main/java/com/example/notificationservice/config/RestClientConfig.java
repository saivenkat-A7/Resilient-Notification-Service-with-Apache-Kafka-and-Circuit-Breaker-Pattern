package com.example.notificationservice.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;

@Configuration
public class RestClientConfig {

    /** Timeouts make a "slow" User Profile Service count as a failure for the circuit breaker. */
    @Bean
    public RestTemplate restTemplate(RestTemplateBuilder builder,
                                     @Value("${user.profile.service.connect-timeout-ms:1000}") long connectTimeoutMs,
                                     @Value("${user.profile.service.read-timeout-ms:2000}") long readTimeoutMs) {
        return builder
                .setConnectTimeout(Duration.ofMillis(connectTimeoutMs))
                .setReadTimeout(Duration.ofMillis(readTimeoutMs))
                .build();
    }
}
