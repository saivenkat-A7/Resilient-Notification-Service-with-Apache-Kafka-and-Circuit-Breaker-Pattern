package com.example.notificationservice.mock;

import com.example.notificationservice.dto.UserProfile;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simulated external User Profile Service, hosted inside the Notification Service.
 * <p>
 * Behaviour can be controlled
 * <ul>
 *   <li>per request: {@code GET /mock-users/{id}?fail=true&delay=3000}</li>
 *   <li>globally at runtime (affects the Kafka consumer's calls too):
 *       {@code POST /mock-users/admin/config?fail=true&delay=0}</li>
 *   <li>at start-up: {@code MOCK_USER_PROFILE_FAIL} / {@code MOCK_USER_PROFILE_DELAY_MS}</li>
 * </ul>
 */
@RestController
@RequestMapping("/mock-users")
public class MockUserProfileServiceController {

    private final AtomicBoolean defaultFail;
    private final AtomicLong defaultDelayMs;
    private final AtomicInteger invocations = new AtomicInteger();
    private final boolean initialFail;
    private final long initialDelayMs;

    public MockUserProfileServiceController(@Value("${mock.user-profile.fail:false}") boolean fail,
                                            @Value("${mock.user-profile.delay-ms:0}") long delayMs) {
        this.initialFail = fail;
        this.initialDelayMs = delayMs;
        this.defaultFail = new AtomicBoolean(fail);
        this.defaultDelayMs = new AtomicLong(delayMs);
    }

    @GetMapping("/{userId}")
    public ResponseEntity<UserProfile> getUserProfile(@PathVariable String userId,
                                                      @RequestParam(value = "fail", required = false) Boolean fail,
                                                      @RequestParam(value = "delay", required = false) Long delayMs)
            throws InterruptedException {
        invocations.incrementAndGet();
        boolean shouldFail = fail != null ? fail : defaultFail.get();
        long delay = delayMs != null ? delayMs : defaultDelayMs.get();
        if (delay > 0) {
            Thread.sleep(delay);
        }
        if (shouldFail) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
        return ResponseEntity.ok(new UserProfile(userId, userId + "@example.com", "+1-555-0100"));
    }

    @PostMapping("/admin/config")
    public Map<String, Object> configure(@RequestParam(value = "fail", required = false) Boolean fail,
                                         @RequestParam(value = "delay", required = false) Long delayMs) {
        if (fail != null) {
            defaultFail.set(fail);
        }
        if (delayMs != null) {
            defaultDelayMs.set(Math.max(0, delayMs));
        }
        return currentConfig();
    }

    @GetMapping("/admin/config")
    public Map<String, Object> currentConfig() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("fail", defaultFail.get());
        config.put("delayMs", defaultDelayMs.get());
        config.put("invocations", invocations.get());
        return config;
    }

    /** Programmatic access for tests. */
    public void configure(boolean fail, long delayMs) {
        defaultFail.set(fail);
        defaultDelayMs.set(delayMs);
    }

    public int getInvocationCount() {
        return invocations.get();
    }

    public void reset() {
        defaultFail.set(initialFail);
        defaultDelayMs.set(initialDelayMs);
        invocations.set(0);
    }
}
