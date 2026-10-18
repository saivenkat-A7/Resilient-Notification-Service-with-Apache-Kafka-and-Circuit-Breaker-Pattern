package com.example.notificationservice.store;

import java.time.Instant;

public record NotificationRecord(
        String eventId,
        String userId,
        String notificationType,
        String message,
        String recipientEmail,
        String recipientPhone,
        Status status,
        Instant processedAt) {

    public enum Status {
        /** Contact details were available; the (simulated) notification was sent. */
        SENT,
        /** The User Profile Service was unavailable (fallback used); the notification was deferred. */
        DEFERRED
    }
}
