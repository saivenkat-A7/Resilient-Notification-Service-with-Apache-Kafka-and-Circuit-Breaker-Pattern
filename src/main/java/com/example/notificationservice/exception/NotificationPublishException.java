package com.example.notificationservice.exception;

/** Raised when a notification event could not be published to Kafka. */
public class NotificationPublishException extends RuntimeException {

    public NotificationPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
