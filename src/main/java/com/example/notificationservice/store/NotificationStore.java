package com.example.notificationservice.store;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Bounded, thread-safe in-memory store. Holds the notification records ("sent" or "deferred")
 * and the ids of already processed events, which makes the consumer idempotent when Kafka
 * redelivers a message (at-least-once delivery).
 */
@Component
public class NotificationStore {

    private static final int MAX_RECORDS = 1_000;
    private static final int MAX_PROCESSED_IDS = 10_000;

    private final List<NotificationRecord> records = new ArrayList<>();
    private final Map<String, Boolean> processedEventIds = new LinkedHashMap<>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return size() > MAX_PROCESSED_IDS;
        }
    };

    public synchronized boolean isProcessed(String eventId) {
        return eventId != null && processedEventIds.containsKey(eventId);
    }

    public synchronized void add(NotificationRecord record) {
        if (record.eventId() != null) {
            processedEventIds.put(record.eventId(), Boolean.TRUE);
        }
        records.add(record);
        if (records.size() > MAX_RECORDS) {
            Iterator<NotificationRecord> it = records.iterator();
            it.next();
            it.remove();
        }
    }

    public synchronized List<NotificationRecord> findAll() {
        return Collections.unmodifiableList(new ArrayList<>(records));
    }

    public synchronized List<NotificationRecord> findByUserId(String userId) {
        return records.stream().filter(r -> r.userId().equals(userId)).toList();
    }

    public synchronized void clear() {
        records.clear();
        processedEventIds.clear();
    }
}
