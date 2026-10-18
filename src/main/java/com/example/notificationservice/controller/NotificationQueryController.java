package com.example.notificationservice.controller;

import com.example.notificationservice.store.NotificationRecord;
import com.example.notificationservice.store.NotificationStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Read-only view on the in-memory "sent" / "deferred" notification records (handy for demos and tests). */
@RestController
@RequestMapping("/api/notifications")
public class NotificationQueryController {

    private final NotificationStore store;

    public NotificationQueryController(NotificationStore store) {
        this.store = store;
    }

    @GetMapping("/records")
    public List<NotificationRecord> records(@RequestParam(value = "userId", required = false) String userId) {
        return userId == null ? store.findAll() : store.findByUserId(userId);
    }
}
