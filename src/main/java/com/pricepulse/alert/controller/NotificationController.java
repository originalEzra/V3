package com.pricepulse.alert.controller;

import com.pricepulse.alert.model.Notification;
import com.pricepulse.alert.service.NotificationService;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/notifications")
public class NotificationController {

    private final NotificationService notificationService;

    public NotificationController(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @GetMapping
    public List<Notification> listNotifications(@RequestParam(required = false) Long userId,
                                                @RequestParam(defaultValue = "50") int limit) {
        return notificationService.listNotifications(userId, limit);
    }

    @GetMapping("/unread-count")
    public Map<String, Object> getUnreadCount(@RequestParam Long userId) {
        return Map.of(
                "userId", userId,
                "unreadCount", notificationService.getUnreadCount(userId)
        );
    }

    @PostMapping("/{id}/read")
    public Map<String, Object> markRead(@PathVariable Long id) {
        return Map.of("updated", notificationService.markRead(id));
    }

    @PostMapping("/read-all")
    public Map<String, Object> markAllRead(@RequestParam(required = false) Long userId) {
        return Map.of("updated", notificationService.markAllRead(userId));
    }
}
