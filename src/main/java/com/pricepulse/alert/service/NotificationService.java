package com.pricepulse.alert.service;

import com.pricepulse.alert.mapper.NotificationMapper;
import com.pricepulse.alert.model.AlertTriggeredPayload;
import com.pricepulse.alert.model.Notification;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class NotificationService {

    private final NotificationMapper notificationMapper;
    private final TransactionTemplate transactionTemplate;

    public NotificationService(NotificationMapper notificationMapper,
                               TransactionTemplate transactionTemplate) {
        this.notificationMapper = notificationMapper;
        this.transactionTemplate = transactionTemplate;
    }

    public SaveResult saveIdempotently(AlertTriggeredPayload payload) {
        SaveResult result = transactionTemplate.execute(status -> {
            Notification notification = new Notification();
            notification.setTriggerId(payload.triggerId());
            notification.setUserId(payload.userId());
            notification.setStockId(payload.stockId());
            notification.setContent(payload.content());
            notification.setIsRead(false);
            notification.setCreatedAt(LocalDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS));
            try {
                notificationMapper.insert(notification);
                return new SaveResult(notification, true);
            } catch (DuplicateKeyException ignored) {
                return new SaveResult(null, false);
            }
        });
        return result != null ? result : new SaveResult(null, false);
    }

    public List<Notification> listNotifications(Long userId, int limit) {
        int clampedLimit = Math.max(1, Math.min(limit, 200));
        if (userId != null) {
            return notificationMapper.findByUserId(userId, clampedLimit);
        }
        return notificationMapper.findRecentAll(clampedLimit);
    }

    public long getUnreadCount(Long userId) {
        return notificationMapper.countUnreadByUserId(userId);
    }

    public int markRead(Long notificationId) {
        return notificationMapper.markReadById(notificationId);
    }

    public int markAllRead(Long userId) {
        if (userId != null) {
            return notificationMapper.markAllReadByUserId(userId);
        }
        return notificationMapper.markAllRead();
    }

    public record SaveResult(Notification notification, boolean inserted) {
    }
}
