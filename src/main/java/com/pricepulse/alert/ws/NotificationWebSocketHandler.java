package com.pricepulse.alert.ws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pricepulse.alert.model.Notification;
import java.net.URI;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.springframework.web.util.UriComponentsBuilder;

@Component
public class NotificationWebSocketHandler extends TextWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(NotificationWebSocketHandler.class);

    private final ObjectMapper objectMapper;
    private final ConcurrentHashMap<Long, Set<WebSocketSession>> userSessions = new ConcurrentHashMap<>();
    private final Set<WebSocketSession> globalSessions = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<String, Long> sessionUserMap = new ConcurrentHashMap<>();

    public NotificationWebSocketHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        Long userId = extractUserId(session.getUri());
        if (userId != null) {
            userSessions.computeIfAbsent(userId, k -> ConcurrentHashMap.newKeySet()).add(session);
            sessionUserMap.put(session.getId(), userId);
        } else {
            globalSessions.add(session);
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        removeSession(session);
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        log.debug("WebSocket transport error for session {}: {}", session.getId(), exception.getMessage());
        removeSession(session);
    }

    public void pushNotification(Notification notification, String symbol) {
        if (notification == null) {
            return;
        }
        Set<WebSocketSession> targets = new LinkedHashSet<>();
        if (notification.getUserId() != null) {
            Set<WebSocketSession> perUser = userSessions.get(notification.getUserId());
            if (perUser != null) {
                targets.addAll(perUser);
            }
        }
        targets.addAll(globalSessions);
        if (targets.isEmpty()) {
            return;
        }

        String json = serializeMessage(new NotificationWsMessage(
                notification.getId(),
                notification.getTriggerId(),
                notification.getUserId(),
                notification.getStockId(),
                symbol,
                notification.getContent(),
                Boolean.TRUE.equals(notification.getIsRead()),
                notification.getCreatedAt() != null ? notification.getCreatedAt().toString() : null
        ));

        TextMessage message = new TextMessage(json);
        for (WebSocketSession session : targets) {
            if (!session.isOpen()) {
                removeSession(session);
                continue;
            }
            try {
                synchronized (session) {
                    if (session.isOpen()) {
                        session.sendMessage(message);
                    }
                }
            } catch (Exception ex) {
                log.warn("Failed sending WebSocket notification to session {}: {}",
                        session.getId(), ex.getMessage());
                removeSession(session);
            }
        }
    }

    public int getActiveSessionCount() {
        int userCount = userSessions.values().stream()
                .mapToInt(Set::size)
                .sum();
        return globalSessions.size() + userCount;
    }

    private void removeSession(WebSocketSession session) {
        if (session == null) {
            return;
        }
        globalSessions.remove(session);
        Long userId = sessionUserMap.remove(session.getId());
        if (userId != null) {
            Set<WebSocketSession> sessions = userSessions.get(userId);
            if (sessions != null) {
                sessions.remove(session);
                if (sessions.isEmpty()) {
                    userSessions.remove(userId, sessions);
                }
            }
        }
    }

    private Long extractUserId(URI uri) {
        if (uri == null || uri.getQuery() == null) {
            return null;
        }
        String raw = UriComponentsBuilder.fromUri(uri).build().getQueryParams().getFirst("userId");
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            long parsed = Long.parseLong(raw.trim());
            return parsed > 0 ? parsed : null;
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private String serializeMessage(NotificationWsMessage payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize WebSocket notification", e);
        }
    }

    public record NotificationWsMessage(
            Long id,
            Long triggerId,
            Long userId,
            Long stockId,
            String symbol,
            String content,
            boolean isRead,
            String createdAt
    ) {
    }
}
