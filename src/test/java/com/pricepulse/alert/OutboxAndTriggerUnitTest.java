package com.pricepulse.alert;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pricepulse.PricePulseProperties;
import com.pricepulse.alert.mapper.OutboxEventMapper;
import com.pricepulse.alert.model.AlertTriggeredPayload;
import com.pricepulse.alert.model.Notification;
import com.pricepulse.alert.model.OutboxEvent;
import com.pricepulse.alert.model.OutboxStatus;
import com.pricepulse.alert.service.AlertMessageSender;
import com.pricepulse.alert.service.AlertNotificationConsumer;
import com.pricepulse.alert.service.NotificationService;
import com.pricepulse.alert.service.OutboxPublisher;
import com.pricepulse.alert.ws.NotificationWebSocketHandler;
import com.pricepulse.rule.model.Direction;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OutboxAndTriggerUnitTest {

    @Test
    void backoffIsContinuousAndCappedAtMaxBackoff() {
        PricePulseProperties props = new PricePulseProperties();
        props.getOutbox().setInitialBackoffSeconds(1);
        props.getOutbox().setMaxBackoffSeconds(30);

        OutboxPublisher publisher = new OutboxPublisher(
                mock(OutboxEventMapper.class),
                directTransactionTemplate(),
                mock(AlertMessageSender.class),
                props
        );

        assertEquals(1L, publisher.calculateBackoffSeconds(0));
        assertEquals(2L, publisher.calculateBackoffSeconds(1));
        assertEquals(4L, publisher.calculateBackoffSeconds(2));
        assertEquals(8L, publisher.calculateBackoffSeconds(3));
        assertEquals(16L, publisher.calculateBackoffSeconds(4));
        assertEquals(30L, publisher.calculateBackoffSeconds(5));
        assertEquals(30L, publisher.calculateBackoffSeconds(6));
        assertEquals(30L, publisher.calculateBackoffSeconds(12));
        assertEquals(30L, publisher.calculateBackoffSeconds(60));
    }

    @Test
    void publishBatchOnceUsesLeaseIdAndReleasesRemainingOnFailure() {
        PricePulseProperties props = new PricePulseProperties();
        props.getOutbox().setBatchSize(50);
        props.getOutbox().setLeaseSeconds(30);
        props.getOutbox().setInitialBackoffSeconds(1);
        props.getOutbox().setMaxBackoffSeconds(30);

        OutboxEventMapper mapper = mock(OutboxEventMapper.class);
        AlertMessageSender sender = mock(AlertMessageSender.class);
        OutboxPublisher publisher = new OutboxPublisher(mapper, directTransactionTemplate(), sender, props);

        OutboxEvent e1 = outboxEvent(10L, 100L, 0);
        OutboxEvent e2 = outboxEvent(11L, 101L, 0);
        when(mapper.selectPendingForUpdateSkipLocked(50)).thenReturn(List.of(e1, e2));
        when(mapper.lockEvents(anyList(), anyString(), eq(30))).thenReturn(2);
        when(mapper.markSent(anyLong(), anyString())).thenReturn(1);

        int sent = publisher.publishBatchOnce();
        assertEquals(2, sent);

        ArgumentCaptor<String> leaseCaptor = ArgumentCaptor.forClass(String.class);
        verify(mapper).lockEvents(eq(List.of(10L, 11L)), leaseCaptor.capture(), eq(30));
        String lease1 = leaseCaptor.getValue();
        assertNotNull(lease1);
        verify(mapper).markSent(10L, lease1);
        verify(mapper).markSent(11L, lease1);

        // Failure on second event marks it for retry and releases remaining events in batch
        OutboxEvent f1 = outboxEvent(20L, 200L, 0);
        OutboxEvent f2 = outboxEvent(21L, 201L, 2);
        OutboxEvent f3 = outboxEvent(22L, 202L, 0);
        when(mapper.selectPendingForUpdateSkipLocked(50)).thenReturn(List.of(f1, f2, f3));
        when(mapper.lockEvents(anyList(), anyString(), eq(30))).thenReturn(3);
        when(mapper.markRetry(anyLong(), anyString(), anyLong())).thenReturn(1);
        doNothing().when(sender).send(f1);
        doThrow(new IllegalStateException("broker down")).when(sender).send(f2);

        int sentBeforeFailure = publisher.publishBatchOnce();
        assertEquals(1, sentBeforeFailure);

        ArgumentCaptor<String> lease2Captor = ArgumentCaptor.forClass(String.class);
        verify(mapper).lockEvents(eq(List.of(20L, 21L, 22L)), lease2Captor.capture(), eq(30));
        String lease2 = lease2Captor.getValue();

        verify(sender, times(1)).send(f1);
        verify(sender, times(1)).send(f2);
        verify(sender, never()).send(f3);
        verify(mapper).markSent(20L, lease2);
        verify(mapper).markRetry(21L, lease2, 4L);
        verify(mapper).releaseLeaseWithDelay(22L, lease2, 4L);
    }

    @Test
    void consumerHandlesInsertDuplicateAndDbFailureWithManualAck() throws Exception {
        PricePulseProperties props = new PricePulseProperties();
        ObjectMapper objectMapper = new ObjectMapper();
        NotificationService notificationService = mock(NotificationService.class);
        NotificationWebSocketHandler wsHandler = mock(NotificationWebSocketHandler.class);

        AlertNotificationConsumer consumer = new AlertNotificationConsumer(
                props, objectMapper, notificationService, wsHandler, "127.0.0.1:29876");

        AlertTriggeredPayload payload = new AlertTriggeredPayload(
                500L, 10L, 1L, 1L, "AAPL", Direction.ABOVE,
                new BigDecimal("160.00"), new BigDecimal("165.00"),
                "2026-11-21T10:00:00", "AAPL triggered ABOVE 160.00 at 165.00"
        );
        MessageExt msg = new MessageExt();
        msg.setMsgId("msg-1");
        msg.setBody(objectMapper.writeValueAsString(payload).getBytes(StandardCharsets.UTF_8));

        Notification saved = new Notification();
        saved.setId(1L);
        saved.setTriggerId(500L);
        saved.setUserId(1L);
        saved.setStockId(1L);
        saved.setContent(payload.content());
        saved.setIsRead(false);

        when(notificationService.saveIdempotently(any()))
                .thenReturn(new NotificationService.SaveResult(saved, true))
                .thenReturn(new NotificationService.SaveResult(null, false))
                .thenThrow(new RuntimeException("mysql temporarily unavailable"));

        assertEquals(ConsumeConcurrentlyStatus.CONSUME_SUCCESS, consumer.handleMessages(List.of(msg), null));
        verify(wsHandler, times(1)).pushNotification(saved, "AAPL");
        assertEquals(1L, consumer.getConsumedCount());

        assertEquals(ConsumeConcurrentlyStatus.CONSUME_SUCCESS, consumer.handleMessages(List.of(msg), null));
        verify(wsHandler, times(1)).pushNotification(any(), anyString());
        assertEquals(1L, consumer.getDuplicateCount());

        assertEquals(ConsumeConcurrentlyStatus.RECONSUME_LATER, consumer.handleMessages(List.of(msg), null));
        verify(wsHandler, times(1)).pushNotification(any(), anyString());
        assertEquals(1L, consumer.getConsumeFailureCount());
    }

    private static OutboxEvent outboxEvent(long id, long aggregateId, int attempts) {
        OutboxEvent event = new OutboxEvent();
        event.setId(id);
        event.setAggregateId(aggregateId);
        event.setEventType("ALERT_TRIGGERED");
        event.setPayload("{\"triggerId\":" + aggregateId + "}");
        event.setStatus(OutboxStatus.PENDING);
        event.setAttempts(attempts);
        return event;
    }

    private static TransactionTemplate directTransactionTemplate() {
        return new TransactionTemplate() {
            @Override
            public <T> T execute(TransactionCallback<T> action) {
                AtomicReference<TransactionStatus> statusRef = new AtomicReference<>();
                return action.doInTransaction(statusRef.get());
            }
        };
    }
}
