package com.pricepulse.alert.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pricepulse.PricePulseProperties;
import com.pricepulse.alert.model.AlertTriggeredPayload;
import com.pricepulse.alert.ws.NotificationWebSocketHandler;
import jakarta.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class AlertNotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(AlertNotificationConsumer.class);

    private final PricePulseProperties properties;
    private final ObjectMapper objectMapper;
    private final NotificationService notificationService;
    private final NotificationWebSocketHandler webSocketHandler;
    private final String nameServer;

    private final AtomicLong consumedCount = new AtomicLong(0);
    private final AtomicLong duplicateCount = new AtomicLong(0);
    private final AtomicLong consumeFailureCount = new AtomicLong(0);

    private volatile DefaultMQPushConsumer consumer;

    public AlertNotificationConsumer(PricePulseProperties properties,
                                     ObjectMapper objectMapper,
                                     NotificationService notificationService,
                                     NotificationWebSocketHandler webSocketHandler,
                                     @Value("${rocketmq.name-server:127.0.0.1:29876}") String nameServer) {
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.notificationService = notificationService;
        this.webSocketHandler = webSocketHandler;
        this.nameServer = nameServer;
    }

    @EventListener(ApplicationReadyEvent.class)
    public synchronized void startOnReady() {
        PricePulseProperties.Mq mq = properties.getMq();
        if (!mq.isConsumerEnabled() || consumer != null) {
            return;
        }
        DefaultMQPushConsumer pushConsumer = new DefaultMQPushConsumer(mq.getConsumerGroup());
        pushConsumer.setNamesrvAddr(nameServer);
        pushConsumer.setConsumeThreadMin(mq.getConsumeThreadMin());
        pushConsumer.setConsumeThreadMax(mq.getConsumeThreadMax());
        pushConsumer.setMaxReconsumeTimes(mq.getMaxReconsumeTimes());
        pushConsumer.setConsumeMessageBatchMaxSize(1);
        pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        pushConsumer.setPollNameServerInterval(2_000);
        pushConsumer.setHeartbeatBrokerInterval(3_000);
        try {
            pushConsumer.subscribe(mq.getTopic(), "*");
            pushConsumer.registerMessageListener((MessageListenerConcurrently) this::handleMessages);
            pushConsumer.start();
            this.consumer = pushConsumer;
            log.info("Started AlertNotificationConsumer on topic={} group={} threads=[{}, {}]",
                    mq.getTopic(), mq.getConsumerGroup(), mq.getConsumeThreadMin(), mq.getConsumeThreadMax());
        } catch (MQClientException ex) {
            throw new IllegalStateException("Failed to start RocketMQ consumer for topic " + mq.getTopic(), ex);
        }
    }

    public ConsumeConcurrentlyStatus handleMessages(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        if (msgs == null || msgs.isEmpty()) {
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }
        for (MessageExt msg : msgs) {
            try {
                String body = new String(msg.getBody(), StandardCharsets.UTF_8);
                AlertTriggeredPayload payload = objectMapper.readValue(body, AlertTriggeredPayload.class);
                NotificationService.SaveResult saveResult = notificationService.saveIdempotently(payload);
                if (saveResult.inserted()) {
                    consumedCount.incrementAndGet();
                    webSocketHandler.pushNotification(saveResult.notification(), payload.symbol());
                } else {
                    duplicateCount.incrementAndGet();
                }
            } catch (Exception ex) {
                consumeFailureCount.incrementAndGet();
                log.warn("Failed consuming alert message msgId={} reconsumeTimes={}: {}",
                        msg.getMsgId(), msg.getReconsumeTimes(), ex.getMessage());
                if (context != null && properties.getMq().getReconsumeDelayLevel() > 0) {
                    context.setDelayLevelWhenNextConsume(properties.getMq().getReconsumeDelayLevel());
                }
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    @PreDestroy
    public synchronized void shutdown() {
        if (consumer != null) {
            consumer.shutdown();
            consumer = null;
        }
    }

    public DefaultMQPushConsumer getConsumer() {
        return consumer;
    }

    public long getConsumedCount() {
        return consumedCount.get();
    }

    public long getDuplicateCount() {
        return duplicateCount.get();
    }

    public long getConsumeFailureCount() {
        return consumeFailureCount.get();
    }

    public void resetMetricsForTest() {
        consumedCount.set(0);
        duplicateCount.set(0);
        consumeFailureCount.set(0);
    }
}
