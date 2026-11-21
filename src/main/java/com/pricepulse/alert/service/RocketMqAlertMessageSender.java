package com.pricepulse.alert.service;

import com.pricepulse.PricePulseProperties;
import com.pricepulse.alert.model.OutboxEvent;
import jakarta.annotation.PostConstruct;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

@Component
public class RocketMqAlertMessageSender implements AlertMessageSender {

    private final RocketMQTemplate rocketMQTemplate;
    private final PricePulseProperties properties;

    public RocketMqAlertMessageSender(RocketMQTemplate rocketMQTemplate,
                                      PricePulseProperties properties) {
        this.rocketMQTemplate = rocketMQTemplate;
        this.properties = properties;
    }

    @PostConstruct
    void tuneProducer() {
        DefaultMQProducer producer = rocketMQTemplate.getProducer();
        if (producer != null) {
            producer.setPollNameServerInterval(2_000);
        }
    }

    @Override
    public void send(OutboxEvent event) {
        Message<String> msg = MessageBuilder.withPayload(event.getPayload())
                .setHeader(RocketMQHeaders.KEYS, String.valueOf(event.getAggregateId()))
                .build();
        SendResult result = rocketMQTemplate.syncSend(
                properties.getMq().getTopic(),
                msg,
                properties.getMq().getSendTimeoutMs()
        );
        if (result == null || result.getSendStatus() != SendStatus.SEND_OK) {
            throw new IllegalStateException("RocketMQ send did not return SEND_OK: "
                    + (result == null ? "null" : result.getSendStatus()));
        }
    }
}
