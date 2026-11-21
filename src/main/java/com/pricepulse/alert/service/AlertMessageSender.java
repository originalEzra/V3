package com.pricepulse.alert.service;

import com.pricepulse.alert.model.OutboxEvent;

public interface AlertMessageSender {

    void send(OutboxEvent event);
}
