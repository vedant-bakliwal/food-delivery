package com.vedantbakliwal.fooddelivery.core;

/** Publishes to the order-events topic (SNS in AWS). */
public interface EventPublisher {

    void publish(OrderEvent event, String traceparent);
}
