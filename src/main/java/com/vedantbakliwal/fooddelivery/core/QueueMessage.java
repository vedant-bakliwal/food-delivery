package com.vedantbakliwal.fooddelivery.core;

/** A received message. {@code receiveCount} counts deliveries, including this one. */
public record QueueMessage(String messageId, String receipt, OrderEvent event, String traceparent, int receiveCount) {
}
