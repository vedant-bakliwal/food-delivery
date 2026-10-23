package com.vedantbakliwal.fooddelivery.core;

/** An event saved in the same transaction as the order change, waiting to be published. */
public record OutboxRecord(OrderEvent event, String traceparent) {
}
