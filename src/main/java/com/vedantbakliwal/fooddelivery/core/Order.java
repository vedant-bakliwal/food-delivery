package com.vedantbakliwal.fooddelivery.core;

import java.time.Instant;
import java.util.List;

/** An order. {@code version} increases on every change and guards concurrent updates. */
public record Order(String orderId, String customerId, String restaurantId, String zone, List<LineItem> items,
                    long totalCents, OrderStatus status, String courierId, String rejectionReason,
                    long version, Instant createdAt, Instant updatedAt) {

    public Order moveTo(OrderStatus next, String courier, String reason, Instant now) {
        if (!status.canMoveTo(next)) {
            throw new IllegalStateException("Cannot move order " + orderId + " from " + status + " to " + next);
        }
        return new Order(orderId, customerId, restaurantId, zone, items, totalCents, next,
                courier != null ? courier : courierId, reason, version + 1, createdAt, now);
    }
}
