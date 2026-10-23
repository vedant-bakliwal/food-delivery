package com.vedantbakliwal.fooddelivery.core;

import java.time.Instant;
import java.util.Map;

/**
 * An integration event, published once per state change. Consumers must tolerate duplicates
 * (delivery is at least once), so every event has a unique id and carries the order version.
 */
public record OrderEvent(String eventId, Type type, String orderId, long orderVersion, Instant occurredAt,
                         Map<String, String> data) {

    public enum Type { OrderPlaced, OrderAccepted, OrderRejected, CourierAssigned, OrderPickedUp, OrderDelivered }
}
