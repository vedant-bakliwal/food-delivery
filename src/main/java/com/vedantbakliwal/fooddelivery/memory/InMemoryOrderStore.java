package com.vedantbakliwal.fooddelivery.memory;

import com.vedantbakliwal.fooddelivery.core.Order;
import com.vedantbakliwal.fooddelivery.core.OrderStore;
import com.vedantbakliwal.fooddelivery.core.OutboxRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Single-instance store. One lock gives the same all-or-nothing writes as a DynamoDB transaction. */
public class InMemoryOrderStore implements OrderStore {

    private final Map<String, Order> orders = new HashMap<>();
    private final Map<String, String> keys = new HashMap<>();
    private final LinkedHashMap<String, OutboxRecord> outbox = new LinkedHashMap<>();

    @Override
    public synchronized boolean create(Order order, String idempotencyKey, OutboxRecord event) {
        if (keys.containsKey(idempotencyKey)) {
            return false;
        }
        keys.put(idempotencyKey, order.orderId());
        orders.put(order.orderId(), order);
        outbox.put(event.event().eventId(), event);
        return true;
    }

    @Override
    public synchronized Optional<String> orderIdForKey(String idempotencyKey) {
        return Optional.ofNullable(keys.get(idempotencyKey));
    }

    @Override
    public synchronized Optional<Order> find(String orderId) {
        return Optional.ofNullable(orders.get(orderId));
    }

    @Override
    public synchronized boolean update(Order updated, long expectedVersion, OutboxRecord event) {
        Order current = orders.get(updated.orderId());
        if (current == null || current.version() != expectedVersion) {
            return false;
        }
        orders.put(updated.orderId(), updated);
        outbox.put(event.event().eventId(), event);
        return true;
    }

    @Override
    public synchronized List<OutboxRecord> pendingOutbox(int limit) {
        return new ArrayList<>(outbox.values()).subList(0, Math.min(limit, outbox.size()));
    }

    @Override
    public synchronized void markPublished(String eventId) {
        outbox.remove(eventId);
    }
}
