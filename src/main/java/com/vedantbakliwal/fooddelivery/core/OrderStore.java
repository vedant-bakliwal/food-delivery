package com.vedantbakliwal.fooddelivery.core;

import java.util.List;
import java.util.Optional;

/**
 * Orders, idempotency keys and the outbox, written atomically. Implementations: in-memory and
 * DynamoDB (TransactWriteItems).
 */
public interface OrderStore {

    /** Saves a new order, claims the idempotency key and adds the event to the outbox, all or nothing. False if the key is taken. */
    boolean create(Order order, String idempotencyKey, OutboxRecord event);

    Optional<String> orderIdForKey(String idempotencyKey);

    Optional<Order> find(String orderId);

    /**
     * Replaces the order only if it still has {@code expectedVersion}, and adds the event to the
     * outbox in the same transaction. False if someone else changed the order first.
     */
    boolean update(Order updated, long expectedVersion, OutboxRecord event);

    List<OutboxRecord> pendingOutbox(int limit);

    void markPublished(String eventId);
}
