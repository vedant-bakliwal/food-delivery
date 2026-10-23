package com.vedantbakliwal.fooddelivery.core;

/**
 * Publishes outbox events, oldest first, and removes each one only after the publish succeeded.
 * A crash between the two steps publishes the event again later: at-least-once delivery, which
 * is why consumers are idempotent.
 */
public class OutboxRelay {

    private final OrderStore store;
    private final EventPublisher publisher;

    public OutboxRelay(OrderStore store, EventPublisher publisher) {
        this.store = store;
        this.publisher = publisher;
    }

    public int relayOnce(int limit) {
        int sent = 0;
        for (OutboxRecord r : store.pendingOutbox(limit)) {
            publisher.publish(r.event(), r.traceparent());
            store.markPublished(r.event().eventId());
            sent++;
        }
        return sent;
    }
}
