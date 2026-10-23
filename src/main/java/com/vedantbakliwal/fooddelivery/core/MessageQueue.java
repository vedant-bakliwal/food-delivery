package com.vedantbakliwal.fooddelivery.core;

import java.util.List;

/**
 * A work queue with SQS semantics: a received message stays invisible for a while and comes
 * back unless deleted; after too many deliveries it moves to a dead-letter queue.
 */
public interface MessageQueue {

    String name();

    List<QueueMessage> receive(int max);

    void delete(QueueMessage message);

    long depth();

    long deadLetterDepth();

    /** Moves every dead-lettered message back onto the main queue. Returns how many moved. */
    int redriveDeadLetters();
}
