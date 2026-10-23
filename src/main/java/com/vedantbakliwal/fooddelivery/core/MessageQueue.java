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

    /** Hides the message for {@code delay} so it is retried sooner (or later) than the default visibility timeout. */
    void retryAfter(QueueMessage message, java.time.Duration delay);

    long depth();

    long deadLetterDepth();

    /** Moves every dead-lettered message back onto the main queue. Returns how many moved. */
    int redriveDeadLetters();
}
