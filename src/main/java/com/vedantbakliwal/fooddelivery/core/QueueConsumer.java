package com.vedantbakliwal.fooddelivery.core;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Polls one queue and hands messages to a handler, continuing the producer's trace. A message
 * is deleted only after the handler succeeds; on any failure it is left alone, so the queue
 * redelivers it and finally dead-letters it.
 */
public class QueueConsumer {

    private static final Logger LOG = Logger.getLogger(QueueConsumer.class.getName());

    public record Stats(int handled, int failed, int skipped) { }

    private final MessageQueue queue;
    private final EventHandler handler;
    private final Tracing tracing;

    public QueueConsumer(MessageQueue queue, EventHandler handler, Tracing tracing) {
        this.queue = queue;
        this.handler = handler;
        this.tracing = tracing;
    }

    public Stats pollOnce(int max) {
        int handled = 0, failed = 0, skipped = 0;
        for (QueueMessage m : queue.receive(max)) {
            if (m.event() == null || !handler.handles(m.event().type())) {
                if (m.event() == null) { failed++; continue; } // unreadable: leave it for the DLQ
                queue.delete(m);
                skipped++;
                continue;
            }
            try {
                tracing.inSpan(queue.name() + " " + m.event().type(), m.traceparent(), () -> {
                    handler.handle(m.event());
                    return null;
                });
                queue.delete(m);
                handled++;
            } catch (RetryLaterException e) {
                failed++;
                queue.retryAfter(m, e.delay());
                LOG.fine(() -> queue.name() + ": " + m.event().orderId() + " retry " + m.receiveCount() + ": " + e.getMessage());
                continue;
            } catch (RuntimeException e) {
                failed++;
                LOG.log(Level.WARNING,
                        () -> queue.name() + ": " + m.event().type() + " " + m.event().orderId()
                                + " failed on delivery " + m.receiveCount() + ": " + e.getMessage());
            }
        }
        return new Stats(handled, failed, skipped);
    }
}
