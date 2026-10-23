package com.vedantbakliwal.fooddelivery.memory;

import com.vedantbakliwal.fooddelivery.core.MessageQueue;
import com.vedantbakliwal.fooddelivery.core.OrderEvent;
import com.vedantbakliwal.fooddelivery.core.QueueMessage;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

/**
 * A queue with the SQS behaviour that matters here: visibility timeout, receive counting and
 * a dead-letter queue after {@code maxReceiveCount} deliveries.
 */
public class InMemoryQueue implements MessageQueue {

    private static final class Entry {
        final String id = UUID.randomUUID().toString();
        final OrderEvent event;
        final String traceparent;
        int receives;
        Instant invisibleUntil = Instant.MIN;
        String receipt;

        Entry(OrderEvent event, String traceparent) {
            this.event = event;
            this.traceparent = traceparent;
        }
    }

    private final String name;
    private final Duration visibility;
    private final int maxReceiveCount;
    private final Clock clock;
    private final LinkedList<Entry> messages = new LinkedList<>();
    private final List<Entry> deadLetters = new ArrayList<>();

    public InMemoryQueue(String name, Duration visibility, int maxReceiveCount, Clock clock) {
        this.name = name;
        this.visibility = visibility;
        this.maxReceiveCount = maxReceiveCount;
        this.clock = clock;
    }

    public synchronized void send(OrderEvent event, String traceparent) {
        messages.add(new Entry(event, traceparent));
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public synchronized List<QueueMessage> receive(int max) {
        Instant now = clock.instant();
        List<QueueMessage> out = new ArrayList<>();
        Iterator<Entry> it = messages.iterator();
        while (it.hasNext() && out.size() < max) {
            Entry e = it.next();
            if (now.isBefore(e.invisibleUntil)) continue;
            if (e.receives >= maxReceiveCount) { // already delivered the maximum number of times
                it.remove();
                deadLetters.add(e);
                continue;
            }
            e.receives++;
            e.receipt = UUID.randomUUID().toString();
            e.invisibleUntil = now.plus(visibility);
            out.add(new QueueMessage(e.id, e.receipt, e.event, e.traceparent, e.receives));
        }
        return out;
    }

    @Override
    public synchronized void delete(QueueMessage message) {
        messages.removeIf(e -> e.id.equals(message.messageId()) && message.receipt().equals(e.receipt));
    }

    @Override
    public synchronized long depth() {
        return messages.size();
    }

    @Override
    public synchronized long deadLetterDepth() {
        return deadLetters.size();
    }

    @Override
    public synchronized int redriveDeadLetters() {
        int n = deadLetters.size();
        for (Entry e : deadLetters) {
            Entry fresh = new Entry(e.event, e.traceparent);
            messages.add(fresh);
        }
        deadLetters.clear();
        return n;
    }
}
