package com.vedantbakliwal.fooddelivery.memory;

import com.vedantbakliwal.fooddelivery.core.EventPublisher;
import com.vedantbakliwal.fooddelivery.core.OrderEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Topic with filtered subscriptions, like SNS with a filter policy on the event type. */
public class InMemoryEventBus implements EventPublisher {

    private record Subscription(InMemoryQueue queue, Set<OrderEvent.Type> types) { }

    private final List<Subscription> subscriptions = new ArrayList<>();

    public InMemoryEventBus subscribe(InMemoryQueue queue, Set<OrderEvent.Type> types) {
        subscriptions.add(new Subscription(queue, types));
        return this;
    }

    @Override
    public void publish(OrderEvent event, String traceparent) {
        for (Subscription s : subscriptions) {
            if (s.types().contains(event.type())) s.queue().send(event, traceparent);
        }
    }
}
