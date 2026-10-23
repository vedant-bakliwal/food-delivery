package com.vedantbakliwal.fooddelivery.core;

import java.time.Clock;
import java.util.Map;
import java.util.Optional;

/**
 * Consumes OrderAccepted and assigns the nearest free courier. With no courier free it throws
 * {@link RetryLaterException}: the message comes back after the visibility timeout and, after
 * the queue's max receive count, lands in the dead-letter queue for an operator to redrive.
 * Also releases the courier when an order is delivered.
 */
public class DispatchWorker implements EventHandler {

    private final OrderStore store;
    private final OrderService orders;
    private final CourierPool couriers;
    private final Tracing tracing;
    private final Clock clock;

    public DispatchWorker(OrderStore store, OrderService orders, CourierPool couriers, Tracing tracing, Clock clock) {
        this.store = store;
        this.orders = orders;
        this.couriers = couriers;
        this.tracing = tracing;
        this.clock = clock;
    }

    @Override
    public boolean handles(OrderEvent.Type type) {
        return type == OrderEvent.Type.OrderAccepted || type == OrderEvent.Type.OrderDelivered;
    }

    @Override
    public void handle(OrderEvent event) {
        if (event.type() == OrderEvent.Type.OrderDelivered) {
            String courier = event.data().get("courierId");
            if (courier != null) couriers.release(courier);
            return;
        }
        Optional<Order> found = store.find(event.orderId());
        if (found.isEmpty() || found.get().status() != OrderStatus.ACCEPTED) {
            return; // duplicate: already assigned
        }
        Order order = found.get();
        String courier = couriers.claimNearest(order.zone())
                .orElseThrow(() -> new RetryLaterException("No courier free for " + order.orderId()));
        Order next = order.moveTo(OrderStatus.COURIER_ASSIGNED, courier, null, clock.instant());
        OrderEvent e = orders.event(OrderEvent.Type.CourierAssigned, next, Map.of("courierId", courier));
        if (!store.update(next, order.version(), new OutboxRecord(e, tracing.currentTraceparent()))) {
            couriers.release(courier); // lost a race with a duplicate delivery: give the courier back
        }
    }
}
