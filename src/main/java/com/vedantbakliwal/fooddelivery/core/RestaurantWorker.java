package com.vedantbakliwal.fooddelivery.core;

import java.time.Clock;
import java.util.Map;
import java.util.Optional;

/** Consumes OrderPlaced: the restaurant accepts, or rejects when closed or an item is sold out. */
public class RestaurantWorker implements EventHandler {

    private final OrderStore store;
    private final OrderService orders;
    private final Tracing tracing;
    private final Clock clock;

    public RestaurantWorker(OrderStore store, OrderService orders, Tracing tracing, Clock clock) {
        this.store = store;
        this.orders = orders;
        this.tracing = tracing;
        this.clock = clock;
    }

    @Override
    public boolean handles(OrderEvent.Type type) {
        return type == OrderEvent.Type.OrderPlaced;
    }

    @Override
    public void handle(OrderEvent event) {
        Optional<Order> found = store.find(event.orderId());
        if (found.isEmpty() || found.get().status() != OrderStatus.PLACED) {
            return; // duplicate delivery or already decided: nothing to do
        }
        Order order = found.get();
        Menu.Restaurant r = Menu.restaurant(order.restaurantId()).orElseThrow();
        String reason = !r.open() ? "restaurant is closed"
                : order.items().stream().filter(i -> r.soldOut().contains(i.sku())).map(i -> i.name() + " is sold out")
                .findFirst().orElse(null);
        Order next = reason == null
                ? order.moveTo(OrderStatus.ACCEPTED, null, null, clock.instant())
                : order.moveTo(OrderStatus.REJECTED, null, reason, clock.instant());
        OrderEvent e = reason == null
                ? orders.event(OrderEvent.Type.OrderAccepted, next, Map.of("restaurantId", r.id(), "zone", order.zone()))
                : orders.event(OrderEvent.Type.OrderRejected, next, Map.of("reason", reason));
        store.update(next, order.version(), new OutboxRecord(e, tracing.currentTraceparent())); // false = someone else won; fine
    }
}
