package com.vedantbakliwal.fooddelivery.core;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/** Places orders and records courier progress. Every change goes to the outbox in the same write. */
public class OrderService {

    public record ItemRequest(String sku, int quantity) { }

    public enum Outcome { CREATED, REPLAYED, INVALID, NOT_FOUND, CONFLICT, WRONG_COURIER }

    public record Result(Outcome outcome, Order order, String message) { }

    private final OrderStore store;
    private final Tracing tracing;
    private final Clock clock;

    public OrderService(OrderStore store, Tracing tracing, Clock clock) {
        this.store = store;
        this.tracing = tracing;
        this.clock = clock;
    }

    public Result place(String customerId, String restaurantId, String zone, List<ItemRequest> items, String idempotencyKey) {
        Optional<String> existing = store.orderIdForKey(idempotencyKey);
        if (existing.isPresent()) {
            return new Result(Outcome.REPLAYED, store.find(existing.get()).orElse(null), "Same key: original order returned");
        }
        Optional<Menu.Restaurant> restaurant = Menu.restaurant(restaurantId);
        if (restaurant.isEmpty()) {
            return new Result(Outcome.NOT_FOUND, null, "Unknown restaurant " + restaurantId);
        }
        if (!Menu.ZONES.contains(zone)) {
            return new Result(Outcome.INVALID, null, "Delivery zone must be one of " + Menu.ZONES);
        }
        if (items == null || items.isEmpty() || items.size() > 20) {
            return new Result(Outcome.INVALID, null, "An order needs 1 to 20 lines");
        }
        List<LineItem> lines = new ArrayList<>();
        for (ItemRequest r : items) {
            Menu.Item item = restaurant.get().items().get(r.sku());
            if (item == null) {
                return new Result(Outcome.INVALID, null, "Not on the menu: " + r.sku());
            }
            if (r.quantity() < 1 || r.quantity() > 20) {
                return new Result(Outcome.INVALID, null, "Quantity must be 1 to 20");
            }
            lines.add(new LineItem(item.sku(), item.name(), r.quantity(), item.priceCents()));
        }
        Instant now = clock.instant();
        long total = lines.stream().mapToLong(LineItem::totalCents).sum();
        Order order = new Order("ord-" + UUID.randomUUID(), customerId, restaurantId, zone, List.copyOf(lines), total,
                OrderStatus.PLACED, null, null, 1, now, now);
        OrderEvent placed = event(OrderEvent.Type.OrderPlaced, order, Map.of("restaurantId", restaurantId, "zone", zone));
        if (!store.create(order, idempotencyKey, new OutboxRecord(placed, tracing.currentTraceparent()))) {
            String id = store.orderIdForKey(idempotencyKey).orElseThrow();
            return new Result(Outcome.REPLAYED, store.find(id).orElse(null), "Same key: original order returned");
        }
        return new Result(Outcome.CREATED, order, "Order placed");
    }

    public Optional<Order> find(String orderId) {
        return store.find(orderId);
    }

    public Result pickUp(String orderId, String courierId) {
        return courierStep(orderId, courierId, OrderStatus.PICKED_UP, OrderEvent.Type.OrderPickedUp);
    }

    public Result deliver(String orderId, String courierId) {
        return courierStep(orderId, courierId, OrderStatus.DELIVERED, OrderEvent.Type.OrderDelivered);
    }

    private Result courierStep(String orderId, String courierId, OrderStatus next, OrderEvent.Type type) {
        Optional<Order> found = store.find(orderId);
        if (found.isEmpty()) {
            return new Result(Outcome.NOT_FOUND, null, "No order " + orderId);
        }
        Order order = found.get();
        if (!courierId.equals(order.courierId())) {
            return new Result(Outcome.WRONG_COURIER, order, "This order is assigned to another courier");
        }
        if (order.status() == next) {
            return new Result(Outcome.REPLAYED, order, "Already " + next);
        }
        if (!order.status().canMoveTo(next)) {
            return new Result(Outcome.CONFLICT, order, "Order is " + order.status());
        }
        Order updated = order.moveTo(next, null, null, clock.instant());
        OrderEvent e = event(type, updated, Map.of("courierId", courierId));
        if (!store.update(updated, order.version(), new OutboxRecord(e, tracing.currentTraceparent()))) {
            return new Result(Outcome.CONFLICT, store.find(orderId).orElse(order), "Order changed concurrently; retry");
        }
        return new Result(Outcome.CREATED, updated, "Order " + next);
    }

    public OrderEvent event(OrderEvent.Type type, Order order, Map<String, String> data) {
        return new OrderEvent("evt-" + UUID.randomUUID(), type, order.orderId(), order.version(), clock.instant(), data);
    }
}
