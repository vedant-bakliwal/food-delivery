package com.vedantbakliwal.fooddelivery;

import com.vedantbakliwal.fooddelivery.core.DispatchWorker;
import com.vedantbakliwal.fooddelivery.core.Order;
import com.vedantbakliwal.fooddelivery.core.OrderEvent;
import com.vedantbakliwal.fooddelivery.core.OrderService;
import com.vedantbakliwal.fooddelivery.core.OrderService.ItemRequest;
import com.vedantbakliwal.fooddelivery.core.OrderStatus;
import com.vedantbakliwal.fooddelivery.core.OutboxRecord;
import com.vedantbakliwal.fooddelivery.core.OutboxRelay;
import com.vedantbakliwal.fooddelivery.core.QueueConsumer;
import com.vedantbakliwal.fooddelivery.core.RestaurantWorker;
import com.vedantbakliwal.fooddelivery.core.Tracing;
import com.vedantbakliwal.fooddelivery.memory.InMemoryCourierPool;
import com.vedantbakliwal.fooddelivery.memory.InMemoryEventBus;
import com.vedantbakliwal.fooddelivery.memory.InMemoryOrderStore;
import com.vedantbakliwal.fooddelivery.memory.InMemoryQueue;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/** The whole order-to-dispatch flow with SQS-like in-memory queues and a controllable clock. */
class OrderFlowTest {

    final Instant[] now = {Instant.parse("2026-11-01T18:00:00Z")};
    final Clock clock = new Clock() {
        public ZoneId getZone() { return ZoneOffset.UTC; }
        public Clock withZone(ZoneId z) { return this; }
        public Instant instant() { return now[0]; }
    };

    final InMemoryOrderStore store = new InMemoryOrderStore();
    final InMemoryQueue restaurantQ = new InMemoryQueue("restaurant", Duration.ofSeconds(5), 3, clock);
    final InMemoryQueue dispatchQ = new InMemoryQueue("dispatch", Duration.ofSeconds(5), 3, clock);
    final InMemoryEventBus bus = new InMemoryEventBus()
            .subscribe(restaurantQ, Set.of(OrderEvent.Type.OrderPlaced))
            .subscribe(dispatchQ, Set.of(OrderEvent.Type.OrderAccepted, OrderEvent.Type.OrderDelivered));
    final InMemoryCourierPool couriers = new InMemoryCourierPool(1);
    final OrderService orders = new OrderService(store, Tracing.NONE, clock);
    final OutboxRelay relay = new OutboxRelay(store, bus);
    final QueueConsumer restaurant = new QueueConsumer(restaurantQ, new RestaurantWorker(store, orders, Tracing.NONE, clock), Tracing.NONE);
    final QueueConsumer dispatch = new QueueConsumer(dispatchQ, new DispatchWorker(store, orders, couriers, Tracing.NONE, clock), Tracing.NONE);

    void pump() {
        for (int i = 0; i < 5; i++) {
            relay.relayOnce(100);
            restaurant.pollOnce(10);
            dispatch.pollOnce(10);
        }
    }

    Order place(String restaurantId, String sku, int qty, String key) {
        return orders.place("c-" + key, restaurantId, "east", List.of(new ItemRequest(sku, qty)), key).order();
    }

    @Test
    void orderGoesFromPlacedToDeliveredAndFreesTheCourier() {
        OrderService.Result r = orders.place("c1", "r-brisket-co", "east",
                List.of(new ItemRequest("brisket-plate", 2), new ItemRequest("mac-cheese", 1)), "key-0001");
        assertThat(r.order().totalCents()).isEqualTo(2 * 1895 + 595); // prices from the menu, not the client
        pump();
        Order assigned = orders.find(r.order().orderId()).orElseThrow();
        assertThat(assigned.status()).isEqualTo(OrderStatus.COURIER_ASSIGNED);
        assertThat(couriers.available()).isZero();

        assertThat(orders.pickUp(assigned.orderId(), "courier-1").outcome()).isEqualTo(OrderService.Outcome.CREATED);
        assertThat(orders.deliver(assigned.orderId(), "courier-2").outcome()).isEqualTo(OrderService.Outcome.WRONG_COURIER);
        assertThat(orders.deliver(assigned.orderId(), "courier-1").outcome()).isEqualTo(OrderService.Outcome.CREATED);
        assertThat(orders.deliver(assigned.orderId(), "courier-1").outcome()).isEqualTo(OrderService.Outcome.REPLAYED);
        pump();
        assertThat(couriers.available()).isEqualTo(1);
    }

    @Test
    void noFreeCourierRetriesThenDeadLettersThenRedrives() {
        Order first = place("r-brisket-co", "mac-cheese", 1, "key-0002");
        Order second = place("r-taco-lab", "taco-veggie", 2, "key-0003");
        pump(); // first takes the only courier; second's OrderAccepted fails delivery 1
        for (int i = 0; i < 3; i++) {
            now[0] = now[0].plusSeconds(6); // visibility timeout passes, SQS-style redelivery
            dispatch.pollOnce(10);
        }
        assertThat(dispatchQ.deadLetterDepth()).isEqualTo(1);
        assertThat(orders.find(second.orderId()).orElseThrow().status()).isEqualTo(OrderStatus.ACCEPTED);

        orders.pickUp(first.orderId(), "courier-1");
        orders.deliver(first.orderId(), "courier-1");
        pump();
        assertThat(dispatchQ.redriveDeadLetters()).isEqualTo(1);
        pump();
        assertThat(orders.find(second.orderId()).orElseThrow().status()).isEqualTo(OrderStatus.COURIER_ASSIGNED);
    }

    @Test
    void restaurantRejectsWhenClosedOrSoldOut() {
        Order closed = orders.place("c", "r-night-owl", "north", List.of(new ItemRequest("pho", 1)), "key-0004").order();
        Order soldOut = place("r-taco-lab", "elote", 1, "key-0005");
        pump();
        assertThat(orders.find(closed.orderId()).orElseThrow().rejectionReason()).isEqualTo("restaurant is closed");
        assertThat(orders.find(soldOut.orderId()).orElseThrow().rejectionReason()).isEqualTo("Elote is sold out");
        assertThat(couriers.available()).isEqualTo(1);
    }

    @Test
    void duplicateDeliveryOfAnEventIsHarmless() {
        Order o = place("r-brisket-co", "mac-cheese", 1, "key-0006");
        OutboxRecord rec = store.pendingOutbox(1).get(0);
        bus.publish(rec.event(), null); // the relay crashed after publishing, then published again
        bus.publish(rec.event(), null);
        restaurant.pollOnce(10);
        assertThat(store.find(o.orderId()).orElseThrow().version()).isEqualTo(2);
        assertThat(restaurantQ.depth()).isZero();
    }

    @Test
    void concurrentRequestsWithOneKeyCreateOneOrderAndOneEvent() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(16);
        List<Future<OrderService.Result>> results = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            results.add(pool.submit(() -> orders.place("c", "r-brisket-co", "east", List.of(new ItemRequest("mac-cheese", 1)), "same-key-1")));
        }
        Set<String> ids = new HashSet<>();
        int created = 0;
        for (Future<OrderService.Result> f : results) {
            OrderService.Result r = f.get();
            ids.add(r.order().orderId());
            if (r.outcome() == OrderService.Outcome.CREATED) created++;
        }
        pool.shutdown();
        assertThat(created).isEqualTo(1);
        assertThat(ids).hasSize(1);
        assertThat(store.pendingOutbox(100)).hasSize(1);
    }

    @Test
    void rejectsInvalidOrders() {
        assertThat(orders.place("c", "r-taco-lab", "mars", List.of(new ItemRequest("taco-veggie", 1)), "key-0007").outcome())
                .isEqualTo(OrderService.Outcome.INVALID);
        assertThat(orders.place("c", "r-taco-lab", "east", List.of(new ItemRequest("caviar", 1)), "key-0008").outcome())
                .isEqualTo(OrderService.Outcome.INVALID);
        assertThat(orders.place("c", "r-nope", "east", List.of(new ItemRequest("x", 1)), "key-0009").outcome())
                .isEqualTo(OrderService.Outcome.NOT_FOUND);
    }
}
