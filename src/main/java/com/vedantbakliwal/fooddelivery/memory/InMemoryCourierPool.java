package com.vedantbakliwal.fooddelivery.memory;

import com.vedantbakliwal.fooddelivery.core.CourierPool;
import com.vedantbakliwal.fooddelivery.core.Menu;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public class InMemoryCourierPool implements CourierPool {

    private final Map<String, String> zoneOf = new LinkedHashMap<>();
    private final Map<String, String> orderOf = new LinkedHashMap<>(); // courier -> order, null when free

    /** Creates {@code count} couriers spread across the zones: courier-1 downtown, courier-2 east, ... */
    public InMemoryCourierPool(int count) {
        for (int i = 1; i <= count; i++) {
            String id = "courier-" + i;
            zoneOf.put(id, Menu.ZONES.get((i - 1) % Menu.ZONES.size()));
            orderOf.put(id, null);
        }
    }

    @Override
    public synchronized Optional<String> claimFor(String orderId, String zone) {
        for (Map.Entry<String, String> e : orderOf.entrySet()) {
            if (orderId.equals(e.getValue())) return Optional.of(e.getKey());
        }
        Optional<String> pick = orderOf.entrySet().stream().filter(e -> e.getValue() == null).map(Map.Entry::getKey)
                .min(Comparator.comparingInt(id -> Menu.distance(zoneOf.get(id), zone)));
        pick.ifPresent(id -> orderOf.put(id, orderId));
        return pick;
    }

    @Override
    public synchronized void release(String courierId) {
        if (orderOf.containsKey(courierId)) orderOf.put(courierId, null);
    }

    @Override
    public synchronized long available() {
        return orderOf.values().stream().filter(v -> v == null).count();
    }
}
