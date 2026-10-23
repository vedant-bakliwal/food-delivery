package com.vedantbakliwal.fooddelivery.memory;

import com.vedantbakliwal.fooddelivery.core.CourierPool;
import com.vedantbakliwal.fooddelivery.core.Menu;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public class InMemoryCourierPool implements CourierPool {

    private final Map<String, String> zoneOf = new LinkedHashMap<>();
    private final Map<String, Boolean> busy = new LinkedHashMap<>();

    /** Creates {@code count} couriers spread across the zones: courier-1 downtown, courier-2 east, ... */
    public InMemoryCourierPool(int count) {
        for (int i = 1; i <= count; i++) {
            String id = "courier-" + i;
            zoneOf.put(id, Menu.ZONES.get((i - 1) % Menu.ZONES.size()));
            busy.put(id, false);
        }
    }

    @Override
    public synchronized Optional<String> claimNearest(String zone) {
        Optional<String> pick = busy.entrySet().stream().filter(e -> !e.getValue()).map(Map.Entry::getKey)
                .min(Comparator.comparingInt(id -> Menu.distance(zoneOf.get(id), zone)));
        pick.ifPresent(id -> busy.put(id, true));
        return pick;
    }

    @Override
    public synchronized void release(String courierId) {
        busy.computeIfPresent(courierId, (k, v) -> false);
    }

    @Override
    public synchronized long available() {
        return busy.values().stream().filter(b -> !b).count();
    }
}
