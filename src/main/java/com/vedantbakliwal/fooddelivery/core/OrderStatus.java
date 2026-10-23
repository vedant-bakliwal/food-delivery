package com.vedantbakliwal.fooddelivery.core;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/** Order lifecycle. Only the transitions listed here are allowed. */
public enum OrderStatus {
    PLACED, ACCEPTED, REJECTED, COURIER_ASSIGNED, PICKED_UP, DELIVERED;

    private static final Map<OrderStatus, Set<OrderStatus>> NEXT = Map.of(
            PLACED, EnumSet.of(ACCEPTED, REJECTED),
            ACCEPTED, EnumSet.of(COURIER_ASSIGNED),
            COURIER_ASSIGNED, EnumSet.of(PICKED_UP),
            PICKED_UP, EnumSet.of(DELIVERED),
            REJECTED, EnumSet.noneOf(OrderStatus.class),
            DELIVERED, EnumSet.noneOf(OrderStatus.class));

    public boolean canMoveTo(OrderStatus next) {
        return NEXT.get(this).contains(next);
    }

    public boolean terminal() {
        return NEXT.get(this).isEmpty();
    }
}
