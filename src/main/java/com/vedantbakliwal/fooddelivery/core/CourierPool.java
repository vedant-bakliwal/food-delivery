package com.vedantbakliwal.fooddelivery.core;

import java.util.Optional;

/** Couriers that can take a delivery. Claiming is atomic: a courier is never given two orders. */
public interface CourierPool {

    Optional<String> claimNearest(String zone);

    void release(String courierId);

    long available();
}
