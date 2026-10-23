package com.vedantbakliwal.fooddelivery.core;

import java.util.Optional;

/**
 * Couriers that can take a delivery. Claims are atomic (a courier never gets two orders) and
 * keyed by order: claiming again for the same order returns the same courier, so a retried or
 * duplicated message can never tie up a second courier.
 */
public interface CourierPool {

    /** The courier already holding this order, or the nearest free one claimed for it. */
    Optional<String> claimFor(String orderId, String zone);

    void release(String courierId);

    long available();
}
