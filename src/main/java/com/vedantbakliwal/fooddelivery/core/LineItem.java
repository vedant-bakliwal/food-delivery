package com.vedantbakliwal.fooddelivery.core;

public record LineItem(String sku, String name, int quantity, long unitPriceCents) {

    public long totalCents() {
        return unitPriceCents * quantity;
    }
}
