package com.vedantbakliwal.fooddelivery.core;

public interface EventHandler {

    boolean handles(OrderEvent.Type type);

    void handle(OrderEvent event);
}
