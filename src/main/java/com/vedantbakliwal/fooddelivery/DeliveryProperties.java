package com.vedantbakliwal.fooddelivery;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Set;

/**
 * Settings under {@code fooddelivery.*}. {@code roles} decides what this process runs, so one
 * image can be deployed as the API, the outbox relay, the restaurant worker or the dispatch worker.
 */
@ConfigurationProperties("fooddelivery")
public record DeliveryProperties(String store, Set<String> roles, int couriers, int maxReceiveCount,
                                 int dispatchMaxReceiveCount,
                                 int visibilitySeconds, Aws aws) {

    public record Aws(String endpoint, String region, String prefix) { }

    public boolean has(String role) {
        return roles.contains(role);
    }
}
