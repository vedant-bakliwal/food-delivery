package com.vedantbakliwal.fooddelivery.web;

import com.vedantbakliwal.fooddelivery.core.CourierPool;
import com.vedantbakliwal.fooddelivery.core.MessageQueue;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/** Operator view: queue and dead-letter depths, free couriers, and DLQ redrive. */
@RestController
@RequestMapping("/ops")
public class OpsController {

    private final Map<String, MessageQueue> queues;
    private final CourierPool couriers;

    public OpsController(@Qualifier("restaurantQueue") MessageQueue restaurant, @Qualifier("dispatchQueue") MessageQueue dispatch,
                         CourierPool couriers) {
        this.queues = Map.of("restaurant", restaurant, "dispatch", dispatch);
        this.couriers = couriers;
    }

    @GetMapping("/queues")
    public Map<String, Object> queues() {
        Map<String, Object> out = new java.util.TreeMap<>();
        queues.forEach((name, q) -> out.put(name, Map.of("depth", q.depth(), "deadLetters", q.deadLetterDepth())));
        out.put("couriersAvailable", couriers.available());
        return out;
    }

    @PostMapping("/queues/{name}/redrive")
    public ResponseEntity<?> redrive(@PathVariable String name) {
        MessageQueue q = queues.get(name);
        if (q == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(Map.of("queue", name, "moved", q.redriveDeadLetters()));
    }
}
