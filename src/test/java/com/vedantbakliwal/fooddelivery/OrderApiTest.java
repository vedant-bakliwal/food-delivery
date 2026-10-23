package com.vedantbakliwal.fooddelivery;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Full app on a random port with the in-memory adapters and the background workers running. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"fooddelivery.poll-ms=50", "fooddelivery.couriers=4"})
class OrderApiTest {

    static final ParameterizedTypeReference<Map<String, Object>> MAP = new ParameterizedTypeReference<>() { };

    @Autowired
    TestRestTemplate http;

    ResponseEntity<Map<String, Object>> place(String key, String restaurant, String sku) {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set("Idempotency-Key", key);
        Map<String, Object> body = Map.of("customerId", "c-42", "restaurantId", restaurant, "zone", "east",
                "items", List.of(Map.of("sku", sku, "quantity", 2)));
        return http.exchange("/orders", HttpMethod.POST, new HttpEntity<>(body, h), MAP);
    }

    Map<String, Object> waitForStatus(String id, String status) throws InterruptedException {
        Map<String, Object> order = Map.of();
        for (int i = 0; i < 100; i++) {
            order = http.exchange("/orders/" + id, HttpMethod.GET, null, MAP).getBody();
            if (status.equals(order.get("status"))) return order;
            Thread.sleep(50);
        }
        throw new AssertionError("Order " + id + " never reached " + status + ", last seen " + order);
    }

    ResponseEntity<Map<String, Object>> courier(String id, String action, String courierId) {
        return http.exchange("/orders/" + id + "/" + action, HttpMethod.POST,
                new HttpEntity<>(Map.of("courierId", courierId)), MAP);
    }

    @Test
    void placeRetryAndDeliver() throws Exception {
        ResponseEntity<Map<String, Object>> first = place("api-key-0001", "r-taco-lab", "taco-veggie");
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(((Number) first.getBody().get("totalCents")).longValue()).isEqualTo(800);
        String id = (String) first.getBody().get("orderId");

        ResponseEntity<Map<String, Object>> retry = place("api-key-0001", "r-taco-lab", "taco-veggie");
        assertThat(retry.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(retry.getBody().get("orderId")).isEqualTo(id);

        String courierId = (String) waitForStatus(id, "COURIER_ASSIGNED").get("courierId");
        assertThat(courier(id, "deliver", courierId).getStatusCode()).isEqualTo(HttpStatus.CONFLICT); // not picked up yet
        assertThat(courier(id, "pickup", "courier-999").getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(courier(id, "pickup", courierId).getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(courier(id, "deliver", courierId).getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(waitForStatus(id, "DELIVERED")).containsEntry("courierId", courierId);
    }

    @Test
    void soldOutItemIsRejectedByTheRestaurant() throws Exception {
        String id = (String) place("api-key-0002", "r-taco-lab", "elote").getBody().get("orderId");
        assertThat(waitForStatus(id, "REJECTED")).containsEntry("rejectionReason", "Elote is sold out");
    }

    @Test
    void badRequestsGetClearErrors() {
        assertThat(place("short", "r-taco-lab", "taco-veggie").getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(place("api-key-0003", "r-taco-lab", "caviar").getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(place("api-key-0004", "r-unknown", "x").getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(http.exchange("/orders/ord-missing", HttpMethod.GET, null, MAP).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void opsEndpointReportsQueues() {
        Map<String, Object> ops = http.exchange("/ops/queues", HttpMethod.GET, null, MAP).getBody();
        assertThat(ops).containsKeys("restaurant", "dispatch", "couriersAvailable");
        assertThat(http.getForEntity("/actuator/health", String.class).getStatusCode()).isEqualTo(HttpStatus.OK);
    }
}
