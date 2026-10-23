package com.vedantbakliwal.fooddelivery.web;

import com.vedantbakliwal.fooddelivery.core.Menu;
import com.vedantbakliwal.fooddelivery.core.OrderService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;

@RestController
public class OrderController {

    public record PlaceOrder(@NotBlank @Pattern(regexp = "[A-Za-z0-9._-]{1,64}") String customerId,
                             @NotBlank String restaurantId, @NotBlank String zone,
                             @NotEmpty List<OrderService.ItemRequest> items) { }

    public record CourierAction(@NotBlank String courierId) { }

    private final OrderService orders;

    public OrderController(OrderService orders) {
        this.orders = orders;
    }

    @GetMapping("/restaurants")
    public List<Menu.Restaurant> restaurants() {
        return Menu.all();
    }

    /** Places an order. Retry with the same Idempotency-Key and you get the same order, never a second one. */
    @PostMapping("/orders")
    public ResponseEntity<?> place(@RequestHeader("Idempotency-Key") String key,
                                   @Valid @RequestBody PlaceOrder body) {
        if (!key.matches("[A-Za-z0-9._:-]{8,100}")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Idempotency-Key must be 8-100 characters: letters, digits, . _ : -");
        }
        return respond(orders.place(body.customerId(), body.restaurantId(), body.zone(), body.items(), key));
    }

    @GetMapping("/orders/{id}")
    public ResponseEntity<?> get(@PathVariable String id) {
        return orders.find(id).<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "No order " + id)));
    }

    @PostMapping("/orders/{id}/pickup")
    public ResponseEntity<?> pickUp(@PathVariable String id, @Valid @RequestBody CourierAction body) {
        return respond(orders.pickUp(id, body.courierId()));
    }

    @PostMapping("/orders/{id}/deliver")
    public ResponseEntity<?> deliver(@PathVariable String id, @Valid @RequestBody CourierAction body) {
        return respond(orders.deliver(id, body.courierId()));
    }

    private static ResponseEntity<?> respond(OrderService.Result r) {
        HttpStatus status = switch (r.outcome()) {
            case CREATED -> HttpStatus.CREATED;
            case REPLAYED -> HttpStatus.OK;
            case INVALID -> HttpStatus.UNPROCESSABLE_ENTITY;
            case NOT_FOUND -> HttpStatus.NOT_FOUND;
            case CONFLICT -> HttpStatus.CONFLICT;
            case WRONG_COURIER -> HttpStatus.FORBIDDEN;
        };
        return r.order() != null && (status.is2xxSuccessful())
                ? ResponseEntity.status(status).body(r.order())
                : ResponseEntity.status(status).body(Map.of("error", r.message()));
    }
}
