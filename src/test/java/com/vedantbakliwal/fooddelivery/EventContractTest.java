package com.vedantbakliwal.fooddelivery;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.vedantbakliwal.fooddelivery.aws.Json;
import com.vedantbakliwal.fooddelivery.core.OrderEvent;
import com.vedantbakliwal.fooddelivery.core.OrderService;
import com.vedantbakliwal.fooddelivery.core.OrderService.ItemRequest;
import com.vedantbakliwal.fooddelivery.core.Tracing;
import com.vedantbakliwal.fooddelivery.memory.InMemoryOrderStore;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.time.Clock;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Event contracts. Golden examples in src/test/resources/contracts are what consumers code
 * against. Producer side: the JSON we publish must have exactly the contract's fields and types.
 * Consumer side: every golden example must still parse. A breaking change fails one of these.
 */
class EventContractTest {

    static final ObjectMapper MAPPER = JsonMapper.builder().findAndAddModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS).build();
    static final Json JSON = new Json(MAPPER);

    static JsonNode golden(OrderEvent.Type type) throws Exception {
        try (InputStream in = EventContractTest.class.getResourceAsStream("/contracts/" + type + ".json")) {
            assertThat(in).as("contract file for " + type).isNotNull();
            return MAPPER.readTree(in);
        }
    }

    /** Field names and JSON types, recursively: the shape, not the values. */
    static String shape(JsonNode node) {
        if (!node.isObject()) {
            return node.getNodeType().name();
        }
        StringBuilder sb = new StringBuilder("{");
        Iterator<String> names = new TreeSet<>(iterable(node.fieldNames())).iterator();
        while (names.hasNext()) {
            String n = names.next();
            sb.append(n).append(':').append(shape(node.get(n))).append(names.hasNext() ? "," : "");
        }
        return sb.append('}').toString();
    }

    static List<String> iterable(Iterator<String> it) {
        List<String> out = new java.util.ArrayList<>();
        it.forEachRemaining(out::add);
        return out;
    }

    @ParameterizedTest
    @EnumSource(OrderEvent.Type.class)
    void everyGoldenExampleParses(OrderEvent.Type type) throws Exception {
        OrderEvent e = JSON.eventOrNull(MAPPER.writeValueAsString(golden(type)));
        assertThat(e).isNotNull();
        assertThat(e.type()).isEqualTo(type);
        assertThat(e.eventId()).isNotBlank();
        assertThat(e.orderVersion()).isPositive();
    }

    @Test
    void publishedOrderPlacedMatchesTheContract() throws Exception {
        InMemoryOrderStore store = new InMemoryOrderStore();
        OrderService orders = new OrderService(store, Tracing.NONE, Clock.systemUTC());
        orders.place("c1", "r-taco-lab", "east", List.of(new ItemRequest("taco-veggie", 2)), "contract-1");
        OrderEvent produced = store.pendingOutbox(1).get(0).event();
        assertThat(shape(MAPPER.readTree(JSON.write(produced)))).isEqualTo(shape(golden(OrderEvent.Type.OrderPlaced)));
    }

    @Test
    void courierAssignedCarriesTheCourierId() throws Exception {
        assertThat(golden(OrderEvent.Type.CourierAssigned).at("/data/courierId").asText()).isNotBlank();
        OrderEvent e = new OrderEvent("evt-1", OrderEvent.Type.CourierAssigned, "ord-1", 3, java.time.Instant.now(),
                Map.of("courierId", "courier-7"));
        assertThat(shape(MAPPER.readTree(JSON.write(e)))).isEqualTo(shape(golden(OrderEvent.Type.CourierAssigned)));
    }

    @Test
    void garbageIsNotAnEvent() {
        assertThat(JSON.eventOrNull("{not json")).isNull();
    }
}
