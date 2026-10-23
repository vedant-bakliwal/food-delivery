package com.vedantbakliwal.fooddelivery.aws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vedantbakliwal.fooddelivery.core.Order;
import com.vedantbakliwal.fooddelivery.core.OrderEvent;

/** JSON for stored orders and for event messages. Event JSON is the contract other services rely on. */
public class Json {

    private final ObjectMapper mapper;

    public Json(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public String write(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    public Order order(String json) {
        return read(json, Order.class);
    }

    /** Returns null for a message that is not a valid event, so it can go to the dead-letter queue. */
    public OrderEvent eventOrNull(String json) {
        try {
            return mapper.readValue(json, OrderEvent.class);
        } catch (Exception e) {
            return null;
        }
    }

    private <T> T read(String json, Class<T> type) {
        try {
            return mapper.readValue(json, type);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
