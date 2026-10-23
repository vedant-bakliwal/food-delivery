package com.vedantbakliwal.fooddelivery.aws;

import com.vedantbakliwal.fooddelivery.core.Order;
import com.vedantbakliwal.fooddelivery.core.OrderStore;
import com.vedantbakliwal.fooddelivery.core.OutboxRecord;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * DynamoDB store. Creating or changing an order and adding its outbox event happen in one
 * TransactWriteItems call, so an order change can never be saved without its event (or the
 * other way round). Order updates are guarded by a version condition (optimistic locking).
 */
public class DynamoOrderStore implements OrderStore {

    private final DynamoDbClient dynamo;
    private final AwsResources r;
    private final Json json;

    public DynamoOrderStore(DynamoDbClient dynamo, AwsResources resources, Json json) {
        this.dynamo = dynamo;
        this.r = resources;
        this.json = json;
    }

    private static AttributeValue s(String v) {
        return AttributeValue.fromS(v);
    }

    private Map<String, AttributeValue> orderItem(Order o) {
        return Map.of("pk", s(o.orderId()), "status", s(o.status().name()),
                "version", AttributeValue.fromN(String.valueOf(o.version())), "doc", s(json.write(o)));
    }

    private TransactWriteItem outboxPut(OutboxRecord e) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("pk", s(e.event().eventId()));
        item.put("event", s(json.write(e.event())));
        item.put("createdAt", s(e.event().occurredAt().toString()));
        if (e.traceparent() != null) item.put("traceparent", s(e.traceparent()));
        return TransactWriteItem.builder().put(Put.builder().tableName(r.outboxTable()).item(item).build()).build();
    }

    @Override
    public boolean create(Order order, String idempotencyKey, OutboxRecord event) {
        try {
            dynamo.transactWriteItems(b -> b.transactItems(
                    TransactWriteItem.builder().put(Put.builder().tableName(r.keysTable())
                            .item(Map.of("pk", s(idempotencyKey), "orderId", s(order.orderId())))
                            .conditionExpression("attribute_not_exists(pk)").build()).build(),
                    TransactWriteItem.builder().put(Put.builder().tableName(r.ordersTable()).item(orderItem(order))
                            .conditionExpression("attribute_not_exists(pk)").build()).build(),
                    outboxPut(event)));
            return true;
        } catch (TransactionCanceledException e) {
            return false; // the idempotency key is already taken
        }
    }

    @Override
    public Optional<String> orderIdForKey(String idempotencyKey) {
        var item = dynamo.getItem(b -> b.tableName(r.keysTable()).key(Map.of("pk", s(idempotencyKey))).consistentRead(true));
        return item.hasItem() ? Optional.of(item.item().get("orderId").s()) : Optional.empty();
    }

    @Override
    public Optional<Order> find(String orderId) {
        var item = dynamo.getItem(b -> b.tableName(r.ordersTable()).key(Map.of("pk", s(orderId))).consistentRead(true));
        return item.hasItem() ? Optional.of(json.order(item.item().get("doc").s())) : Optional.empty();
    }

    @Override
    public boolean update(Order updated, long expectedVersion, OutboxRecord event) {
        try {
            dynamo.transactWriteItems(b -> b.transactItems(
                    TransactWriteItem.builder().put(Put.builder().tableName(r.ordersTable()).item(orderItem(updated))
                            .conditionExpression("#v = :expected")
                            .expressionAttributeNames(Map.of("#v", "version"))
                            .expressionAttributeValues(Map.of(":expected", AttributeValue.fromN(String.valueOf(expectedVersion))))
                            .build()).build(),
                    outboxPut(event)));
            return true;
        } catch (TransactionCanceledException e) {
            return false; // someone else changed the order first
        }
    }

    /**
     * Small-scale polling of the outbox table. At production scale DynamoDB Streams on the outbox
     * table would trigger the publisher instead of a scan.
     */
    @Override
    public List<OutboxRecord> pendingOutbox(int limit) {
        return dynamo.scan(b -> b.tableName(r.outboxTable()).limit(Math.max(limit, 1) * 4).consistentRead(true)).items().stream()
                .sorted(Comparator.comparing(i -> i.get("createdAt").s()))
                .limit(limit)
                .map(i -> new OutboxRecord(json.eventOrNull(i.get("event").s()),
                        i.containsKey("traceparent") ? i.get("traceparent").s() : null))
                .filter(o -> o.event() != null)
                .toList();
    }

    @Override
    public void markPublished(String eventId) {
        dynamo.deleteItem(b -> b.tableName(r.outboxTable()).key(Map.of("pk", s(eventId))));
    }
}
