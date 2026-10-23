package com.vedantbakliwal.fooddelivery.aws;

import com.vedantbakliwal.fooddelivery.core.CourierPool;
import com.vedantbakliwal.fooddelivery.core.Menu;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;

/**
 * Couriers in DynamoDB. A claim is a conditional update AVAILABLE -> BUSY, so two dispatch
 * instances can never give the same courier two orders: the loser just tries the next courier.
 */
public class DynamoCourierPool implements CourierPool {

    private final DynamoDbClient dynamo;
    private final String table;

    public DynamoCourierPool(DynamoDbClient dynamo, String table) {
        this.dynamo = dynamo;
        this.table = table;
    }

    @Override
    public Optional<String> claimFor(String orderId, String zone) {
        var mine = dynamo.scan(b -> b.tableName(table).consistentRead(true)
                .filterExpression("orderId = :o").expressionAttributeValues(Map.of(":o", AttributeValue.fromS(orderId)))).items();
        if (!mine.isEmpty()) {
            return Optional.of(mine.get(0).get("pk").s()); // a retry: this order already has its courier
        }
        var free = dynamo.scan(b -> b.tableName(table).consistentRead(true)
                        .filterExpression("#s = :a").expressionAttributeNames(Map.of("#s", "status"))
                        .expressionAttributeValues(Map.of(":a", AttributeValue.fromS("AVAILABLE")))).items().stream()
                .sorted(Comparator.comparingInt(i -> Menu.distance(i.get("zone").s(), zone)))
                .map(i -> i.get("pk").s())
                .toList();
        for (String id : free) {
            if (claim(id, orderId)) return Optional.of(id);
        }
        return Optional.empty();
    }

    @Override
    public void release(String courierId) {
        try {
            dynamo.updateItem(b -> b.tableName(table).key(Map.of("pk", AttributeValue.fromS(courierId)))
                    .updateExpression("SET #s = :a REMOVE orderId").conditionExpression("#s = :b")
                    .expressionAttributeNames(Map.of("#s", "status"))
                    .expressionAttributeValues(Map.of(":a", AttributeValue.fromS("AVAILABLE"), ":b", AttributeValue.fromS("BUSY"))));
        } catch (ConditionalCheckFailedException e) {
            // already free: releasing twice (a duplicate OrderDelivered) is harmless
        }
    }

    /** AVAILABLE -> BUSY for this order, only if nobody claimed the courier first. */
    private boolean claim(String id, String orderId) {
        try {
            dynamo.updateItem(b -> b.tableName(table).key(Map.of("pk", AttributeValue.fromS(id)))
                    .updateExpression("SET #s = :b, orderId = :o").conditionExpression("#s = :a")
                    .expressionAttributeNames(Map.of("#s", "status"))
                    .expressionAttributeValues(Map.of(":a", AttributeValue.fromS("AVAILABLE"),
                            ":b", AttributeValue.fromS("BUSY"), ":o", AttributeValue.fromS(orderId))));
            return true;
        } catch (ConditionalCheckFailedException e) {
            return false;
        }
    }

    @Override
    public long available() {
        return dynamo.scan(b -> b.tableName(table).consistentRead(true).filterExpression("#s = :a")
                .expressionAttributeNames(Map.of("#s", "status"))
                .expressionAttributeValues(Map.of(":a", AttributeValue.fromS("AVAILABLE")))).count();
    }
}
