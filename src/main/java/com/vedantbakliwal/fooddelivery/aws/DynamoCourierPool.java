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
    public Optional<String> claimNearest(String zone) {
        var free = dynamo.scan(b -> b.tableName(table).consistentRead(true)
                        .filterExpression("#s = :a").expressionAttributeNames(Map.of("#s", "status"))
                        .expressionAttributeValues(Map.of(":a", AttributeValue.fromS("AVAILABLE")))).items().stream()
                .sorted(Comparator.comparingInt(i -> Menu.distance(i.get("zone").s(), zone)))
                .map(i -> i.get("pk").s())
                .toList();
        for (String id : free) {
            if (setStatus(id, "AVAILABLE", "BUSY")) return Optional.of(id);
        }
        return Optional.empty();
    }

    @Override
    public void release(String courierId) {
        setStatus(courierId, "BUSY", "AVAILABLE");
    }

    @Override
    public long available() {
        return dynamo.scan(b -> b.tableName(table).consistentRead(true).filterExpression("#s = :a")
                .expressionAttributeNames(Map.of("#s", "status"))
                .expressionAttributeValues(Map.of(":a", AttributeValue.fromS("AVAILABLE")))).count();
    }

    private boolean setStatus(String id, String from, String to) {
        try {
            dynamo.updateItem(b -> b.tableName(table).key(Map.of("pk", AttributeValue.fromS(id)))
                    .updateExpression("SET #s = :to").conditionExpression("#s = :from")
                    .expressionAttributeNames(Map.of("#s", "status"))
                    .expressionAttributeValues(Map.of(":to", AttributeValue.fromS(to), ":from", AttributeValue.fromS(from))));
            return true;
        } catch (ConditionalCheckFailedException e) {
            return false;
        }
    }
}
