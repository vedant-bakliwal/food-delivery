package com.vedantbakliwal.fooddelivery.aws;

import com.vedantbakliwal.fooddelivery.core.Menu;
import com.vedantbakliwal.fooddelivery.core.OrderEvent;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.SubscribeRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Creates the tables, topic, queues (each with a dead-letter queue) and filtered subscriptions.
 * Every call is idempotent, so all instances can run it at startup against Moto. In a real AWS
 * account these resources would come from infrastructure-as-code instead.
 */
public final class AwsSetup {

    private AwsSetup() {
    }

    public static AwsResources ensure(DynamoDbClient dynamo, SnsClient sns, SqsClient sqs, String prefix,
                                      int maxReceiveCount, int dispatchMaxReceiveCount,
                                      int visibilitySeconds, int couriers) {
        String orders = table(dynamo, prefix + "-orders");
        String keys = table(dynamo, prefix + "-idempotency");
        String outbox = table(dynamo, prefix + "-outbox");
        String courierTable = table(dynamo, prefix + "-couriers");
        for (int i = 1; i <= couriers; i++) {
            try {
                dynamo.putItem(PutItemRequest.builder().tableName(courierTable)
                        .item(Map.of("pk", AttributeValue.fromS("courier-" + i),
                                "zone", AttributeValue.fromS(Menu.ZONES.get((i - 1) % Menu.ZONES.size())),
                                "status", AttributeValue.fromS("AVAILABLE")))
                        .conditionExpression("attribute_not_exists(pk)").build());
            } catch (software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException ignored) {
                // courier already exists: keep its current status
            }
        }

        String topicArn = sns.createTopic(b -> b.name(prefix + "-order-events")).topicArn();
        String[] restaurant = queueWithDlq(sqs, prefix + "-restaurant", maxReceiveCount, visibilitySeconds, topicArn);
        String[] dispatch = queueWithDlq(sqs, prefix + "-dispatch", dispatchMaxReceiveCount, visibilitySeconds, topicArn);
        subscribe(sns, sqs, topicArn, restaurant[0], OrderEvent.Type.OrderPlaced);
        subscribe(sns, sqs, topicArn, dispatch[0], OrderEvent.Type.OrderAccepted, OrderEvent.Type.OrderDelivered);
        return new AwsResources(orders, keys, outbox, courierTable, topicArn, restaurant[0], restaurant[1], dispatch[0], dispatch[1]);
    }

    private static String table(DynamoDbClient dynamo, String name) {
        try {
            dynamo.createTable(CreateTableRequest.builder().tableName(name)
                    .keySchema(KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build())
                    .attributeDefinitions(AttributeDefinition.builder().attributeName("pk").attributeType(ScalarAttributeType.S).build())
                    .billingMode(BillingMode.PAY_PER_REQUEST).build());
        } catch (ResourceInUseException ignored) {
            // already exists
        }
        dynamo.waiter().waitUntilTableExists(b -> b.tableName(name)); // real AWS creates tables asynchronously
        return name;
    }

    /** Returns {queueUrl, dlqUrl}. */
    private static String[] queueWithDlq(SqsClient sqs, String name, int maxReceiveCount, int visibilitySeconds, String topicArn) {
        String dlqUrl = sqs.createQueue(b -> b.queueName(name + "-dlq")).queueUrl();
        String dlqArn = arn(sqs, dlqUrl);
        String redrive = "{\"deadLetterTargetArn\":\"" + dlqArn + "\",\"maxReceiveCount\":\"" + maxReceiveCount + "\"}";
        String queueUrl = sqs.createQueue(b -> b.queueName(name).attributes(Map.of(
                QueueAttributeName.REDRIVE_POLICY, redrive,
                QueueAttributeName.VISIBILITY_TIMEOUT, String.valueOf(visibilitySeconds)))).queueUrl();
        String queueArn = arn(sqs, queueUrl);
        // Lets the topic deliver to this queue (needed in real AWS).
        String policy = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"sns.amazonaws.com\"},"
                + "\"Action\":\"sqs:SendMessage\",\"Resource\":\"" + queueArn + "\",\"Condition\":{\"ArnEquals\":{\"aws:SourceArn\":\"" + topicArn + "\"}}}]}";
        sqs.setQueueAttributes(b -> b.queueUrl(queueUrl).attributes(Map.of(QueueAttributeName.POLICY, policy)));
        return new String[]{queueUrl, dlqUrl};
    }

    static String arn(SqsClient sqs, String queueUrl) {
        return sqs.getQueueAttributes(GetQueueAttributesRequest.builder().queueUrl(queueUrl)
                .attributeNames(QueueAttributeName.QUEUE_ARN).build()).attributes().get(QueueAttributeName.QUEUE_ARN);
    }

    private static void subscribe(SnsClient sns, SqsClient sqs, String topicArn, String queueUrl, OrderEvent.Type... types) {
        String filter = "{\"type\":[" + Arrays.stream(types).map(t -> "\"" + t + "\"").collect(Collectors.joining(",")) + "]}";
        sns.subscribe(SubscribeRequest.builder().topicArn(topicArn).protocol("sqs").endpoint(arn(sqs, queueUrl))
                .attributes(Map.of("RawMessageDelivery", "true", "FilterPolicy", filter)).returnSubscriptionArn(true).build());
    }
}
