package com.vedantbakliwal.fooddelivery;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.vedantbakliwal.fooddelivery.aws.AwsResources;
import com.vedantbakliwal.fooddelivery.aws.AwsSetup;
import com.vedantbakliwal.fooddelivery.aws.DynamoCourierPool;
import com.vedantbakliwal.fooddelivery.aws.DynamoOrderStore;
import com.vedantbakliwal.fooddelivery.aws.Json;
import com.vedantbakliwal.fooddelivery.aws.SnsEventPublisher;
import com.vedantbakliwal.fooddelivery.aws.SqsMessageQueue;
import com.vedantbakliwal.fooddelivery.core.DispatchWorker;
import com.vedantbakliwal.fooddelivery.core.Order;
import com.vedantbakliwal.fooddelivery.core.OrderEvent;
import com.vedantbakliwal.fooddelivery.core.OrderService;
import com.vedantbakliwal.fooddelivery.core.OrderService.ItemRequest;
import com.vedantbakliwal.fooddelivery.core.OrderStatus;
import com.vedantbakliwal.fooddelivery.core.OutboxRecord;
import com.vedantbakliwal.fooddelivery.core.OutboxRelay;
import com.vedantbakliwal.fooddelivery.core.QueueConsumer;
import com.vedantbakliwal.fooddelivery.core.RestaurantWorker;
import com.vedantbakliwal.fooddelivery.core.Tracing;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.net.URI;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The same flow against DynamoDB, SNS and SQS APIs, served by Moto in Docker.
 * Checks the parts the in-memory adapters can only imitate: TransactWriteItems, conditional
 * version checks, SNS filter policies into SQS, and conditional courier claims.
 * Skipped automatically when Docker is not available.
 */
@Testcontainers(disabledWithoutDocker = true)
class AwsMotoTest {

    @Container
    static final GenericContainer<?> MOTO = new GenericContainer<>("motoserver/moto:latest")
            .withExposedPorts(5000)
            .waitingFor(Wait.forHttp("/moto-api/").forStatusCode(200));

    static DynamoDbClient dynamo;
    static OrderService orders;
    static DynamoOrderStore store;
    static DynamoCourierPool couriers;
    static OutboxRelay relay;
    static QueueConsumer restaurant;
    static QueueConsumer dispatch;
    static SqsMessageQueue dispatchQueue;

    @BeforeAll
    static void wire() {
        URI endpoint = URI.create("http://" + MOTO.getHost() + ":" + MOTO.getMappedPort(5000));
        StaticCredentialsProvider creds = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));
        dynamo = DynamoDbClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1).credentialsProvider(creds).build();
        SnsClient sns = SnsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1).credentialsProvider(creds).build();
        SqsClient sqs = SqsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1).credentialsProvider(creds).build();

        AwsResources r = AwsSetup.ensure(dynamo, sns, sqs, "it", 3, 30, 2, 4);
        Json json = new Json(JsonMapper.builder().findAndAddModules()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS).build());
        store = new DynamoOrderStore(dynamo, r, json);
        couriers = new DynamoCourierPool(dynamo, r.couriersTable());
        orders = new OrderService(store, Tracing.NONE, Clock.systemUTC());
        relay = new OutboxRelay(store, new SnsEventPublisher(sns, r.topicArn(), json));
        SqsMessageQueue restaurantQueue = new SqsMessageQueue(sqs, "restaurant", r.restaurantQueueUrl(), r.restaurantDlqUrl(), json);
        dispatchQueue = new SqsMessageQueue(sqs, "dispatch", r.dispatchQueueUrl(), r.dispatchDlqUrl(), json);
        restaurant = new QueueConsumer(restaurantQueue, new RestaurantWorker(store, orders, Tracing.NONE, Clock.systemUTC()), Tracing.NONE);
        dispatch = new QueueConsumer(dispatchQueue, new DispatchWorker(store, orders, couriers, Tracing.NONE, Clock.systemUTC()), Tracing.NONE);
    }

    static Order until(Supplier<Order> read, OrderStatus status) throws InterruptedException {
        Order o = null;
        for (int i = 0; i < 60; i++) {
            relay.relayOnce(50);
            restaurant.pollOnce(10);
            dispatch.pollOnce(10);
            o = read.get();
            if (o.status() == status) return o;
            Thread.sleep(100);
        }
        throw new AssertionError("Expected " + status + " but was " + (o == null ? null : o.status()));
    }

    @Test
    void orderFlowsThroughSnsAndSqs() throws Exception {
        OrderService.Result r = orders.place("c1", "r-brisket-co", "downtown", List.of(new ItemRequest("brisket-plate", 1)), "moto-key-1");
        assertThat(orders.place("c1", "r-brisket-co", "downtown", List.of(new ItemRequest("brisket-plate", 1)), "moto-key-1").order().orderId())
                .isEqualTo(r.order().orderId());
        String id = r.order().orderId();

        Order assigned = until(() -> orders.find(id).orElseThrow(), OrderStatus.COURIER_ASSIGNED);
        assertThat(assigned.courierId()).isEqualTo("courier-1"); // courier-1 is in downtown, the nearest
        orders.pickUp(id, assigned.courierId());
        orders.deliver(id, assigned.courierId());
        assertThat(until(() -> orders.find(id).orElseThrow(), OrderStatus.DELIVERED).version()).isEqualTo(5);
        assertThat(store.pendingOutbox(10)).isEmpty();
    }

    @Test
    void staleVersionWriteIsRefused() {
        Order o = orders.place("c2", "r-taco-lab", "east", List.of(new ItemRequest("taco-veggie", 1)), "moto-key-2").order();
        Order accepted = o.moveTo(OrderStatus.ACCEPTED, null, null, o.updatedAt());
        OutboxRecord event = new OutboxRecord(orders.event(OrderEvent.Type.OrderAccepted, accepted, Map.of()), null);
        assertThat(store.update(accepted, o.version(), event)).isTrue();
        assertThat(store.update(accepted, o.version(), event)).isFalse(); // someone else already moved it
        assertThat(store.find(o.orderId()).orElseThrow().version()).isEqualTo(2);
    }

    @Test
    void aCourierCannotBeClaimedTwice() {
        String a = couriers.claimFor("ord-a", "north").orElseThrow();
        String b = couriers.claimFor("ord-b", "north").orElseThrow();
        assertThat(a).isNotEqualTo(b);
        assertThat(couriers.claimFor("ord-a", "north")).contains(a); // a retry for the same order gets the same courier
        couriers.release(a);
        couriers.release(b);
    }
}
