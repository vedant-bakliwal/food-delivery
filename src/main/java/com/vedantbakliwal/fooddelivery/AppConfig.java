package com.vedantbakliwal.fooddelivery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vedantbakliwal.fooddelivery.aws.AwsResources;
import com.vedantbakliwal.fooddelivery.aws.AwsSetup;
import com.vedantbakliwal.fooddelivery.aws.DynamoCourierPool;
import com.vedantbakliwal.fooddelivery.aws.DynamoOrderStore;
import com.vedantbakliwal.fooddelivery.aws.Json;
import com.vedantbakliwal.fooddelivery.aws.SnsEventPublisher;
import com.vedantbakliwal.fooddelivery.aws.SqsMessageQueue;
import com.vedantbakliwal.fooddelivery.core.CourierPool;
import com.vedantbakliwal.fooddelivery.core.DispatchWorker;
import com.vedantbakliwal.fooddelivery.core.EventPublisher;
import com.vedantbakliwal.fooddelivery.core.MessageQueue;
import com.vedantbakliwal.fooddelivery.core.OrderEvent;
import com.vedantbakliwal.fooddelivery.core.OrderService;
import com.vedantbakliwal.fooddelivery.core.OrderStore;
import com.vedantbakliwal.fooddelivery.core.OutboxRelay;
import com.vedantbakliwal.fooddelivery.core.QueueConsumer;
import com.vedantbakliwal.fooddelivery.core.RestaurantWorker;
import com.vedantbakliwal.fooddelivery.core.Tracing;
import com.vedantbakliwal.fooddelivery.memory.InMemoryCourierPool;
import com.vedantbakliwal.fooddelivery.memory.InMemoryEventBus;
import com.vedantbakliwal.fooddelivery.memory.InMemoryOrderStore;
import com.vedantbakliwal.fooddelivery.memory.InMemoryQueue;
import com.vedantbakliwal.fooddelivery.tracing.MicrometerTracing;
import io.micrometer.observation.ObservationPredicate;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.http.server.observation.ServerRequestObservationContext;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.Set;

@Configuration
public class AppConfig {

    /** Keeps traces about orders: drops the spans for every poll tick and every health probe. */
    @Bean
    ObservationPredicate noPollingOrProbeSpans() {
        return (name, context) -> !name.startsWith("tasks.scheduled")
                && !(context instanceof ServerRequestObservationContext http
                     && http.getCarrier().getRequestURI().startsWith("/actuator"));
    }

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    Tracing tracing(ObjectProvider<Tracer> tracer, ObjectProvider<Propagator> propagator) {
        Tracer t = tracer.getIfAvailable();
        Propagator p = propagator.getIfAvailable();
        return t == null || p == null ? Tracing.NONE : new MicrometerTracing(t, p);
    }

    @Bean
    OrderService orderService(OrderStore store, Tracing tracing, Clock clock) {
        return new OrderService(store, tracing, clock);
    }

    @Bean
    OutboxRelay outboxRelay(OrderStore store, EventPublisher publisher) {
        return new OutboxRelay(store, publisher);
    }

    @Bean
    QueueConsumer restaurantConsumer(@Qualifier("restaurantQueue") MessageQueue queue, OrderStore store, OrderService orders,
                                     Tracing tracing, Clock clock) {
        return new QueueConsumer(queue, new RestaurantWorker(store, orders, tracing, clock), tracing);
    }

    @Bean
    QueueConsumer dispatchConsumer(@Qualifier("dispatchQueue") MessageQueue queue, OrderStore store, OrderService orders,
                                   CourierPool couriers, Tracing tracing, Clock clock) {
        return new QueueConsumer(queue, new DispatchWorker(store, orders, couriers, tracing, clock), tracing);
    }

    /** Default: one process, in-memory store and queues with SQS-like retry and dead-letter behaviour. */
    @Configuration
    @ConditionalOnProperty(name = "fooddelivery.store", havingValue = "memory", matchIfMissing = true)
    static class Memory {
        @Bean
        OrderStore orderStore() {
            return new InMemoryOrderStore();
        }

        @Bean
        InMemoryQueue restaurantQueue(DeliveryProperties p, Clock clock) {
            return new InMemoryQueue("restaurant", Duration.ofSeconds(p.visibilitySeconds()), p.maxReceiveCount(), clock);
        }

        @Bean
        InMemoryQueue dispatchQueue(DeliveryProperties p, Clock clock) {
            return new InMemoryQueue("dispatch", Duration.ofSeconds(p.visibilitySeconds()), p.maxReceiveCount(), clock);
        }

        @Bean
        EventPublisher eventPublisher(@Qualifier("restaurantQueue") InMemoryQueue restaurant, @Qualifier("dispatchQueue") InMemoryQueue dispatch) {
            return new InMemoryEventBus()
                    .subscribe(restaurant, Set.of(OrderEvent.Type.OrderPlaced))
                    .subscribe(dispatch, Set.of(OrderEvent.Type.OrderAccepted, OrderEvent.Type.OrderDelivered));
        }

        @Bean
        CourierPool courierPool(DeliveryProperties p) {
            return new InMemoryCourierPool(p.couriers());
        }
    }

    /** AWS: DynamoDB, SNS and SQS. Point fooddelivery.aws.endpoint at Moto locally, or leave it empty for real AWS. */
    @Configuration
    @ConditionalOnProperty(name = "fooddelivery.store", havingValue = "aws")
    static class Aws {

        private static <B extends AwsClientBuilder<B, C>, C> C build(B builder, DeliveryProperties p) {
            builder.region(Region.of(p.aws().region()));
            String endpoint = p.aws().endpoint();
            if (endpoint != null && !endpoint.isBlank()) {
                builder.endpointOverride(URI.create(endpoint))
                        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")));
            } else {
                builder.credentialsProvider(DefaultCredentialsProvider.builder().build());
            }
            return builder.build();
        }

        @Bean
        DynamoDbClient dynamoDbClient(DeliveryProperties p) {
            return build(DynamoDbClient.builder(), p);
        }

        @Bean
        SnsClient snsClient(DeliveryProperties p) {
            return build(SnsClient.builder(), p);
        }

        @Bean
        SqsClient sqsClient(DeliveryProperties p) {
            return build(SqsClient.builder(), p);
        }

        @Bean
        AwsResources awsResources(DynamoDbClient dynamo, SnsClient sns, SqsClient sqs, DeliveryProperties p) {
            return AwsSetup.ensure(dynamo, sns, sqs, p.aws().prefix(), p.maxReceiveCount(), p.visibilitySeconds(), p.couriers());
        }

        @Bean
        Json json(ObjectMapper mapper) {
            return new Json(mapper);
        }

        @Bean
        OrderStore orderStore(DynamoDbClient dynamo, AwsResources r, Json json) {
            return new DynamoOrderStore(dynamo, r, json);
        }

        @Bean
        EventPublisher eventPublisher(SnsClient sns, AwsResources r, Json json) {
            return new SnsEventPublisher(sns, r.topicArn(), json);
        }

        @Bean
        MessageQueue restaurantQueue(SqsClient sqs, AwsResources r, Json json) {
            return new SqsMessageQueue(sqs, "restaurant", r.restaurantQueueUrl(), r.restaurantDlqUrl(), json);
        }

        @Bean
        MessageQueue dispatchQueue(SqsClient sqs, AwsResources r, Json json) {
            return new SqsMessageQueue(sqs, "dispatch", r.dispatchQueueUrl(), r.dispatchDlqUrl(), json);
        }

        @Bean
        CourierPool courierPool(DynamoDbClient dynamo, AwsResources r) {
            return new DynamoCourierPool(dynamo, r.couriersTable());
        }
    }
}
