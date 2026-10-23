package com.vedantbakliwal.fooddelivery;

import com.vedantbakliwal.fooddelivery.core.OutboxRelay;
import com.vedantbakliwal.fooddelivery.core.QueueConsumer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Runs the relay and the workers this instance has the role for. */
@Component
public class BackgroundJobs {

    private final DeliveryProperties props;
    private final OutboxRelay relay;
    private final QueueConsumer restaurant;
    private final QueueConsumer dispatch;

    public BackgroundJobs(DeliveryProperties props, OutboxRelay relay,
                          @Qualifier("restaurantConsumer") QueueConsumer restaurant,
                          @Qualifier("dispatchConsumer") QueueConsumer dispatch) {
        this.props = props;
        this.relay = relay;
        this.restaurant = restaurant;
        this.dispatch = dispatch;
    }

    @Scheduled(fixedDelayString = "${fooddelivery.poll-ms:250}")
    public void relay() {
        if (props.has("relay")) relay.relayOnce(25);
    }

    @Scheduled(fixedDelayString = "${fooddelivery.poll-ms:250}")
    public void restaurantWorker() {
        if (props.has("restaurant")) restaurant.pollOnce(10);
    }

    @Scheduled(fixedDelayString = "${fooddelivery.poll-ms:250}")
    public void dispatchWorker() {
        if (props.has("dispatch")) dispatch.pollOnce(10);
    }
}
