package com.vedantbakliwal.fooddelivery.aws;

import com.vedantbakliwal.fooddelivery.core.EventPublisher;
import com.vedantbakliwal.fooddelivery.core.OrderEvent;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;

import java.util.HashMap;
import java.util.Map;

/**
 * Publishes to the order-events topic. The event type is a message attribute so each queue's
 * filter policy receives only what its worker handles; the W3C traceparent rides along so the
 * consumer continues the same trace.
 */
public class SnsEventPublisher implements EventPublisher {

    private final SnsClient sns;
    private final String topicArn;
    private final Json json;

    public SnsEventPublisher(SnsClient sns, String topicArn, Json json) {
        this.sns = sns;
        this.topicArn = topicArn;
        this.json = json;
    }

    @Override
    public void publish(OrderEvent event, String traceparent) {
        Map<String, MessageAttributeValue> attributes = new HashMap<>();
        attributes.put("type", MessageAttributeValue.builder().dataType("String").stringValue(event.type().name()).build());
        if (traceparent != null) {
            attributes.put("traceparent", MessageAttributeValue.builder().dataType("String").stringValue(traceparent).build());
        }
        sns.publish(b -> b.topicArn(topicArn).message(json.write(event)).messageAttributes(attributes));
    }
}
