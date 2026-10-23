package com.vedantbakliwal.fooddelivery.aws;

import com.vedantbakliwal.fooddelivery.core.MessageQueue;
import com.vedantbakliwal.fooddelivery.core.QueueMessage;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.util.List;
import java.util.Map;

/** An SQS queue with its dead-letter queue. Redelivery and dead-lettering are done by SQS itself. */
public class SqsMessageQueue implements MessageQueue {

    private final SqsClient sqs;
    private final String name;
    private final String queueUrl;
    private final String dlqUrl;
    private final Json json;

    public SqsMessageQueue(SqsClient sqs, String name, String queueUrl, String dlqUrl, Json json) {
        this.sqs = sqs;
        this.name = name;
        this.queueUrl = queueUrl;
        this.dlqUrl = dlqUrl;
        this.json = json;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public List<QueueMessage> receive(int max) {
        return sqs.receiveMessage(b -> b.queueUrl(queueUrl).maxNumberOfMessages(Math.min(max, 10)).waitTimeSeconds(1)
                        .messageSystemAttributeNames(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT)
                        .messageAttributeNames("All"))
                .messages().stream().map(this::toMessage).toList();
    }

    private QueueMessage toMessage(Message m) {
        var trace = m.messageAttributes().get("traceparent");
        String count = m.attributes().get(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT);
        return new QueueMessage(m.messageId(), m.receiptHandle(), json.eventOrNull(m.body()),
                trace == null ? null : trace.stringValue(), count == null ? 1 : Integer.parseInt(count));
    }

    @Override
    public void delete(QueueMessage message) {
        sqs.deleteMessage(b -> b.queueUrl(queueUrl).receiptHandle(message.receipt()));
    }

    @Override
    public void retryAfter(QueueMessage message, java.time.Duration delay) {
        sqs.changeMessageVisibility(b -> b.queueUrl(queueUrl).receiptHandle(message.receipt())
                .visibilityTimeout((int) Math.max(0, delay.toSeconds())));
    }

    @Override
    public long depth() {
        return count(queueUrl);
    }

    @Override
    public long deadLetterDepth() {
        return count(dlqUrl);
    }

    private long count(String url) {
        Map<QueueAttributeName, String> a = sqs.getQueueAttributes(b -> b.queueUrl(url).attributeNames(
                QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE)).attributes();
        return Long.parseLong(a.getOrDefault(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, "0"))
                + Long.parseLong(a.getOrDefault(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE, "0"));
    }

    /** Copies dead-lettered messages back to the main queue (with their attributes), then deletes them from the DLQ. */
    @Override
    public int redriveDeadLetters() {
        int moved = 0;
        while (true) {
            List<Message> batch = sqs.receiveMessage(b -> b.queueUrl(dlqUrl).maxNumberOfMessages(10).waitTimeSeconds(0)
                    .messageAttributeNames("All")).messages();
            if (batch.isEmpty()) return moved;
            for (Message m : batch) {
                sqs.sendMessage(b -> b.queueUrl(queueUrl).messageBody(m.body()).messageAttributes(m.messageAttributes()));
                sqs.deleteMessage(b -> b.queueUrl(dlqUrl).receiptHandle(m.receiptHandle()));
                moved++;
            }
        }
    }
}
