package com.vedantbakliwal.fooddelivery.aws;

/** Names and addresses of the AWS resources the service uses. */
public record AwsResources(String ordersTable, String keysTable, String outboxTable, String couriersTable,
                           String topicArn, String restaurantQueueUrl, String restaurantDlqUrl,
                           String dispatchQueueUrl, String dispatchDlqUrl) {
}
