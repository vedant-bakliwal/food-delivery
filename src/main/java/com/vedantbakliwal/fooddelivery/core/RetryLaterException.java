package com.vedantbakliwal.fooddelivery.core;

/** A temporary condition (for example no courier free). The message is retried, then dead-lettered. */
public class RetryLaterException extends RuntimeException {

    public RetryLaterException(String message) {
        super(message);
    }
}
