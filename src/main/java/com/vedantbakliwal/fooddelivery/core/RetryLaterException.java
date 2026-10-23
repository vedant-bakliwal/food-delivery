package com.vedantbakliwal.fooddelivery.core;

import java.time.Duration;

/**
 * A temporary condition (for example no courier free). The message comes back after {@link #delay()},
 * and is dead-lettered only once the queue's maximum receive count is reached.
 */
public class RetryLaterException extends RuntimeException {

    private final Duration delay;

    public RetryLaterException(String message) {
        this(message, Duration.ofSeconds(1));
    }

    public RetryLaterException(String message, Duration delay) {
        super(message);
        this.delay = delay;
    }

    public Duration delay() {
        return delay;
    }
}
