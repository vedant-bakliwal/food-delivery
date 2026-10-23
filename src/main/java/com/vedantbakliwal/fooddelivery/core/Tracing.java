package com.vedantbakliwal.fooddelivery.core;

import java.util.function.Supplier;

/** Trace propagation across the queue hop, in W3C traceparent form. */
public interface Tracing {

    /** The current trace context, or null. */
    String currentTraceparent();

    /** Runs work in a new span that continues the given parent trace. */
    <T> T inSpan(String name, String parentTraceparent, Supplier<T> work);

    Tracing NONE = new Tracing() {
        @Override
        public String currentTraceparent() {
            return null;
        }

        @Override
        public <T> T inSpan(String name, String parent, Supplier<T> work) {
            return work.get();
        }
    };
}
