package com.vedantbakliwal.fooddelivery.tracing;

import com.vedantbakliwal.fooddelivery.core.Tracing;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Carries the trace across the queue: the producer writes its W3C traceparent into the message,
 * the consumer starts a child span from it. One trace then covers the HTTP request, the outbox
 * publish and both workers (OpenTelemetry via Micrometer Tracing, exported over OTLP).
 */
public class MicrometerTracing implements Tracing {

    private final Tracer tracer;
    private final Propagator propagator;

    public MicrometerTracing(Tracer tracer, Propagator propagator) {
        this.tracer = tracer;
        this.propagator = propagator;
    }

    @Override
    public String currentTraceparent() {
        Span span = tracer.currentSpan();
        if (span == null) {
            return null;
        }
        Map<String, String> carrier = new HashMap<>();
        propagator.inject(span.context(), carrier, Map::put);
        return carrier.get("traceparent");
    }

    @Override
    public <T> T inSpan(String name, String parentTraceparent, Supplier<T> work) {
        Span.Builder builder = parentTraceparent == null
                ? tracer.spanBuilder()
                : propagator.extract(Map.of("traceparent", parentTraceparent), Map::get);
        Span span = builder.name(name).kind(Span.Kind.CONSUMER).start();
        try (Tracer.SpanInScope ignored = tracer.withSpan(span)) {
            return work.get();
        } catch (RuntimeException e) {
            span.error(e);
            throw e;
        } finally {
            span.end();
        }
    }
}
