# Food Delivery Order Service

An order-to-dispatch backend for a made-up food delivery app, built to show the failure handling that event-driven systems need: idempotent order placement, a transactional outbox, at-least-once delivery with retries and dead-letter queues, optimistic locking, and one trace per order across three services.

Runs on DynamoDB, SNS and SQS (Moto locally, real AWS with one setting), as one Spring Boot image in three roles. All restaurants, menus and customers are invented.

```
POST /orders ──► order-api ──(one DynamoDB transaction: order + idempotency key + outbox row)
                     │
                  outbox relay ──► SNS topic "order-events" (filter policy on event type)
                                     ├─► SQS restaurant (+DLQ) ──► restaurant-worker: accept, or reject (closed / sold out)
                                     └─► SQS dispatch   (+DLQ) ──► dispatch-worker: claim nearest free courier,
                                                                    release it on OrderDelivered
order states: PLACED → ACCEPTED → COURIER_ASSIGNED → PICKED_UP → DELIVERED   (or PLACED → REJECTED)
```

## What it handles, and the test that proves each one

| Failure | How it is handled | Test |
|---|---|---|
| Client retries a POST after a timeout | `Idempotency-Key` header; the key, order and outbox row are written in one `TransactWriteItems`, so a retry returns the original order (200) | `OrderFlowTest.concurrentRequestsWithOneKeyCreateOneOrderAndOneEvent` (50 threads, 1 order), k6 retries every order |
| Service crashes between saving and publishing | Transactional outbox: the event is stored with the order, a relay publishes it later | `OrderFlowTest`, `AwsMotoTest.orderFlowsThroughSnsAndSqs` |
| The same event is delivered twice | Workers check the order's version and status, so a duplicate is a no-op | `OrderFlowTest.duplicateDeliveryOfAnEventIsHarmless` |
| Two writers race on one order | Optimistic locking (`#v = :expected` condition) | `AwsMotoTest.staleVersionWriteIsRefused` |
| No courier is free | Normal at rush hour: the message is retried every second (`ChangeMessageVisibility`) for about 30 s, then dead-lettered and redriven with `POST /ops/queues/dispatch/redrive` | `OrderFlowTest.noFreeCourierRetriesThenDeadLettersThenRedrives` |
| Dispatcher crashes after claiming a courier | Claims are keyed by order, so the retry gets the same courier back instead of tying up a second one | `OrderFlowTest.retryAfterACrashReusesTheCourierInsteadOfLeakingOne`, `AwsMotoTest.aCourierCannotBeClaimedTwice`; k6 fails if any courier is still busy after the run |
| Two dispatchers grab one courier | Conditional update `AVAILABLE → BUSY` | `AwsMotoTest.aCourierCannotBeClaimedTwice` |
| Client sends its own prices | Prices come from the menu on the server | `OrderFlowTest.orderGoesFromPlacedToDeliveredAndFreesTheCourier` |
| An event schema changes | Golden JSON contracts in `src/test/resources/contracts`; producer output and consumer parsing are both checked | `EventContractTest` |

## Run it

Needs Java 21+ and Maven. Docker is optional (the AWS tests skip without it).

```bash
mvn -B verify                        # unit, contract, API tests; AWS tests on Moto if Docker is running
mvn spring-boot:run                  # single process, in-memory queues, http://localhost:8080
```

Full system (Moto, Jaeger, API, two workers):

```bash
docker compose up --build -d --wait
curl -s -X POST localhost:8080/orders -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-0001' \
  -d '{"customerId":"c1","restaurantId":"r-taco-lab","zone":"east","items":[{"sku":"taco-al-pastor","quantity":2}]}'
curl -s localhost:8080/ops/queues
docker run --rm --network host -v "$PWD/load:/load" grafana/k6:0.54.0 run /load/orders.js
open http://localhost:16686          # Jaeger: one trace from POST /orders through both workers
docker compose down -v
```

The k6 run fails if any order is duplicated, any message ends in a dead-letter queue, or a courier is still busy after every order has finished, or p95 latency is over budget (place under 500 ms, courier assigned under 8 s).

## Kubernetes

`k8s/` has a Deployment per role (API + relay, restaurant worker, dispatch worker) with startup, liveness and readiness probes, a ConfigMap, and CPU-based HPAs. Workers scale independently of the API. On kind:

```bash
docker build -t food-delivery:local . && kind load docker-image food-delivery:local
kubectl apply -k k8s/ && kubectl -n food-delivery get pods
```

CI renders the manifests with kustomize and validates them with kubeconform.

## Real AWS

Set `fooddelivery.aws.endpoint` to empty and give the app AWS credentials (an IAM role in EKS). Tables, the topic, queues, DLQs and subscriptions are created at startup if missing. Everything is pay-per-request, so an idle stack costs close to nothing, but delete the resources when you are done.

## Layout

```
core/      domain, state machine, OrderService, outbox relay, workers (no framework code)
memory/    in-memory store, SQS-like queue with visibility timeout and DLQ, event bus, couriers
aws/       DynamoDB store (transactions, conditions), SNS publisher, SQS queue, resource setup
tracing/   W3C traceparent through SNS/SQS message attributes
web/       REST API and ops endpoints
```

## License

MIT
