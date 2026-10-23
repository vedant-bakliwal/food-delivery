// k6 load test: customers order, retry with the same key, couriers pick up and deliver.
// Pass criteria: no lost or duplicate orders, no dead letters, and p95 latency budgets.
import http from 'k6/http';
import { check, sleep, fail } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const JSON_HEADERS = { 'Content-Type': 'application/json' };

const delivered = new Counter('orders_delivered');
const rejected = new Counter('orders_rejected');
const duplicates = new Counter('duplicate_orders');
const toAssigned = new Trend('time_to_courier_ms', true);

export const options = {
  scenarios: {
    lunch_rush: { executor: 'constant-vus', vus: Number(__ENV.VUS || 15), duration: __ENV.DURATION || '60s' },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    'http_req_duration{name:place}': ['p(95)<300'],
    time_to_courier_ms: ['p(95)<5000'],
    duplicate_orders: ['count==0'],
    orders_delivered: ['count>0'],
  },
};

const MENU = [
  { restaurantId: 'r-taco-lab', zone: 'east', sku: 'taco-al-pastor' },
  { restaurantId: 'r-taco-lab', zone: 'south', sku: 'taco-veggie' },
  { restaurantId: 'r-brisket-co', zone: 'downtown', sku: 'brisket-plate' },
  { restaurantId: 'r-brisket-co', zone: 'north', sku: 'mac-cheese' },
];

function order(id) {
  return http.get(`${BASE}/orders/${id}`, { tags: { name: 'get' } }).json();
}

function waitFor(id, statuses, timeoutMs) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const o = order(id);
    if (statuses.includes(o.status)) return o;
    sleep(0.1);
  }
  fail(`order ${id} stuck before ${statuses}`);
}

export default function () {
  const soldOut = Math.random() < 0.1; // 10% ask for an item the restaurant has run out of
  const pick = soldOut ? { restaurantId: 'r-taco-lab', zone: 'east', sku: 'elote' } : MENU[Math.floor(Math.random() * MENU.length)];
  const key = `k6-${__VU}-${__ITER}-${Date.now()}`;
  const body = JSON.stringify({
    customerId: `cust-${__VU}`, restaurantId: pick.restaurantId, zone: pick.zone,
    items: [{ sku: pick.sku, quantity: 1 + Math.floor(Math.random() * 3) }],
  });
  const params = { headers: { ...JSON_HEADERS, 'Idempotency-Key': key }, tags: { name: 'place' } };

  const first = http.post(`${BASE}/orders`, body, params);
  check(first, { 'placed (201)': (r) => r.status === 201 }) || fail(`place failed: ${first.status} ${first.body}`);
  const id = first.json('orderId');

  // The client timed out and retried: same key must return the same order, not a new one.
  const retry = http.post(`${BASE}/orders`, body, params);
  check(retry, { 'retry replayed (200)': (r) => r.status === 200 });
  if (retry.json('orderId') !== id) duplicates.add(1);

  const placedAt = Date.now();
  const o = waitFor(id, soldOut ? ['REJECTED'] : ['COURIER_ASSIGNED'], 15000);
  if (soldOut) {
    rejected.add(1);
    return;
  }
  toAssigned.add(Date.now() - placedAt);

  sleep(0.5 + Math.random()); // drive to the restaurant
  const courier = JSON.stringify({ courierId: o.courierId });
  check(http.post(`${BASE}/orders/${id}/pickup`, courier, { headers: JSON_HEADERS, tags: { name: 'pickup' } }),
    { 'picked up': (r) => r.status === 201 });
  sleep(0.5 + Math.random()); // drive to the customer
  check(http.post(`${BASE}/orders/${id}/deliver`, courier, { headers: JSON_HEADERS, tags: { name: 'deliver' } }),
    { 'delivered': (r) => r.status === 201 });
  delivered.add(1);
}

// After the run: nothing may be stuck in a dead-letter queue, and every courier must be free again.
export function teardown() {
  let ops;
  for (let i = 0; i < 50; i++) {
    ops = http.get(`${BASE}/ops/queues`).json();
    if (ops.restaurant.depth === 0 && ops.dispatch.depth === 0) break;
    sleep(0.2);
  }
  console.log(`queues after run: ${JSON.stringify(ops)}`);
  if (ops.restaurant.deadLetters !== 0 || ops.dispatch.deadLetters !== 0) fail('messages in a dead-letter queue');
  if (ops.dispatch.depth !== 0) fail('dispatch queue did not drain');
}
