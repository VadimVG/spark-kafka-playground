# Event Simulator

Fake e-commerce event generator for testing streaming data pipelines.

Generates events of three types: **sales**, **orders**, and **logs**. Designed to produce realistic, joinable data with shared user IDs and order lifecycles.

## Event Types

### sales
A completed purchase. One event = one product bought.
```json
{"user_id": 768, "product_id": 21, "amount": 44.99, "sale_id": "606d5f4a7aa0", "timestamp": "2026-08-08T09:16:57.260113+00:00"}
```

### orders

Order lifecycle events. Each item in an order gets its own event. Status changes per item. An order can be partially shipped or cancelled.
```json
{"order_number": "ORD-0000001", "user_id": 286, "product_id": 5, "quantity": 2, "price": 99.99, "status": "created", "total": 362.99, "order_id": "9967098c760d", "timestamp": "2026-08-08T09:17:02.261256+00:00"}
```
Status flow: created → paid → shipped → delivered (or cancelled, refunded, lost at certain steps). Probabilities configured in config.py.

### logs
User activity on pages. No order linkage.
```json
{"user_id": 512, "action": "page_view", "page": "catalog", "log_id": "abc123def456", "timestamp": "2026-08-08T09:17:05.123456+00:00"}
```

Actions: page_view, add_to_cart, checkout, payment.\
Pages: home, catalog, product, cart, checkout, profile.

## Load Levels

Selected interactively at startup.
- low - 0.2 rps
- low - 1 rps
- low - 10 rps

## Structure

```
event-simulator/
├── main.py       # Entry point, load level prompt, event loop
├── events.py     # EventGenerator class, order lifecycle logic
├── models.py     # Dataclasses and StrEnums for event types
├── config.py     # Rates, products catalog, status transitions, user count
```

## Configuration

Edit config.py to adjust:
- RATES — messages per second per topic for each load level
- NUM_USERS — total unique users in the system
- PRODUCTS — product catalog (id → price)
- STATUS_TRANSITIONS — order status flow and probabilities
- PAGES — available pages for log events

## Design Notes

Shared user IDs across all three event types — enables joins in downstream analytics.

Repeating order_number across items and status changes — one order, multiple events.

Unique order_id and sale_id per event row — every Kafka message has its own identifier.

Per-item status tracking — each product inside an order has its own status. Two items in the same order can be at different stages (e.g. one `delivered`, another still `shipped`).

No external dependencies. Pure Python stdlib.

