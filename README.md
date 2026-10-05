# Notification Service

Part of the DSA612S Distributed Food Delivery Platform. Written in Ballerina, stores data in MySQL.

It listens to Kafka events from the other services, decides who must be told (customer, restaurant or driver),
saves each notification in its own MySQL database and "sends" it on the right channels. A REST API on port 8085
lets clients read the notifications.

## Kafka topics consumed

| Topic | Published by | Fields used | Who is notified |
|---|---|---|---|
| `orders.created` | order-service | orderId, customerId, restaurantId, totalAmount | customer, restaurant |
| `orders.status.updated` | order-service | orderId, newStatus | customer (restaurant on DELIVERED / CANCELLED, driver when known) |
| `payments.completed` | payment-service | orderId, customerId, amount, currency | customer, restaurant |
| `payments.failed` | payment-service | orderId, customerId, reason | customer |
| `delivery.assigned` | delivery-service | orderId, driverId | driver, customer, restaurant |
| `delivery.completed` | delivery-service | orderId, driverId | driver, customer, restaurant |

Extra fields in the JSON are ignored. The status, payment and delivery events do not carry every id, so the service
saves the customer, restaurant and driver of each order (table `order_parties`) when `orders.created` arrives.

## Channels (simulated with log lines)

| Recipient | Channels |
|---|---|
| Customer | PUSH, EMAIL |
| Restaurant | DASHBOARD, SMS |
| Driver | PUSH, SMS |

## REST API (port 8085)

| Method | Path | Meaning |
|---|---|---|
| GET | `/notifications/health` | status check |
| GET | `/notifications?recipientType=&recipientId=&status=&maxResults=` | list, all filters optional |
| GET | `/notifications/customer/{customerId}?status=UNREAD` | one customer's notifications |
| GET | `/notifications/restaurant/{restaurantId}?status=UNREAD` | one restaurant's notifications |
| GET | `/notifications/driver/{driverId}?status=UNREAD` | one driver's notifications |
| GET | `/notifications/orders/{orderId}` | everything sent about one order |
| GET | `/notifications/{id}` | one notification |
| PUT | `/notifications/{id}/read` | mark as read |
| POST | `/notifications` | send a manual notification |

`status` is `UNREAD` or `READ`. POST body: `{"recipientType":"CUSTOMER","recipientId":"CUS-1","title":"Hi","message":"Hello"}`
(`orderId` optional).

## Database (`notification_db`)

- `notifications` - one row per notification. `event_key` is UNIQUE so a duplicate Kafka message is ignored.
- `order_parties` - customer, restaurant and driver of each order.

Tables are created automatically at startup.

## Run with Docker

1. Paste the two services from `docker-compose.snippet.yml` into the root `docker-compose.yml`.
2. From the repository root: `docker compose up --build kafka notification-db notification-service`
3. Check: `curl http://localhost:8085/notifications/health`

## Run locally

Start Kafka and the database (`docker compose up kafka notification-db`), then create `notification-service/Config.toml`
(ignored by git) with:

    dbPort = 3307

and run `bal run` inside `notification-service`.

## Test with sample events

Create the topics (3 partitions each):

    for t in orders.created orders.status.updated payments.completed payments.failed delivery.assigned delivery.completed; do
      docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic $t --partitions 3 --replication-factor 1
    done

Publish events (one JSON per line):

    echo '{"orderId":"ORD-1","customerId":"CUS-1","restaurantId":"RES-1","itemId":"ITM-1","quantity":2,"totalAmount":45.50,"status":"CREATED"}' | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders.created
    echo '{"paymentId":"pay-1","orderId":"ORD-1","customerId":"CUS-1","amount":45.50,"currency":"NAD","status":"COMPLETED"}' | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic payments.completed
    echo '{"orderId":"ORD-1","previousStatus":"CREATED","newStatus":"CONFIRMED"}' | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders.status.updated
    echo '{"orderId":"ORD-1","driverId":"DRV-1"}' | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic delivery.assigned
    echo '{"orderId":"ORD-1","driverId":"DRV-1"}' | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic delivery.completed

Then:

    docker logs notification-service
    curl http://localhost:8085/notifications/customer/CUS-1
    curl http://localhost:8085/notifications/driver/DRV-1
    curl -X PUT http://localhost:8085/notifications/1/read

Sending the same event twice should log "Duplicate event ignored" and not create a second notification.

## Notes for the team

- The delivery service must publish `delivery.assigned` and `delivery.completed` with at least `orderId` and `driverId`.
- To avoid double messages, `orders.status.updated` with `DELIVERED` and `delivery.completed` produce one customer notification between them.
