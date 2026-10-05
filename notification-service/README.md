
# Notification Service

This is the notification service for the DSA612S Distributed Food Delivery Platform.

The service is built with Ballerina and uses MySQL for storing notifications. It also listens to events from Kafka. When an event is received, it creates the notifications needed for the customer, restaurant, or driver.

The REST API runs on port 8085.

## Kafka topics

The service listens to these topics:

| Topic | Used for |
|---|---|
| `orders.created` | Creates notifications for the customer and restaurant |
| `orders.status.updated` | Sends updates to the customer and, for some statuses, the restaurant and driver |
| `payments.completed` | Notifies the customer and restaurant about a completed payment |
| `payments.failed` | Notifies the customer when a payment fails |
| `delivery.assigned` | Notifies the driver, customer and restaurant |
| `delivery.completed` | Notifies the driver, customer and restaurant |

The service only uses the fields it needs from each event. When an order is created, the customer, restaurant and driver information is kept in `order_parties` so it can be used by later events.

## Notification channels

The channels are currently simulated by writing them to the log.

| Recipient | Channels |
|---|---|
| Customer | PUSH, EMAIL |
| Restaurant | DASHBOARD, SMS |
| Driver | PUSH, SMS |

## REST API

The service runs on port 8085.

| Method | Endpoint | Description |
|---|---|---|
| GET | `/notifications/health` | Checks if the service is running |
| GET | `/notifications?recipientType=&recipientId=&status=&maxResults=` | Gets notifications using optional filters |
| GET | `/notifications/customer/{customerId}?status=UNREAD` | Gets notifications for a customer |
| GET | `/notifications/restaurant/{restaurantId}?status=UNREAD` | Gets notifications for a restaurant |
| GET | `/notifications/driver/{driverId}?status=UNREAD` | Gets notifications for a driver |
| GET | `/notifications/orders/{orderId}` | Gets notifications for an order |
| GET | `/notifications/{id}` | Gets one notification |
| PUT | `/notifications/{id}/read` | Marks a notification as read |
| POST | `/notifications` | Creates a notification manually |

The available notification statuses are `UNREAD` and `READ`.

Example POST request:

```json
{
  "recipientType": "CUSTOMER",
  "recipientId": "CUS-1",
  "title": "Hi",
  "message": "Hello"
}
```

`orderId` can also be included if the notification is linked to an order.

## Database

The database is called `notification_db`.

There are two main tables:

- `notifications` stores the notifications.
- `order_parties` stores the customer, restaurant and driver connected to an order.

The tables are created when the service starts.

The `event_key` in the `notifications` table is unique. This stops the same Kafka event from creating duplicate notifications.

## Running with Docker

1. Add the two services from `docker-compose.snippet.yml` to the main `docker-compose.yml`.
2. From the root of the repository, run:

```bash
docker compose up --build kafka notification-db notification-service
```

3. Check that the service is running:

```bash
curl http://localhost:8085/notifications/health
```

## Running locally

Start Kafka and the notification database first:

```bash
docker compose up kafka notification-db
```

Create a `Config.toml` file inside `notification-service` with the database port:

```toml
dbPort = 3307
```

`Config.toml` is ignored by git.

Then run the service from the `notification-service` folder:

```bash
bal run
```

## Testing with Kafka events

Create the required Kafka topics:

```bash
for t in orders.created orders.status.updated payments.completed payments.failed delivery.assigned delivery.completed; do
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic $t --partitions 3 --replication-factor 1
done
```

Send some test events:

```bash
echo '{"orderId":"ORD-1","customerId":"CUS-1","restaurantId":"RES-1","itemId":"ITM-1","quantity":2,"totalAmount":45.50,"status":"CREATED"}' | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders.created

echo '{"paymentId":"pay-1","orderId":"ORD-1","customerId":"CUS-1","amount":45.50,"currency":"NAD","status":"COMPLETED"}' | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic payments.completed

echo '{"orderId":"ORD-1","previousStatus":"CREATED","newStatus":"CONFIRMED"}' | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders.status.updated

echo '{"orderId":"ORD-1","driverId":"DRV-1"}' | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic delivery.assigned

echo '{"orderId":"ORD-1","driverId":"DRV-1"}' | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic delivery.completed
```

Check the service logs:

```bash
docker logs notification-service
```

You can also check the API:

```bash
curl http://localhost:8085/notifications/customer/CUS-1
curl http://localhost:8085/notifications/driver/DRV-1
curl -X PUT http://localhost:8085/notifications/1/read
```

If the same event is sent more than once, the duplicate should be ignored and the log should show:

```text
Duplicate event ignored
```

## Notes

The delivery service needs to send `delivery.assigned` and `delivery.completed` events with at least `orderId` and `driverId`.

For `orders.status.updated`, the `DELIVERED` status and `delivery.completed` can result in the same customer notification. The service uses the event key to prevent a duplicate notification from being created.
