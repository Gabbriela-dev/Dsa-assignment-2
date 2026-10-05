# Delivery Service

Driver assignment and delivery tracking for the DSA612S Food Delivery Platform.

- **Port:** 8085
- **Database:** MySQL 8.0 (`delivery_db`)
- **Ballerina:** 2201.13.x

## Kafka Topics

| Direction | Topic |
|-----------|-------|
| Consume   | `orders.created` |
| Consume   | `orders.ready` |
| Produce   | `delivery.assigned` |
| Produce   | `delivery.completed` |
| Produce   | `delivery.failed` |

## REST Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| GET    | `/health` | Health check |
| GET    | `/drivers` | List drivers |
| GET    | `/deliveries/{deliveryId}` | Get delivery |

## Environment Variables

| Variable | Default |
|----------|---------|
| `dbHost` | `localhost` |
| `dbPort` | `3306` |
| `dbUser` | `delivery` |
| `dbPassword` | `delivery` |
| `dbName` | `delivery_db` |