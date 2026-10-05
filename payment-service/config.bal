
// PostgreSQL Settings
final string DB_HOST = "localhost";
final int DB_PORT = 5432;
final string DB_USER = "payment_user";
final string DB_PASS = "payment_pass";
final string DB_NAME = "payments_db";

// Kafka Settings
final string KAFKA_BROKER = "localhost:9092";
final string ORDER_TOPIC = "orders.created";
final string PAYMENT_SUCCESS_TOPIC = "payments.completed";
final string PAYMENT_FAILED_TOPIC = "payments.failed";

// HTTP Service Settings
final int HTTP_PORT = 8080;