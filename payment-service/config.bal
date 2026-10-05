configurable string DB_HOST = "localhost";
configurable int DB_PORT = 3306;
configurable string DB_USER = "payment_user";
configurable string DB_PASS = "payment_pass";
configurable string DB_NAME = "payments_db";

configurable string KAFKA_BROKER = "localhost:9092";
final string ORDER_TOPIC = "orders.created";
final string SUCCESS_TOPIC = "payments.completed";
final string FAIL_TOPIC = "payments.failed";

final int PORT = 8080;
