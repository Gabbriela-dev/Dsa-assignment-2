// db stuff
final string DB_HOST = "localhost";
final int DB_PORT = 3306;
final string DB_USER = "payment_user";
final string DB_PASS = "payment_pass";
final string DB_NAME = "payments_db";

// kafka topics
final string KAFKA_BROKER = "localhost:9092";
final string ORDER_TOPIC = "orders.created";
final string SUCCESS_TOPIC = "payments.completed";
final string FAIL_TOPIC = "payments.failed";

// our port
final int PORT = 8080;