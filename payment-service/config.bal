configurable string dbHost = "localhost";
configurable int dbPort = 3306;
configurable string dbUser = "payment_user";
configurable string dbPassword = "payment_pass";
configurable string dbName = "payments_db";

configurable string kafkaBroker = "localhost:9092";
final string ORDER_TOPIC = "orders.created";
final string SUCCESS_TOPIC = "payments.completed";
final string FAIL_TOPIC = "payments.failed";

final int PORT = 8080;
