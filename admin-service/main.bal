```ballerina
import ballerina/http;
import ballerina/lang.value;
import ballerina/log;
import ballerina/sql;
import ballerinax/kafka;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;

configurable string dbHost = "localhost";
configurable int dbPort = 3306;
configurable string dbUser = "root";
configurable string dbPassword = "rootpass";
configurable string dbName = "admin_db";
configurable string kafkaUrl = "localhost:9092";

type OrderCreated record {
    string orderId;
    string restaurantId;
    decimal totalAmount;
    string timestamp;
};

type DeliveryEvent record {
    string orderId;
    string driverId;
    string timestamp;
};

type RestaurantStat record {|
    string restaurantId;
    int orderCount;
    decimal revenue;
|};

type DriverPerformance record {|
    string driverId;
    int deliveries;
    decimal avgMinutes;
|};

final mysql:Client db = check new (dbHost, dbUser, dbPassword, dbName, dbPort);

function init() returns error? {
    _ = check db->execute(`
        CREATE TABLE IF NOT EXISTS order_events (
            order_id      VARCHAR(50) PRIMARY KEY,
            restaurant_id VARCHAR(50) NOT NULL,
            total_amount  DECIMAL(10,2) NOT NULL,
            created_at    DATETIME NOT NULL
        )`);
    _ = check db->execute(`
        CREATE TABLE IF NOT EXISTS delivery_events (
            order_id     VARCHAR(50) PRIMARY KEY,
            driver_id    VARCHAR(50) NOT NULL,
            assigned_at  DATETIME NULL,
            completed_at DATETIME NULL
        )`);
}

listener kafka:Listener orderListener = new (kafkaUrl, {
    groupId: "admin-service",
    topics: ["orders.created"]
});

listener kafka:Listener assignedListener = new (kafkaUrl, {
    groupId: "admin-service",
    topics: ["delivery.assigned"]
});

listener kafka:Listener completedListener = new (kafkaUrl, {
    groupId: "admin-service",
    topics: ["delivery.completed"]
});

service on orderListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) {
        foreach kafka:BytesConsumerRecord rec in records {
            error? result = storeOrder(rec.value);
            if result is error {
                log:printError("Bad order
```
