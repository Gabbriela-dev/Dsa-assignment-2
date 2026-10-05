import ballerina/http;
import ballerina/lang.value;
import ballerina/log;
import ballerina/sql;
import ballerinax/kafka;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;

configurable string dbHost = "localhost";
configurable int dbPort = 3306;
configurable string dbUser = "admin";
configurable string dbPassword = "admin";
configurable string dbName = "admin_db";
configurable string kafkaUrl = "localhost:9092";

type OrderCreated record {
    string orderId;
    string restaurantId;
    decimal totalAmount;
    string timestamp;
};

type DeliveryEvent record {
    string? orderId;
    string? deliveryId;
    string? driverId;
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

public function main() returns error? {
    check init();
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
                log:printError("Bad orders.created message, skipped", 'error = result);
            }
        }
    }
}

service on assignedListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) {
        foreach kafka:BytesConsumerRecord rec in records {
            error? result = storeAssigned(rec.value);
            if result is error {
                log:printError("Bad delivery.assigned message, skipped", 'error = result);
            }
        }
    }
}

service on completedListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) {
        foreach kafka:BytesConsumerRecord rec in records {
            error? result = storeCompleted(rec.value);
            if result is error {
                log:printError("Bad delivery.completed message, skipped", 'error = result);
            }
        }
    }
}

function storeOrder(byte[] payload) returns error? {
    json j = check value:fromJsonString(check string:fromBytes(payload));
    OrderCreated o = check j.cloneWithType();
    _ = check db->execute(`
        INSERT INTO order_events (order_id, restaurant_id, total_amount, created_at)
        VALUES (${o.orderId}, ${o.restaurantId}, ${o.totalAmount}, ${o.timestamp})
        ON DUPLICATE KEY UPDATE order_id = order_id`);
    log:printInfo("Stored order " + o.orderId);
}

function storeAssigned(byte[] payload) returns error? {
    json j = check value:fromJsonString(check string:fromBytes(payload));
    DeliveryEvent d = check j.cloneWithType();
    string orderId = d.orderId ?: deriveOrderId(d.deliveryId ?: "");
    _ = check db->execute(`
        INSERT INTO delivery_events (order_id, driver_id, assigned_at)
        VALUES (${orderId}, ${d.driverId ?: ""}, ${d.timestamp}) AS new
        ON DUPLICATE KEY UPDATE driver_id = new.driver_id, assigned_at = new.assigned_at`);
    log:printInfo("Stored assignment for " + orderId);
}

function storeCompleted(byte[] payload) returns error? {
    json j = check value:fromJsonString(check string:fromBytes(payload));
    DeliveryEvent d = check j.cloneWithType();
    string orderId = d.orderId ?: deriveOrderId(d.deliveryId ?: "");
    string orderId = d.orderId ?: deriveOrderId(d.deliveryId ?: "");
    _ = check db->execute(`
        INSERT INTO delivery_events (order_id, driver_id, completed_at)
        VALUES (${orderId}, ${d.driverId ?: ""}, ${d.timestamp}) AS new
        ON DUPLICATE KEY UPDATE completed_at = new.completed_at`);
    log:printInfo("Stored completion for " + orderId);
}

function deriveOrderId(string deliveryId) returns string {
    if deliveryId.startsWith("DEL-") {
        return deliveryId.substring(4);
    }
    return deliveryId;
}

service /admin on new http:Listener(9096) {
    resource function get restaurants/stats() returns RestaurantStat[]|error {
        stream<RestaurantStat, sql:Error?> rows = db->query(`
            SELECT restaurant_id AS restaurantId,
                   COUNT(*) AS orderCount,
                   SUM(total_amount) AS revenue
            FROM order_events
            GROUP BY restaurant_id`);
        return from RestaurantStat r in rows select r;
    }

    resource function get deliveries/performance() returns DriverPerformance[]|error {
        stream<DriverPerformance, sql:Error?> rows = db->query(`
            SELECT driver_id AS driverId,
                   COUNT(*) AS deliveries,
                   AVG(TIMESTAMPDIFF(MINUTE, assigned_at, completed_at)) AS avgMinutes
            FROM delivery_events
            WHERE assigned_at IS NOT NULL AND completed_at IS NOT NULL
            GROUP BY driver_id`);
        return from DriverPerformance p in rows select p;
    }
}
