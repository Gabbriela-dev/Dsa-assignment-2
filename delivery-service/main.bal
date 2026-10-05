import ballerina/http;
import ballerina/log;
import ballerina/sql;
import ballerina/time;
import ballerinax/kafka;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;

configurable string dbHost = "localhost";
configurable int dbPort = 3306;
configurable string dbUser = "delivery";
configurable string dbPassword = "delivery";
configurable string dbName = "delivery_db";
configurable string kafkaBroker = "localhost:9092";

final mysql:Client db = check new (
    host = dbHost,
    port = dbPort,
    user = dbUser,
    password = dbPassword,
    database = dbName
);

final kafka:Producer producer = check new (kafkaBroker, {
    clientId: "delivery-service"
});

listener kafka:Listener orderCreatedListener = new (kafkaBroker, {
    groupId: "delivery-service-group",
    topics: ["orders.created"]
});

service / on new http:Listener(8085) {

    resource function get health() returns json {
        log:printInfo("Health check");
        return {
            status: "UP",
            serviceName: "delivery-service",
            timestamp: time:utcNow().toString()
        };
    }

    resource function get drivers() returns json|error {
        stream<DriverRow, sql:Error?> rows = db->query(
            `SELECT driver_id, name, phone, vehicle_type, status
             FROM drivers`
        );
        DriverRow[] result = [];
        check from var row in rows do { result.push(row); };
        return result.toJson();
    }

    resource function get deliveries/[string deliveryId]() returns json|http:NotFound {
        DeliveryRow|sql:Error row = db->queryRow(
            `SELECT delivery_id, order_id, customer_id, restaurant_id,
                    driver_id, status, failure_reason
             FROM deliveries WHERE delivery_id = ${deliveryId}`
        );
        if row is sql:Error { return http:NOT_FOUND; }
        return row.toJson();
    }
}

service on orderCreatedListener {
    remote function onConsumerRecord(kafka:Caller caller,
                                     kafka:BytesConsumerRecord[] records) returns error? {
        foreach var rec in records {
            string valueStr = check string:fromBytes(rec.value);
            json payload = check valueStr.fromJsonString();

            string eventId = check payload.eventId.ensureType(string);
            boolean alreadyDone = check isEventProcessed(eventId);
            if alreadyDone { continue; }

            string orderId = check payload.orderId.ensureType(string);
            string customerId = check payload.customerId.ensureType(string);
            string restaurantId = check payload.restaurantId.ensureType(string);
            string deliveryId = "DEL-" + orderId;

            _ = check db->execute(
                `INSERT INTO deliveries
                    (delivery_id, order_id, customer_id, restaurant_id, status)
                 VALUES (${deliveryId}, ${orderId}, ${customerId},
                         ${restaurantId}, 'PENDING')
                 ON DUPLICATE KEY UPDATE delivery_id = delivery_id`
            );

            _ = check recordProcessedEvent(eventId, "orders.created");
            log:printInfo("Delivery created: " + deliveryId);
        }
        _ = check caller->commit();
    }
}

function publishEvent(string topic, json payload) returns error? {
    _ = check producer->send({
        topic: topic,
        value: payload.toJsonString().toBytes()
    });
}

function isEventProcessed(string eventId) returns boolean|error {
    record {| int c; |}|sql:Error result = db->queryRow(
        `SELECT COUNT(*) AS c FROM processed_events WHERE event_id = ${eventId}`
    );
    if result is sql:Error {
        return result;
    }
    return result.c > 0;
}

function recordProcessedEvent(string eventId, string topic) returns error? {
    _ = check db->execute(
        `INSERT INTO processed_events (event_id, topic)
         VALUES (${eventId}, ${topic})
         ON DUPLICATE KEY UPDATE processed_at = NOW()`
    );
}

type DriverRow record {|
    string driver_id;
    string? name;
    string? phone;
    string? vehicle_type;
    string? status;
|};

type DeliveryRow record {|
    string delivery_id;
    string order_id;
    string customer_id;
    string restaurant_id;
    string? driver_id;
    string status;
    string? failure_reason;
|};