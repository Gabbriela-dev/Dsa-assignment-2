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

function publishEvent(string topic, json payload) returns error? {
    _ = check producer->send({
        topic: topic,
        value: payload.toJsonString().toBytes()
    });
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