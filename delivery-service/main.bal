import ballerina/http;
import ballerina/log;
import ballerina/sql;
import ballerina/time;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;

// ============================================================
// Configuration
// ============================================================
configurable string dbHost = "localhost";
configurable int dbPort = 3306;
configurable string dbUser = "delivery";
configurable string dbPassword = "delivery";
configurable string dbName = "delivery_db";

// ============================================================
// MySQL Connection Pool
// ============================================================
final mysql:Client db = check new (
    host = dbHost,
    port = dbPort,
    user = dbUser,
    password = dbPassword,
    database = dbName,
    connectionPool = {
        maxOpenConnections: 10,
        maxConnectionLifeTime: 1800
    }
);

// ============================================================
// HTTP Service
// ============================================================
service / on new http:Listener(8085) {

    // -------- Health check --------
            resource function get health() returns json {
        log:printInfo("Health check called");
        return {
            status: "UP",
            serviceName: "delivery-service",
            timestamp: time:utcNow().toString()
        };
    }

    // -------- List drivers --------
        resource function get drivers() returns json|error { 
        stream<DriverRow, sql:Error?> rows = db->query(
            `SELECT driver_id, name, phone, vehicle_type, status
             FROM drivers`
        );
        DriverRow[] result = [];
        check from DriverRow row in rows
            do { result.push(row); };
        return result.toJson();
    }

    // -------- Get delivery by ID --------
    resource function get deliveries/[string deliveryId]() returns json|http:NotFound {
        DeliveryRow|sql:Error row = db->queryRow(
            `SELECT delivery_id, order_id, driver_id, status
             FROM deliveries WHERE delivery_id = ${deliveryId}`
        );
        if row is sql:Error {
            return http:NOT_FOUND;
        }
        return row.toJson();
    }
}

// ============================================================
// Types
// ============================================================
type DriverRow record {|
    string driver_id;
    string name;
    string phone;
    string vehicle_type;
    string status;
|};

type DeliveryRow record {|
    string delivery_id;
    string order_id;
    string? driver_id;
    string status;
|};