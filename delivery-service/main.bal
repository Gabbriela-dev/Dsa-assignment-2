import ballerina/http;
import ballerina/log;
import ballerina/sql;
import ballerina/time;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;

configurable string dbHost = "localhost";
configurable int dbPort = 3306;
configurable string dbUser = "delivery";
configurable string dbPassword = "delivery";
configurable string dbName = "delivery_db";

final mysql:Client db = check new (
    host = dbHost,
    port = dbPort,
    user = dbUser,
    password = dbPassword,
    database = dbName
);

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
}

type DriverRow record {|
    string driver_id;
    string? name;
    string? phone;
    string? vehicle_type;
    string? status;
|};