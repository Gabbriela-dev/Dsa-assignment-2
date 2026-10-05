import ballerina/http;
import ballerina/log;

service / on new http:Listener(HTTP_PORT) {

    resource function get health() returns string {
        return "Payment Service is running!";
    }

    resource function get payments() returns PaymentRecord[]|http:InternalServerError {
        PaymentRecord[]|error result = getAllPayments();
        if result is error {
            log:printError("Failed to fetch payments", result);
            return http:INTERNAL_SERVER_ERROR;
        }
        return result;
    }

    resource function get payments/order/[string orderId]() returns PaymentRecord|http:NotFound|http:InternalServerError {
        PaymentRecord|error? result = getPaymentByOrderId(orderId);
        if result is error {
            log:printError("Database error", result);
            return http:INTERNAL_SERVER_ERROR;
        }
        if result is () {
            return http:NOT_FOUND;
        }
        return result;
    }
}