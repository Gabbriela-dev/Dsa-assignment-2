import ballerina/http;
import ballerina/log;

service / on new http:Listener(PORT) {

    resource function get health() returns string {
        return "payment service ok";
    }

    resource function get payments() returns PaymentRecord[]|http:InternalServerError {
        PaymentRecord[]|error result = listAllPayments();
        if result is error {
            log:printError("failed to fetch payments", result);
            return http:INTERNAL_SERVER_ERROR;
        }
        return result;
    }

    resource function get payments/order/[string orderId]() returns PaymentRecord|http:NotFound|http:InternalServerError {
        PaymentRecord|error? result = findPaymentByOrder(orderId);
        if result is error {
            log:printError("database error", result);
            return http:INTERNAL_SERVER_ERROR;
        }
        if result is () {
            return http:NOT_FOUND;
        }
        return result;
    }
}