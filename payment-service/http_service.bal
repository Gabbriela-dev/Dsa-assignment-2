import ballerina/http;

service / on new http:Listener(PORT) {

    // simple health check for docker
    resource function get health() returns string {
        return "payment service ok";
    }

    // list all payments
    resource function get payments() returns PaymentRecord[]|http:InternalServerError {
        PaymentRecord[]|error result = listAllPayments();
        if result is error {
            return http:INTERNAL_SERVER_ERROR;
        }
        return result;
    }

    // look up one payment by order id
    resource function get payments/order/[string orderId]() returns PaymentRecord|http:NotFound|http:InternalServerError {
        PaymentRecord|error? result = findPaymentByOrder(orderId);
        if result is error {
            return http:INTERNAL_SERVER_ERROR;
        }
        if result is () {
            return http:NOT_FOUND;
        }
        return result;
    }
}