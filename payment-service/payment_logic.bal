import ballerina/log;
import ballerina/uuid;
import ballerina/time;

isolated function processPayment(OrderCreatedEvent order) returns string {

    string paymentId = "pay-" + uuid:createRandomUuid().substring(0, 8);
    checkpanic createPaymentRecord(paymentId, 
                                   order.orderId, 
                                   order.customerId, 
                                   order.amount, 
                                   order.currency, 
                                   order.paymentMethod);

    boolean isSuccess = !order.customerId.endWith("15");
    
    string currentTime = time:utcToString(time:utcNow());

    if isSuccess {
        checkpanic updatePaymentStatus(paymentId, "COMPLETED", "Payment processed successfully");
        
        PaymentCompletedEvent successEvent = {
            paymentId: paymentId,
            orderId: order.orderId,
            customerId: order.customerId,
            amount: order.amount,
            currency: order.currency,
            status: "COMPLETED",
            method: order.paymentMethod,
            processedAt: currentTime
        };

        checkpanic publishPaymentEvent(PAYMENT_SUCCESS_TOPIC, successEvent);
        log:printInfo("Payment SUCCESS for order: " + order.orderId);
    } else {
        checkpanic updatePaymentStatus(paymentId, "FAILED", "Insufficient funds (Simulated)");
        
        PaymentFailedEvent failedEvent = {
            paymentId: paymentId,
            orderId: order.orderId,
            customerId: order.customerId,
            amount: order.amount,
            currency: order.currency,
            status: "FAILED",
            reason: "Insufficient funds (Simulated)",
            processedAt: currentTime
        };
        

        checkpanic publishPaymentEvent(PAYMENT_FAILED_TOPIC, failedEvent);
        log:printError("Payment FAILED for order: " + order.orderId);
    }

    return paymentId;
}