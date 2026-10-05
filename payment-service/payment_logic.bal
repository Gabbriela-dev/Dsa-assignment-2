import ballerina/log;
import ballerina/uuid;
import ballerina/time;

function processPayment(OrderCreatedEvent order) returns string {
    string pid = "pay-" + uuid:createRandomUuid().substring(0, 8);
    decimal amount = order.totalAmount;
    string currency = "NAD";
    string paymentMethod = "CARD";

    checkpanic savePayment(
        pid, order.orderId, order.customerId,
        amount, currency, paymentMethod
    );

    boolean ok = amount < 5000.00;
    string now = time:utcToString(time:utcNow());

    if ok {
        checkpanic markStatus(pid, "COMPLETED", "ok");

        PaymentCompletedEvent doneEvt = {
            paymentId: pid,
            orderId: order.orderId,
            customerId: order.customerId,
            amount: amount,
            currency: currency,
            status: "COMPLETED",
            method: paymentMethod,
            processedAt: now
        };
        checkpanic publishEvent(SUCCESS_TOPIC, doneEvt);
        log:printInfo("paid order " + order.orderId);
    } else {
        checkpanic markStatus(pid, "FAILED", "amount too high");

        PaymentFailedEvent failEvt = {
            paymentId: pid,
            orderId: order.orderId,
            customerId: order.customerId,
            amount: amount,
            currency: currency,
            status: "FAILED",
            reason: "amount too high (simulated)",
            processedAt: now
        };
        checkpanic publishEvent(FAIL_TOPIC, failEvt);
        log:printError("payment failed for " + order.orderId);
    }

    return pid;
}
