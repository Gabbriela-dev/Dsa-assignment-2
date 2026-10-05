import ballerinax/kafka;
import ballerina/log;

final kafka:Producer producer = check new (kafkaBroker);

function publishEvent(string topic, anydata payload) returns error? {
    _ = check producer->send({topic: topic, value: payload});
    log:printInfo("published to " + topic);
}

final kafka:ConsumerConfiguration consumerCfg = {
    groupId: "payment-service-group",
    topics: [ORDER_TOPIC],
    offsetReset: kafka:EARLIEST
};

listener kafka:Listener orderListener = new (kafkaBroker, consumerCfg);

service on orderListener {
    remote function onConsumerRecord(kafka:Caller caller, OrderCreatedEvent[] orders) returns error? {
        foreach var order in orders {
            log:printInfo("got order " + order.orderId);

            PaymentRecord|error? existing = findPaymentByOrder(order.orderId);
            if existing is PaymentRecord {
                log:printWarn("order " + order.orderId + " already paid, skipping");
                continue;
            }

            _ = processPayment(order);
        }
    }
}
