import ballerinax/kafka;
import ballerina/log;

final kafka:ProducerConfiguration producerConfig = {
    clientId: "payment-service-producer",
    acks: "all" 
};

final kafka:Producer paymentProducer = check new (KAFKA_BROKER, producerConfig);

isolated function publishPaymentEvent(string topic, anydata eventPayload) returns error? {
    _ = check paymentProducer->send({
        topic: topic,
        value: eventPayload
    });
    log:printInfo("Event published to Kafka topic: " + topic);
}

final kafka:ConsumerConfiguration consumerConfig = {
    groupId: "payment-service-group",
    topics: [ORDER_TOPIC],
    offsetReset: "earliest"
};


listener kafka:Listener orderListener = new (KAFKA_BROKER, consumerConfig);

service on orderListener {
    remote function onConsumerRecord(OrderCreatedEvent[] orders) returns error? {
        foreach var order in orders {
            log:printInfo("Received new order event: " + order.orderId);

            PaymentRecord|error? existingPayment = getPaymentByOrderId(order.orderId);
            
            if existingPayment is PaymentRecord {
                log:printWarn("Payment already exists for order: " + order.orderId + ". Skipping.");
                continue;
            }

            _ = processPayment(order);
        }
    }
}