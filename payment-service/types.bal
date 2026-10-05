// incoming from order service
public type OrderCreatedEvent record {
    string orderId;
    string customerId;
    string restaurantId;
    decimal amount;
    string currency;
    string paymentMethod;
};

// what we send when payment works
public type PaymentCompletedEvent record {
    string paymentId;
    string orderId;
    string customerId;
    decimal amount;
    string currency;
    string status;
    string method;
    string processedAt;
};

// what we send when it fails
public type PaymentFailedEvent record {
    string paymentId;
    string orderId;
    string customerId;
    decimal amount;
    string currency;
    string status;
    string reason;
    string processedAt;
};

// matches our sql table
public type PaymentRecord record {
    string payment_id;
    string order_id;
    string customer_id;
    decimal amount;
    string currency;
    string method;
    string status;
    string? reason;
    string created_at;
    string updated_at;
};