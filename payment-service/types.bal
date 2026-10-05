
public type newOrder record {
    string orderId;
    string customerId;
    string restaurantId;
    decimal amount;
    string currency;
    string paymentMethod;}

public type paymentCompletedEvent record {
    string paymentId;
    string orderId;
    string customerId;
    decimal amount;
    string currency;
    string status;
    string method;
    string processedAt;
};

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

public type PaymentRecord record {
    string payment_id;
    string order_id;
    string customer_id;
    decimal amount;
    string currency;
    string method;
    string status;
    string reason;
    string created_at;
    string updated_at;
};