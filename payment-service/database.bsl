import ballerinax/postgresql;
import ballerina/log;

final postgresql:Client dbClient = check new (
    host = DB_HOST,
    port = DB_PORT,
    username = DB_USER,
    password = DB_PASS,
    database = DB_NAME
);


isolated function createPaymentRecord(
          string paymentId, 
          string orderId, 
          string customerId, 
          decimal amount, 
          string currency, 
          string method) 
          returns error? {
            
    _ = check dbClient->execute(`
        INSERT INTO payments (payment_id, order_id, customer_id, amount, currency, method, status)
        VALUES (${paymentId}, ${orderId}, ${customerId}, ${amount}, ${currency}, ${method}, 'PENDING')
    `);

    log:printInfo("Payment record created in DB: " + paymentId);
}

isolated function updatePaymentStatus(string paymentId, string status, string reason) returns error? {
    _ = check dbClient->execute(`
        UPDATE payments 
        SET status = ${status}, reason = ${reason}, updated_at = CURRENT_TIMESTAMP 
        WHERE payment_id = ${paymentId}
    `);
    log:printInfo("Payment status updated to " + status + " for: " + paymentId);
}

isolated function getPaymentByOrderId(string orderId) returns PaymentRecord|error? {
    return dbClient->queryRow(`
        SELECT * FROM payments WHERE order_id = ${orderId}
    `);
}

isolated function getAllPayments() returns PaymentRecord[]|error {
    return dbClient->query(`
        SELECT * FROM payments ORDER BY created_at DESC
    `);
}