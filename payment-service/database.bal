import ballerinax/mysql;
import ballerinax/mysql.driver as _;
import ballerina/sql;
import ballerina/log;

final mysql:Client db = check new (
    host = dbHost,
    port = dbPort,
    user = dbUser,
    password = dbPassword,
    database = dbName
);

function savePayment(string pid, string oid, string cid, decimal amt, string cur, string method) returns error? {
    _ = check db->execute(`
        INSERT INTO payments (payment_id, order_id, customer_id, amount, currency, method, status)
        VALUES (${pid}, ${oid}, ${cid}, ${amt}, ${cur}, ${method}, 'PENDING')
    `);
    log:printInfo("saved payment " + pid);
}

function markStatus(string pid, string status, string reason) returns error? {
    _ = check db->execute(`
        UPDATE payments
        SET status = ${status}, reason = ${reason}, updated_at = CURRENT_TIMESTAMP
        WHERE payment_id = ${pid}
    `);
    log:printInfo("payment " + pid + " now " + status);
}

function findPaymentByOrder(string oid) returns PaymentRecord|error? {
    return db->queryRow(`SELECT * FROM payments WHERE order_id = ${oid}`);
}

function listAllPayments() returns PaymentRecord[]|error {
    stream<PaymentRecord, sql:Error?> rs = db->query(`SELECT * FROM payments ORDER BY created_at DESC`);
    return from PaymentRecord p in rs select p;
}
