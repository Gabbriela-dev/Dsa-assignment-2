// Notification Service
// DSA612S Assignment 2 - Distributed Food Delivery Platform
//
// Listens to Kafka events from the other services, works out who must be told
// (customer, restaurant or driver), saves each notification in MySQL and
// "sends" it on the right channels. A small REST API lets clients read them.

import ballerina/http;
import ballerina/lang.runtime;
import ballerina/lang.value;
import ballerina/log;
import ballerina/sql;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;

configurable string dbHost = "localhost";
configurable int dbPort = 3306;
configurable string dbUser = "root";
configurable string dbPassword = "rootpass";
configurable string dbName = "notification_db";
configurable string kafkaUrl = "localhost:9092";
configurable int httpPort = 8085;

const string TOPIC_ORDER_CREATED = "orders.created";
const string TOPIC_ORDER_STATUS = "orders.status.updated";
const string TOPIC_PAYMENT_COMPLETED = "payments.completed";
const string TOPIC_PAYMENT_FAILED = "payments.failed";
const string TOPIC_DELIVERY_ASSIGNED = "delivery.assigned";
const string TOPIC_DELIVERY_COMPLETED = "delivery.completed";

const int PARTY_LOOKUP_ATTEMPTS = 5;

// Who a notification is for.
type RecipientType "CUSTOMER"|"RESTAURANT"|"DRIVER";

// A notification as it is stored and returned by the REST API.
type Notification record {|
    int id;
    string recipientType;
    string recipientId;
    string? orderId;
    string title;
    string message;
    string channels;
    string eventType;
    string status;
    string createdAt;
    string? readAt;
|};

// Body of POST /notifications (a manual notification, e.g. an admin announcement).
type NewNotification record {|
    RecipientType recipientType;
    string recipientId;
    string orderId?;
    string title;
    string message;
|};

// Who is involved in an order. Saved from orders.created and delivery.assigned
// because the status and delivery events do not carry customer or restaurant ids.
type OrderParties record {|
    string orderId;
    string customerId;
    string restaurantId;
    string? driverId;
|};

// Event payloads. These records are open, so extra fields sent by the other
// services (itemId, quantity, timestamp ...) are simply ignored.
type OrderCreatedEvent record {
    string orderId;
    string customerId;
    string restaurantId;
    decimal totalAmount?;
};

type OrderStatusEvent record {
    string orderId;
    string newStatus;
};

type PaymentEvent record {
    string orderId;
    string customerId;
    decimal amount?;
    string currency?;
    string reason?;
};

type DeliveryEvent record {
    string orderId;
    string driverId;
};

final mysql:Client db = check new (dbHost, dbUser, dbPassword, dbName, dbPort);

function init() returns error? {
    _ = check db->execute(`
        CREATE TABLE IF NOT EXISTS order_parties (
            order_id      VARCHAR(50) PRIMARY KEY,
            customer_id   VARCHAR(50) NOT NULL,
            restaurant_id VARCHAR(50) NOT NULL,
            driver_id     VARCHAR(50) NULL,
            created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        )`);
    _ = check db->execute(`
        CREATE TABLE IF NOT EXISTS notifications (
            id             INT AUTO_INCREMENT PRIMARY KEY,
            event_key      VARCHAR(200) NOT NULL,
            recipient_type VARCHAR(20)  NOT NULL,
            recipient_id   VARCHAR(50)  NOT NULL,
            order_id       VARCHAR(50)  NULL,
            title          VARCHAR(100) NOT NULL,
            message        VARCHAR(500) NOT NULL,
            channels       VARCHAR(100) NOT NULL,
            event_type     VARCHAR(50)  NOT NULL,
            status         VARCHAR(10)  NOT NULL DEFAULT 'UNREAD',
            created_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
            read_at        DATETIME     NULL,
            UNIQUE KEY uq_event_key (event_key),
            INDEX idx_recipient (recipient_type, recipient_id, status),
            INDEX idx_order (order_id)
        )`);
    log:printInfo("Notification Service database is ready");
}

// Which channels each kind of recipient is notified on.
function channelsFor(RecipientType recipientType) returns string[] {
    if recipientType == "CUSTOMER" {
        return ["PUSH", "EMAIL"];
    }
    if recipientType == "RESTAURANT" {
        return ["DASHBOARD", "SMS"];
    }
    return ["PUSH", "SMS"];
}

// The assignment only asks us to simulate delivery, so each channel just writes
// a log line. A real system would call an email, SMS or push provider here.
function dispatchToChannels(string[] channelList, RecipientType recipientType,
        string recipientId, string title, string message) {
    foreach string channel in channelList {
        log:printInfo(string `[${channel}] to ${recipientType} ${recipientId}: ${title} - ${message}`);
    }
}

// Saves one notification and "sends" it. The event key makes this safe to repeat:
// if the same event arrives twice (Kafka can redeliver), the second insert is ignored.
function createNotification(string logicalEvent, string eventType, string? orderId,
        RecipientType recipientType, string recipientId, string title, string message)
        returns error? {
    string eventKey = string `${logicalEvent}:${orderId ?: "-"}:${recipientType}`;
    string[] channelList = channelsFor(recipientType);
    string channels = string:'join(",", ...channelList);

    sql:ExecutionResult result = check db->execute(`
        INSERT IGNORE INTO notifications
            (event_key, recipient_type, recipient_id, order_id, title, message,
             channels, event_type)
        VALUES
            (${eventKey}, ${recipientType}, ${recipientId}, ${orderId}, ${title},
             ${message}, ${channels}, ${eventType})`);

    int inserted = result.affectedRowCount ?: 0;
    if inserted == 0 {
        log:printInfo("Duplicate event ignored: " + eventKey);
        return;
    }
    dispatchToChannels(channelList, recipientType, recipientId, title, message);
}

// Turns the bytes of a Kafka message into JSON.
function toJson(byte[] payload) returns json|error {
    string text = check string:fromBytes(payload);
    return value:fromJsonString(text);
}

// Looks up the customer, restaurant and driver of an order (empty if unknown).
function findParties(string orderId) returns OrderParties|error? {
    OrderParties|error row = db->queryRow(`
        SELECT order_id AS orderId, customer_id AS customerId,
               restaurant_id AS restaurantId, driver_id AS driverId
        FROM order_parties
        WHERE order_id = ${orderId}`, OrderParties);
    if row is sql:NoRowsError {
        return ();
    }
    return row;
}

// Kafka does not guarantee ordering between different topics, so an
// orders.status.updated message can occasionally arrive just before the matching
// orders.created message has been saved. Retry for a few seconds before giving up.
function waitForParties(string orderId) returns OrderParties|error? {
    foreach int attempt in 1 ... PARTY_LOOKUP_ATTEMPTS {
        OrderParties|error? parties = findParties(orderId);
        if parties is OrderParties|error {
            return parties;
        }
        if attempt < PARTY_LOOKUP_ATTEMPTS {
            runtime:sleep(1);
        }
    }
    return ();
}

function handleOrderCreated(byte[] payload) returns error? {
    json j = check toJson(payload);
    OrderCreatedEvent evt = check j.cloneWithType();
    string orderId = evt.orderId;

    // Remember who is involved so later events (status, delivery) can find them.
    _ = check db->execute(`
        INSERT IGNORE INTO order_parties (order_id, customer_id, restaurant_id)
        VALUES (${orderId}, ${evt.customerId}, ${evt.restaurantId})`);

    decimal? total = evt?.totalAmount;
    string totalText = total is decimal ? string ` Total: ${total}.` : "";

    check createNotification("ORDER_CREATED", TOPIC_ORDER_CREATED, orderId, "CUSTOMER",
        evt.customerId, "Order placed",
        string `We received your order ${orderId}.${totalText}`);
    check createNotification("ORDER_CREATED", TOPIC_ORDER_CREATED, orderId, "RESTAURANT",
        evt.restaurantId, "New order",
        string `Order ${orderId} has just come in. Please confirm it.${totalText}`);
}

function handleOrderStatus(byte[] payload) returns error? {
    json j = check toJson(payload);
    OrderStatusEvent evt = check j.cloneWithType();
    string orderId = evt.orderId;

    OrderParties? parties = check waitForParties(orderId);
    if parties is () {
        log:printWarn("Unknown order " + orderId + ", status notification skipped");
        return;
    }
    string? driverId = parties.driverId;
    string logicalEvent = "ORDER_" + evt.newStatus;

    match evt.newStatus {
        "CONFIRMED" => {
            check createNotification(logicalEvent, TOPIC_ORDER_STATUS, orderId, "CUSTOMER",
                parties.customerId, "Order confirmed",
                string `The restaurant confirmed your order ${orderId}.`);
        }
        "PREPARING" => {
            check createNotification(logicalEvent, TOPIC_ORDER_STATUS, orderId, "CUSTOMER",
                parties.customerId, "Order being prepared",
                string `Your order ${orderId} is being prepared.`);
        }
        "READY" => {
            check createNotification(logicalEvent, TOPIC_ORDER_STATUS, orderId, "CUSTOMER",
                parties.customerId, "Order ready",
                string `Your order ${orderId} is ready and waiting for a driver.`);
            if driverId is string {
                check createNotification(logicalEvent, TOPIC_ORDER_STATUS, orderId, "DRIVER",
                    driverId, "Order ready for pickup",
                    string `Order ${orderId} is ready. Please collect it.`);
            }
        }
        "OUT_FOR_DELIVERY" => {
            check createNotification(logicalEvent, TOPIC_ORDER_STATUS, orderId, "CUSTOMER",
                parties.customerId, "Out for delivery",
                string `Your order ${orderId} is on its way.`);
        }
        "DELIVERED" => {
            check createNotification(logicalEvent, TOPIC_ORDER_STATUS, orderId, "CUSTOMER",
                parties.customerId, "Order delivered",
                string `Your order ${orderId} has been delivered. Enjoy your meal!`);
            check createNotification(logicalEvent, TOPIC_ORDER_STATUS, orderId, "RESTAURANT",
                parties.restaurantId, "Order delivered",
                string `Order ${orderId} was delivered to the customer.`);
        }
        "CANCELLED" => {
            check createNotification(logicalEvent, TOPIC_ORDER_STATUS, orderId, "CUSTOMER",
                parties.customerId, "Order cancelled",
                string `Your order ${orderId} was cancelled.`);
            check createNotification(logicalEvent, TOPIC_ORDER_STATUS, orderId, "RESTAURANT",
                parties.restaurantId, "Order cancelled",
                string `Order ${orderId} was cancelled.`);
            if driverId is string {
                check createNotification(logicalEvent, TOPIC_ORDER_STATUS, orderId, "DRIVER",
                    driverId, "Delivery cancelled",
                    string `Order ${orderId} was cancelled. No delivery is needed.`);
            }
        }
        _ => {
            log:printWarn("Unknown order status " + evt.newStatus + ", skipped");
        }
    }
}

function handlePaymentCompleted(byte[] payload) returns error? {
    json j = check toJson(payload);
    PaymentEvent evt = check j.cloneWithType();
    string orderId = evt.orderId;

    check createNotification("PAYMENT_COMPLETED", TOPIC_PAYMENT_COMPLETED, orderId,
        "CUSTOMER", evt.customerId, "Payment successful",
        string `We received your payment${paymentText(evt)} for order ${orderId}.`);

    // The payment event has no restaurant id, so look it up from the saved order.
    OrderParties? parties = check waitForParties(orderId);
    if parties is OrderParties {
        check createNotification("PAYMENT_COMPLETED", TOPIC_PAYMENT_COMPLETED, orderId,
            "RESTAURANT", parties.restaurantId, "Payment confirmed",
            string `Order ${orderId} is paid. You can start preparing it.`);
    } else {
        log:printWarn("Unknown order " + orderId + ", restaurant payment notice skipped");
    }
}

function handlePaymentFailed(byte[] payload) returns error? {
    json j = check toJson(payload);
    PaymentEvent evt = check j.cloneWithType();
    string reason = evt?.reason ?: "no reason given";

    check createNotification("PAYMENT_FAILED", TOPIC_PAYMENT_FAILED, evt.orderId,
        "CUSTOMER", evt.customerId, "Payment failed",
        string `Payment for order ${evt.orderId} failed: ${reason}.`);
}

// Builds " of NAD 45.50" when the payment event carries an amount, otherwise "".
function paymentText(PaymentEvent evt) returns string {
    decimal? amount = evt?.amount;
    string? currency = evt?.currency;
    if amount is decimal {
        string label = currency is string ? currency + " " : "";
        return string ` of ${label}${amount}`;
    }
    return "";
}

function handleDeliveryAssigned(byte[] payload) returns error? {
    json j = check toJson(payload);
    DeliveryEvent evt = check j.cloneWithType();
    string orderId = evt.orderId;

    check createNotification("DELIVERY_ASSIGNED", TOPIC_DELIVERY_ASSIGNED, orderId,
        "DRIVER", evt.driverId, "New delivery",
        string `You have been assigned order ${orderId}.`);

    OrderParties? parties = check waitForParties(orderId);
    if parties is () {
        log:printWarn("Unknown order " + orderId + ", customer and restaurant not notified");
        return;
    }

    // Remember the driver so later status events (READY, CANCELLED) can reach them.
    _ = check db->execute(`
        UPDATE order_parties SET driver_id = ${evt.driverId} WHERE order_id = ${orderId}`);

    check createNotification("DELIVERY_ASSIGNED", TOPIC_DELIVERY_ASSIGNED, orderId,
        "CUSTOMER", parties.customerId, "Driver assigned",
        string `A driver has been assigned to your order ${orderId}.`);
    check createNotification("DELIVERY_ASSIGNED", TOPIC_DELIVERY_ASSIGNED, orderId,
        "RESTAURANT", parties.restaurantId, "Driver assigned",
        string `A driver will collect order ${orderId} shortly.`);
}

function handleDeliveryCompleted(byte[] payload) returns error? {
    json j = check toJson(payload);
    DeliveryEvent evt = check j.cloneWithType();
    string orderId = evt.orderId;

    check createNotification("DELIVERY_COMPLETED", TOPIC_DELIVERY_COMPLETED, orderId,
        "DRIVER", evt.driverId, "Delivery complete",
        string `Delivery of order ${orderId} is complete. Thank you!`);

    OrderParties? parties = check waitForParties(orderId);
    if parties is () {
        log:printWarn("Unknown order " + orderId + ", customer and restaurant not notified");
        return;
    }

    // "ORDER_DELIVERED" is the same logical event as the DELIVERED order status,
    // so whichever event arrives second is ignored as a duplicate.
    check createNotification("ORDER_DELIVERED", TOPIC_DELIVERY_COMPLETED, orderId,
        "CUSTOMER", parties.customerId, "Order delivered",
        string `Your order ${orderId} has been delivered. Enjoy your meal!`);
    check createNotification("ORDER_DELIVERED", TOPIC_DELIVERY_COMPLETED, orderId,
        "RESTAURANT", parties.restaurantId, "Order delivered",
        string `Order ${orderId} was delivered to the customer.`);
}

// One listener per topic. Each has its own consumer group so the topics are
// consumed independently, and "earliest" means a brand new group reads from the
// start of the topic instead of missing messages sent before it started.
listener kafka:Listener orderCreatedListener = new (kafkaUrl, {
    groupId: "notification-service-orders-created",
    topics: [TOPIC_ORDER_CREATED],
    offsetReset: "earliest"
});

listener kafka:Listener orderStatusListener = new (kafkaUrl, {
    groupId: "notification-service-orders-status",
    topics: [TOPIC_ORDER_STATUS],
    offsetReset: "earliest"
});

listener kafka:Listener paymentCompletedListener = new (kafkaUrl, {
    groupId: "notification-service-payments-completed",
    topics: [TOPIC_PAYMENT_COMPLETED],
    offsetReset: "earliest"
});

listener kafka:Listener paymentFailedListener = new (kafkaUrl, {
    groupId: "notification-service-payments-failed",
    topics: [TOPIC_PAYMENT_FAILED],
    offsetReset: "earliest"
});

listener kafka:Listener deliveryAssignedListener = new (kafkaUrl, {
    groupId: "notification-service-delivery-assigned",
    topics: [TOPIC_DELIVERY_ASSIGNED],
    offsetReset: "earliest"
});

listener kafka:Listener deliveryCompletedListener = new (kafkaUrl, {
    groupId: "notification-service-delivery-completed",
    topics: [TOPIC_DELIVERY_COMPLETED],
    offsetReset: "earliest"
});

service on orderCreatedListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) {
        foreach kafka:BytesConsumerRecord rec in records {
            error? result = handleOrderCreated(rec.value);
            if result is error {
                log:printError("Bad " + TOPIC_ORDER_CREATED + " message, skipped", 'error = result);
            }
        }
    }
}

service on orderStatusListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) {
        foreach kafka:BytesConsumerRecord rec in records {
            error? result = handleOrderStatus(rec.value);
            if result is error {
                log:printError("Bad " + TOPIC_ORDER_STATUS + " message, skipped", 'error = result);
            }
        }
    }
}

service on paymentCompletedListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) {
        foreach kafka:BytesConsumerRecord rec in records {
            error? result = handlePaymentCompleted(rec.value);
            if result is error {
                log:printError("Bad " + TOPIC_PAYMENT_COMPLETED + " message, skipped", 'error = result);
            }
        }
    }
}

service on paymentFailedListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) {
        foreach kafka:BytesConsumerRecord rec in records {
            error? result = handlePaymentFailed(rec.value);
            if result is error {
                log:printError("Bad " + TOPIC_PAYMENT_FAILED + " message, skipped", 'error = result);
            }
        }
    }
}

service on deliveryAssignedListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) {
        foreach kafka:BytesConsumerRecord rec in records {
            error? result = handleDeliveryAssigned(rec.value);
            if result is error {
                log:printError("Bad " + TOPIC_DELIVERY_ASSIGNED + " message, skipped", 'error = result);
            }
        }
    }
}

service on deliveryCompletedListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) {
        foreach kafka:BytesConsumerRecord rec in records {
            error? result = handleDeliveryCompleted(rec.value);
            if result is error {
                log:printError("Bad " + TOPIC_DELIVERY_COMPLETED + " message, skipped", 'error = result);
            }
        }
    }
}

// Looks up notifications. Any filter that is empty (null) is ignored.
function queryNotifications(string? recipientType, string? recipientId, string? orderId,
        string? status, int maxResults) returns Notification[]|error {
    stream<Notification, sql:Error?> rows = db->query(`
        SELECT id, recipient_type AS recipientType, recipient_id AS recipientId,
               order_id AS orderId, title, message, channels, event_type AS eventType,
               status, DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s') AS createdAt,
               DATE_FORMAT(read_at, '%Y-%m-%d %H:%i:%s') AS readAt
        FROM notifications
        WHERE (${recipientType} IS NULL OR recipient_type = ${recipientType})
          AND (${recipientId} IS NULL OR recipient_id = ${recipientId})
          AND (${orderId} IS NULL OR order_id = ${orderId})
          AND (${status} IS NULL OR status = ${status})
        ORDER BY id DESC
        LIMIT ${maxResults}`, Notification);
    return from Notification n in rows
        select n;
}

function queryOneNotification(int id) returns Notification|error {
    Notification|error found = db->queryRow(`
        SELECT id, recipient_type AS recipientType, recipient_id AS recipientId,
               order_id AS orderId, title, message, channels, event_type AS eventType,
               status, DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s') AS createdAt,
               DATE_FORMAT(read_at, '%Y-%m-%d %H:%i:%s') AS readAt
        FROM notifications
        WHERE id = ${id}`, Notification);
    return found;
}

// Runs the list query and turns a failure into a 500 response.
function listResponse(string? recipientType, string? recipientId, string? orderId,
        string? status, int maxResults) returns Notification[]|http:InternalServerError {
    Notification[]|error result =
        queryNotifications(recipientType, recipientId, orderId, status, maxResults);
    if result is error {
        log:printError("Failed to read notifications", 'error = result);
        return <http:InternalServerError>{
            body: {
                message: "Failed to retrieve notifications"
            }
        };
    }
    return result;
}

function isValidStatus(string? status) returns boolean {
    return status is () || status == "UNREAD" || status == "READ";
}

function isValidRecipientType(string? recipientType) returns boolean {
    return recipientType is () || recipientType == "CUSTOMER"
        || recipientType == "RESTAURANT" || recipientType == "DRIVER";
}

service /notifications on new http:Listener(httpPort) {

    // health check
    resource function get health() returns string {
        return "Notification Service is running";
    }

    // list all notifications, optionally filtered:
    // GET /notifications?recipientType=CUSTOMER&recipientId=CUS-1&status=UNREAD&maxResults=20
    resource function get .(string? recipientType, string? recipientId, string? status,
            int maxResults = 100)
            returns Notification[]|http:BadRequest|http:InternalServerError {
        if !isValidRecipientType(recipientType) || !isValidStatus(status)
                || maxResults < 1 || maxResults > 500 {
            return <http:BadRequest>{
                body: {
                    message: "Invalid filter value, or maxResults is outside 1 to 500"
                }
            };
        }
        return listResponse(recipientType, recipientId, (), status, maxResults);
    }

    // notifications of one customer
    resource function get customer/[string customerId](string? status, int maxResults = 100)
            returns Notification[]|http:BadRequest|http:InternalServerError {
        if !isValidStatus(status) || maxResults < 1 || maxResults > 500 {
            return <http:BadRequest>{
                body: {
                    message: "status must be UNREAD or READ and maxResults must be 1 to 500"
                }
            };
        }
        return listResponse("CUSTOMER", customerId, (), status, maxResults);
    }

    // notifications of one restaurant
    resource function get restaurant/[string restaurantId](string? status, int maxResults = 100)
            returns Notification[]|http:BadRequest|http:InternalServerError {
        if !isValidStatus(status) || maxResults < 1 || maxResults > 500 {
            return <http:BadRequest>{
                body: {
                    message: "status must be UNREAD or READ and maxResults must be 1 to 500"
                }
            };
        }
        return listResponse("RESTAURANT", restaurantId, (), status, maxResults);
    }

    // notifications of one driver
    resource function get driver/[string driverId](string? status, int maxResults = 100)
            returns Notification[]|http:BadRequest|http:InternalServerError {
        if !isValidStatus(status) || maxResults < 1 || maxResults > 500 {
            return <http:BadRequest>{
                body: {
                    message: "status must be UNREAD or READ and maxResults must be 1 to 500"
                }
            };
        }
        return listResponse("DRIVER", driverId, (), status, maxResults);
    }

    // all notifications about one order
    resource function get orders/[string orderId]()
            returns Notification[]|http:InternalServerError {
        return listResponse((), (), orderId, (), 500);
    }

    // get one notification
    resource function get [int id]()
            returns Notification|http:NotFound|http:InternalServerError {
        Notification|error found = queryOneNotification(id);
        if found is Notification {
            return found;
        }
        if found is sql:NoRowsError {
            return <http:NotFound>{
                body: {
                    message: "Notification not found"
                }
            };
        }
        log:printError("Failed to read notification", 'error = found);
        return <http:InternalServerError>{
            body: {
                message: "Failed to retrieve notification"
            }
        };
    }

    // mark a notification as read
    resource function put [int id]/read()
            returns Notification|http:NotFound|http:InternalServerError {
        Notification|error found = queryOneNotification(id);
        if found is sql:NoRowsError {
            return <http:NotFound>{
                body: {
                    message: "Notification not found"
                }
            };
        }
        if found is error {
            log:printError("Failed to read notification", 'error = found);
            return <http:InternalServerError>{
                body: {
                    message: "Failed to retrieve notification"
                }
            };
        }

        sql:ExecutionResult|error result = db->execute(`
            UPDATE notifications
            SET status = 'READ', read_at = CURRENT_TIMESTAMP
            WHERE id = ${id} AND status = 'UNREAD'`);
        if result is error {
            log:printError("Failed to mark notification as read", 'error = result);
            return <http:InternalServerError>{
                body: {
                    message: "Failed to update notification"
                }
            };
        }

        Notification|error updated = queryOneNotification(id);
        if updated is Notification {
            return updated;
        }
        return <http:InternalServerError>{
            body: {
                message: "Failed to retrieve notification"
            }
        };
    }

    // send a one-off notification (for example an admin announcement)
    resource function post .(@http:Payload NewNotification newNotification)
            returns http:Created|http:BadRequest|http:InternalServerError {
        if newNotification.recipientId.trim() == "" || newNotification.title.trim() == ""
                || newNotification.message.trim() == "" {
            return <http:BadRequest>{
                body: {
                    message: "recipientId, title and message must not be empty"
                }
            };
        }

        error? result = createNotification("MANUAL-" + uuid:createRandomUuid(), "manual",
            newNotification.orderId, newNotification.recipientType,
            newNotification.recipientId, newNotification.title, newNotification.message);
        if result is error {
            log:printError("Failed to create manual notification", 'error = result);
            return <http:InternalServerError>{
                body: {
                    message: "Failed to create notification"
                }
            };
        }
        return <http:Created>{
            body: {
                message: "Notification created"
            }
        };
    }
}
