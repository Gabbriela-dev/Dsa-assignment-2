import ballerina/http;
import ballerina/sql;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;
import ballerinax/kafka;

configurable string dbHost = ?;
configurable int dbPort = ?;
configurable string dbUser = ?;
configurable string dbPassword = ?;
configurable string dbName = ?;
configurable string kafkaUrl = ?;

final mysql:Client dbClient = check new (
    host = dbHost,
    user = dbUser,
    password = dbPassword,
    database = dbName,
    port = dbPort
);

final kafka:Producer kafkaProducer = check new (
    kafkaUrl,
    {
        clientId: "order-service"
    }
);

type Order record {|
    string orderId;
    string customerId;
    string restaurantId;
    string itemId;
    int quantity;
    decimal totalAmount;
    string status;
|};

type OrderCreatedEvent record {|
    string orderId;
    string customerId;
    string restaurantId;
    string itemId;
    int quantity;
    decimal totalAmount;
    string status;
|};

type OrderStatusEvent record {|
    string orderId;
    string previousStatus;
    string newStatus;
|};

type NewOrder record {|
    string customerId;
    string restaurantId;
    string itemId;
    int quantity;
    decimal totalAmount;
|};

type StatusUpdate record {|
    string status;
|};

type CounterResult record {|
    int nextId;
|};

int orderCounter = 1;

//initialise counter
function initialiseCounter() returns error? {

    CounterResult orderResult = check dbClient->queryRow(
        `SELECT CAST(
            COALESCE(
                MAX(CAST(SUBSTRING(order_id, 5) AS UNSIGNED)),
                0
            ) + 1
            AS SIGNED
         ) AS nextId
         FROM orders`,
        CounterResult
    );

    orderCounter = orderResult.nextId;
}

//startup
public function main() returns error? {
    check initialiseCounter();
}

service /orders on new http:Listener(8083) {

    //health check
    resource function get health() returns string {
        return "Order Service is running";
    }

   //create order
resource function post .(@http:Payload NewOrder newOrder)
        returns Order|http:BadRequest|http:InternalServerError {

    if newOrder.quantity <= 0 {
        return <http:BadRequest>{
            body: {
                message: "Quantity must be greater than zero"
            }
        };
    }

    if newOrder.totalAmount <= 0.0d {
        return <http:BadRequest>{
            body: {
                message: "Total amount must be greater than zero"
            }
        };
    }

    string orderId = string `ORD-${orderCounter}`;
    orderCounter += 1;

    string status = "CREATED";

    sql:ExecutionResult|error result = dbClient->execute(
        `INSERT INTO orders
        (order_id, customer_id, restaurant_id, item_id,
         quantity, total_amount, status)
        VALUES
        (${orderId},
         ${newOrder.customerId},
         ${newOrder.restaurantId},
         ${newOrder.itemId},
         ${newOrder.quantity},
         ${newOrder.totalAmount},
         ${status})`
    );

    if result is error {
        return <http:InternalServerError>{
            body: {
                message: "Failed to create order"
            }
        };
    }

    Order createdOrder = {
        orderId: orderId,
        customerId: newOrder.customerId,
        restaurantId: newOrder.restaurantId,
        itemId: newOrder.itemId,
        quantity: newOrder.quantity,
        totalAmount: newOrder.totalAmount,
        status: status
    };

    OrderCreatedEvent orderEvent = {
        orderId: orderId,
        customerId: newOrder.customerId,
        restaurantId: newOrder.restaurantId,
        itemId: newOrder.itemId,
        quantity: newOrder.quantity,
        totalAmount: newOrder.totalAmount,
        status: status
    };

    json eventJson = orderEvent;

    kafka:Error? kafkaResult = kafkaProducer->send(
        {
            topic: "orders.created",
            value: eventJson.toJsonString().toBytes()
        }
    );

    if kafkaResult is error {
        return <http:InternalServerError>{
            body: {
                message: "Order created but Kafka event failed"
            }
        };
    }

    return createdOrder;
}

    //get all orders
    resource function get .()
            returns Order[]|http:InternalServerError {

        stream<Order, sql:Error?> orderStream = dbClient->query(
            `SELECT
                order_id AS orderId,
                customer_id AS customerId,
                restaurant_id AS restaurantId,
                item_id AS itemId,
                quantity,
                total_amount AS totalAmount,
                status
             FROM orders`,
            Order
        );

        Order[] allOrders = [];

        error? streamError = orderStream.forEach(
            function(Order orderRecord) {
                allOrders.push(orderRecord);
            }
        );

        if streamError is error {
            return <http:InternalServerError>{
                body: {
                    message: "Failed to retrieve orders"
                }
            };
        }

        return allOrders;
    }

    //get order
    resource function get [string orderId]()
            returns Order|http:NotFound|http:InternalServerError {

        Order|error foundOrder = dbClient->queryRow(
            `SELECT
                order_id AS orderId,
                customer_id AS customerId,
                restaurant_id AS restaurantId,
                item_id AS itemId,
                quantity,
                total_amount AS totalAmount,
                status
             FROM orders
             WHERE order_id = ${orderId}`,
            Order
        );

        if foundOrder is Order {
            return foundOrder;
        }

        if foundOrder is sql:NoRowsError {
            return <http:NotFound>{
                body: {
                    message: "Order not found"
                }
            };
        }

        return <http:InternalServerError>{
            body: {
                message: "Failed to retrieve order"
            }
        };
    }

    //get customer orders
    resource function get customer/[string customerId]()
            returns Order[]|http:InternalServerError {

        stream<Order, sql:Error?> orderStream = dbClient->query(
            `SELECT
                order_id AS orderId,
                customer_id AS customerId,
                restaurant_id AS restaurantId,
                item_id AS itemId,
                quantity,
                total_amount AS totalAmount,
                status
             FROM orders
             WHERE customer_id = ${customerId}`,
            Order
        );

        Order[] customerOrders = [];

        error? streamError = orderStream.forEach(
            function(Order orderRecord) {
                customerOrders.push(orderRecord);
            }
        );

        if streamError is error {
            return <http:InternalServerError>{
                body: {
                    message: "Failed to retrieve customer orders"
                }
            };
        }

        return customerOrders;
    }

    //get restaurant orders
    resource function get restaurant/[string restaurantId]()
            returns Order[]|http:InternalServerError {

        stream<Order, sql:Error?> orderStream = dbClient->query(
            `SELECT
                order_id AS orderId,
                customer_id AS customerId,
                restaurant_id AS restaurantId,
                item_id AS itemId,
                quantity,
                total_amount AS totalAmount,
                status
             FROM orders
             WHERE restaurant_id = ${restaurantId}`,
            Order
        );

        Order[] restaurantOrders = [];

        error? streamError = orderStream.forEach(
            function(Order orderRecord) {
                restaurantOrders.push(orderRecord);
            }
        );

        if streamError is error {
            return <http:InternalServerError>{
                body: {
                    message: "Failed to retrieve restaurant orders"
                }
            };
        }

        return restaurantOrders;
    }

    //update order status
    resource function put [string orderId]/status(
            @http:Payload StatusUpdate statusUpdate)
            returns Order|http:BadRequest|http:NotFound|
                    http:InternalServerError {

        Order|error foundOrder = dbClient->queryRow(
            `SELECT
                order_id AS orderId,
                customer_id AS customerId,
                restaurant_id AS restaurantId,
                item_id AS itemId,
                quantity,
                total_amount AS totalAmount,
                status
             FROM orders
             WHERE order_id = ${orderId}`,
            Order
        );

        if foundOrder is sql:NoRowsError {
            return <http:NotFound>{
                body: {
                    message: "Order not found"
                }
            };
        }

        if foundOrder is error {
            return <http:InternalServerError>{
                body: {
                    message: "Failed to retrieve order"
                }
            };
        }

        string currentStatus = foundOrder.status;
        string newStatus = statusUpdate.status;

        boolean validTransition = false;

        if newStatus == "CANCELLED" {
            if currentStatus != "DELIVERED" &&
                    currentStatus != "CANCELLED" {
                validTransition = true;
            }
        } else if currentStatus == "CREATED" &&
                newStatus == "CONFIRMED" {
            validTransition = true;
        } else if currentStatus == "CONFIRMED" &&
                newStatus == "PREPARING" {
            validTransition = true;
        } else if currentStatus == "PREPARING" &&
                newStatus == "READY" {
            validTransition = true;
        } else if currentStatus == "READY" &&
                newStatus == "OUT_FOR_DELIVERY" {
            validTransition = true;
        } else if currentStatus == "OUT_FOR_DELIVERY" &&
                newStatus == "DELIVERED" {
            validTransition = true;
        }

        if !validTransition {
            return <http:BadRequest>{
                body: {
                    message: string `Invalid order status transition from ${currentStatus} to ${newStatus}`
                }
            };
        }

        sql:ExecutionResult|error result = dbClient->execute(
            `UPDATE orders
             SET status = ${newStatus}
             WHERE order_id = ${orderId}`
        );

        if result is error {
            return <http:InternalServerError>{
                body: {
                    message: "Failed to update order status"
                }
            };
        }
        //publish status event
OrderStatusEvent statusEvent = {
    orderId: orderId,
    previousStatus: currentStatus,
    newStatus: newStatus
};

json statusEventJson = statusEvent;

kafka:Error? kafkaResult = kafkaProducer->send(
    {
        topic: "orders.status.updated",
        value: statusEventJson.toJsonString().toBytes()
    }
);

if kafkaResult is error {
    return <http:InternalServerError>{
        body: {
            message: "Order status updated but Kafka event failed"
        }
    };
}




        Order updatedOrder = {
            orderId: foundOrder.orderId,
            customerId: foundOrder.customerId,
            restaurantId: foundOrder.restaurantId,
            itemId: foundOrder.itemId,
            quantity: foundOrder.quantity,
            totalAmount: foundOrder.totalAmount,
            status: newStatus
        };

        return updatedOrder;
    }
}