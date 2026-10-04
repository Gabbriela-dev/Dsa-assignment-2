import ballerina/http;

type Order record {|
    string orderId;
    string customerId;
    string restaurantId;
    string itemId;
    int quantity;
    decimal totalAmount;
    string status;
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

map<Order> orders = {};
int orderCounter = 1;

service /orders on new http:Listener(8083) {

    // HEALTH CHECK
    resource function get health() returns string {
        return "Order Service is running";
    }

    // CREATE ORDER
    resource function post .(@http:Payload NewOrder newOrder)
            returns Order|http:BadRequest {

        if newOrder.quantity <= 0 {
            return <http:BadRequest>{
                body: {
                    message: "Order quantity must be greater than 0"
                }
            };
        }

        if newOrder.totalAmount <= 0.0d {
            return <http:BadRequest>{
                body: {
                    message: "Total amount must be greater than 0"
                }
            };
        }

        string orderId = string `ORD-${orderCounter}`;
        orderCounter += 1;

        Order createdOrder = {
            orderId: orderId,
            customerId: newOrder.customerId,
            restaurantId: newOrder.restaurantId,
            itemId: newOrder.itemId,
            quantity: newOrder.quantity,
            totalAmount: newOrder.totalAmount,
            status: "CREATED"
        };

        orders[orderId] = createdOrder;

        return createdOrder;
    }

    // GET ALL ORDERS
    resource function get .() returns Order[] {

        Order[] allOrders = [];

        foreach Order currentOrder in orders {
            allOrders.push(currentOrder);
        }

        return allOrders;
    }

    // GET ONE ORDER
    resource function get [string orderId]()
            returns Order|http:NotFound {

        Order? foundOrder = orders[orderId];

        if foundOrder is Order {
            return foundOrder;
        }

        return <http:NotFound>{
            body: {
                message: "Order not found"
            }
        };
    }

    // GET CUSTOMER ORDER HISTORY
    resource function get customer/[string customerId]()
            returns Order[] {

        Order[] customerOrders = [];

        foreach Order currentOrder in orders {
            if currentOrder.customerId == customerId {
                customerOrders.push(currentOrder);
            }
        }

        return customerOrders;
    }

    // GET RESTAURANT ORDERS
    resource function get restaurant/[string restaurantId]()
            returns Order[] {

        Order[] restaurantOrders = [];

        foreach Order currentOrder in orders {
            if currentOrder.restaurantId == restaurantId {
                restaurantOrders.push(currentOrder);
            }
        }

        return restaurantOrders;
    }

    // UPDATE ORDER STATUS
    resource function put [string orderId]/status(
            @http:Payload StatusUpdate statusUpdate)
            returns Order|http:NotFound|http:BadRequest {

        Order? existingOrder = orders[orderId];

        if existingOrder is () {
            return <http:NotFound>{
                body: {
                    message: "Order not found"
                }
            };
        }

        string currentStatus = existingOrder.status;
        string newStatus = statusUpdate.status;

        boolean validTransition = false;

        if currentStatus == "CREATED" && newStatus == "CONFIRMED" {
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

        } else if newStatus == "CANCELLED" &&
                currentStatus != "DELIVERED" &&
                currentStatus != "CANCELLED" {

            validTransition = true;
        }

        if !validTransition {
            return <http:BadRequest>{
                body: {
                    message: string `Invalid status transition from ${currentStatus} to ${newStatus}`
                }
            };
        }

        Order updatedOrder = {
            orderId: existingOrder.orderId,
            customerId: existingOrder.customerId,
            restaurantId: existingOrder.restaurantId,
            itemId: existingOrder.itemId,
            quantity: existingOrder.quantity,
            totalAmount: existingOrder.totalAmount,
            status: newStatus
        };

        orders[orderId] = updatedOrder;

        return updatedOrder;
    }
}