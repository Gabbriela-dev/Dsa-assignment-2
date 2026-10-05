import ballerina/http;
import ballerina/sql;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;


// DATABASE CONFIGURATION


configurable string dbHost = ?;
configurable int dbPort = ?;
configurable string dbUser = ?;
configurable string dbPassword = ?;
configurable string dbName = ?;

final mysql:Client dbClient = check new (
    host = dbHost,
    user = dbUser,
    password = dbPassword,
    database = dbName,
    port = dbPort
);


// RECORD TYPES


type Restaurant record {|
    string restaurantId;
    string name;
    string location;
    string openingTime;
    string closingTime;
|};

type NewRestaurant record {|
    string name;
    string location;
    string openingTime;
    string closingTime;
|};

type MenuItem record {|
    string itemId;
    string restaurantId;
    string name;
    decimal price;
    int quantity;
    boolean available;
|};

type NewMenuItem record {|
    string name;
    decimal price;
    int quantity;
|};

type InventoryUpdate record {|
    int quantity;
|};

type CounterResult record {|
    int nextId;
|};


// ID COUNTERS


int restaurantCounter = 1;
int menuItemCounter = 1;


// INITIALISE COUNTERS


function initialiseCounters() returns error? {

    CounterResult restaurantResult = check dbClient->queryRow(
        `SELECT CAST(
            COALESCE(
                MAX(CAST(SUBSTRING(restaurant_id, 5) AS UNSIGNED)),
                0
            ) + 1
            AS SIGNED
         ) AS nextId
         FROM restaurants`,
        CounterResult
    );

    restaurantCounter = restaurantResult.nextId;

    CounterResult menuItemResult = check dbClient->queryRow(
        `SELECT CAST(
            COALESCE(
                MAX(CAST(SUBSTRING(item_id, 6) AS UNSIGNED)),
                0
            ) + 1
            AS SIGNED
         ) AS nextId
         FROM menu_items`,
        CounterResult
    );

    menuItemCounter = menuItemResult.nextId;
}


// STARTUP


public function main() returns error? {
    check initialiseCounters();
}


// RESTAURANT SERVICE


service /restaurants on new http:Listener(8082) {

   
    // HEALTH CHECK
    

    resource function get health() returns string {
        return "Restaurant Service is running";
    }

   
    // CREATE RESTAURANT
   

    resource function post .(@http:Payload NewRestaurant newRestaurant)
            returns Restaurant|http:InternalServerError {

        string restaurantId = string `RES-${restaurantCounter}`;
        restaurantCounter += 1;

        sql:ExecutionResult|error result = dbClient->execute(
            `INSERT INTO restaurants
            (restaurant_id, name, location, opening_time, closing_time)
            VALUES
            (${restaurantId},
             ${newRestaurant.name},
             ${newRestaurant.location},
             ${newRestaurant.openingTime},
             ${newRestaurant.closingTime})`
        );

        if result is error {
            return <http:InternalServerError>{
                body: {
                    message: "Failed to create restaurant"
                }
            };
        }

        Restaurant restaurant = {
            restaurantId: restaurantId,
            name: newRestaurant.name,
            location: newRestaurant.location,
            openingTime: newRestaurant.openingTime,
            closingTime: newRestaurant.closingTime
        };

        return restaurant;
    }

    
    // GET RESTAURANT
    

    resource function get [string restaurantId]()
            returns Restaurant|http:NotFound|http:InternalServerError {

        Restaurant|error restaurant = dbClient->queryRow(
            `SELECT
                restaurant_id AS restaurantId,
                name,
                location,
                TIME_FORMAT(opening_time, '%H:%i') AS openingTime,
                TIME_FORMAT(closing_time, '%H:%i') AS closingTime
             FROM restaurants
             WHERE restaurant_id = ${restaurantId}`,
            Restaurant
        );

        if restaurant is Restaurant {
            return restaurant;
        }

        if restaurant is sql:NoRowsError {
            return <http:NotFound>{
                body: {
                    message: "Restaurant not found"
                }
            };
        }

        return <http:InternalServerError>{
            body: {
                message: "Failed to retrieve restaurant"
            }
        };
    }

  
    // ADD MENU ITEM
    

    resource function post [string restaurantId]/menu(
            @http:Payload NewMenuItem newItem)
            returns MenuItem|http:NotFound|http:InternalServerError {

        Restaurant|error restaurant = dbClient->queryRow(
            `SELECT
                restaurant_id AS restaurantId,
                name,
                location,
                TIME_FORMAT(opening_time, '%H:%i') AS openingTime,
                TIME_FORMAT(closing_time, '%H:%i') AS closingTime
             FROM restaurants
             WHERE restaurant_id = ${restaurantId}`,
            Restaurant
        );

        if restaurant is sql:NoRowsError {
            return <http:NotFound>{
                body: {
                    message: "Restaurant not found"
                }
            };
        }

        if restaurant is error {
            return <http:InternalServerError>{
                body: {
                    message: "Failed to retrieve restaurant"
                }
            };
        }

        string itemId = string `ITEM-${menuItemCounter}`;
        menuItemCounter += 1;

        boolean available = newItem.quantity > 0;

        sql:ExecutionResult|error result = dbClient->execute(
            `INSERT INTO menu_items
            (item_id, restaurant_id, name, price, quantity, available)
            VALUES
            (${itemId},
             ${restaurantId},
             ${newItem.name},
             ${newItem.price},
             ${newItem.quantity},
             ${available})`
        );

        if result is error {
            return <http:InternalServerError>{
                body: {
                    message: "Failed to add menu item"
                }
            };
        }

        MenuItem menuItem = {
            itemId: itemId,
            restaurantId: restaurantId,
            name: newItem.name,
            price: newItem.price,
            quantity: newItem.quantity,
            available: available
        };

        return menuItem;
    }

   
    // GET RESTAURANT MENU
    

    resource function get [string restaurantId]/menu()
            returns MenuItem[]|http:NotFound|http:InternalServerError {

        Restaurant|error restaurant = dbClient->queryRow(
            `SELECT
                restaurant_id AS restaurantId,
                name,
                location,
                TIME_FORMAT(opening_time, '%H:%i') AS openingTime,
                TIME_FORMAT(closing_time, '%H:%i') AS closingTime
             FROM restaurants
             WHERE restaurant_id = ${restaurantId}`,
            Restaurant
        );

        if restaurant is sql:NoRowsError {
            return <http:NotFound>{
                body: {
                    message: "Restaurant not found"
                }
            };
        }

        if restaurant is error {
            return <http:InternalServerError>{
                body: {
                    message: "Failed to retrieve restaurant"
                }
            };
        }

        stream<MenuItem, sql:Error?> menuStream = dbClient->query(
            `SELECT
                item_id AS itemId,
                restaurant_id AS restaurantId,
                name,
                price,
                quantity,
                available
             FROM menu_items
             WHERE restaurant_id = ${restaurantId}`,
            MenuItem
        );

        MenuItem[] restaurantMenu = [];

        error? streamError = menuStream.forEach(
            function(MenuItem item) {
                restaurantMenu.push(item);
            }
        );

        if streamError is error {
            return <http:InternalServerError>{
                body: {
                    message: "Failed to retrieve restaurant menu"
                }
            };
        }

        return restaurantMenu;
    }

   
    // UPDATE INVENTORY
    

    resource function put [string restaurantId]/menu/[string itemId]/inventory(
            @http:Payload InventoryUpdate inventoryUpdate)
            returns MenuItem|http:NotFound|http:InternalServerError {

        MenuItem|error existingItem = dbClient->queryRow(
            `SELECT
                item_id AS itemId,
                restaurant_id AS restaurantId,
                name,
                price,
                quantity,
                available
             FROM menu_items
             WHERE item_id = ${itemId}
             AND restaurant_id = ${restaurantId}`,
            MenuItem
        );

        if existingItem is sql:NoRowsError {
            return <http:NotFound>{
                body: {
                    message: "Menu item not found"
                }
            };
        }

        if existingItem is error {
            return <http:InternalServerError>{
                body: {
                    message: "Failed to retrieve menu item"
                }
            };
        }

        boolean available = inventoryUpdate.quantity > 0;

        sql:ExecutionResult|error result = dbClient->execute(
            `UPDATE menu_items
             SET quantity = ${inventoryUpdate.quantity},
                 available = ${available}
             WHERE item_id = ${itemId}
             AND restaurant_id = ${restaurantId}`
        );

        if result is error {
            return <http:InternalServerError>{
                body: {
                    message: "Failed to update inventory"
                }
            };
        }

        MenuItem updatedItem = {
            itemId: existingItem.itemId,
            restaurantId: existingItem.restaurantId,
            name: existingItem.name,
            price: existingItem.price,
            quantity: inventoryUpdate.quantity,
            available: available
        };

        return updatedItem;
    }
}
