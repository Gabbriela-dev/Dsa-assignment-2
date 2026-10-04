import ballerina/http;

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
map<Restaurant> restaurants = {};
int restaurantCounter = 1;
map<MenuItem> menuItems = {};
int menuItemCounter = 1;

service /restaurants on new http:Listener(8082) {
resource function put [string restaurantId]/menu/[string itemId]/inventory(
        @http:Payload InventoryUpdate inventoryUpdate)
        returns MenuItem|http:NotFound {

    if !restaurants.hasKey(restaurantId) {
        return <http:NotFound>{
            body: {
                message: "Restaurant not found"
            }
        };
    }

    MenuItem? existingItem = menuItems[itemId];

    if existingItem is () || existingItem.restaurantId != restaurantId {
        return <http:NotFound>{
            body: {
                message: "Menu item not found"
            }
        };
    }

    MenuItem updatedItem = {
        itemId: existingItem.itemId,
        restaurantId: existingItem.restaurantId,
        name: existingItem.name,
        price: existingItem.price,
        quantity: inventoryUpdate.quantity,
        available: inventoryUpdate.quantity > 0
    };

    menuItems[itemId] = updatedItem;

    return updatedItem;
}
resource function post [string restaurantId]/menu(
        @http:Payload NewMenuItem newItem)
        returns MenuItem|http:NotFound {

    if !restaurants.hasKey(restaurantId) {
        return <http:NotFound>{
            body: {
                message: "Restaurant not found"
            }
        };
    }

    string itemId = string `ITEM-${menuItemCounter}`;
    menuItemCounter += 1;

    MenuItem menuItem = {
        itemId: itemId,
        restaurantId: restaurantId,
        name: newItem.name,
        price: newItem.price,
        quantity: newItem.quantity,
        available: newItem.quantity > 0
    };

    menuItems[itemId] = menuItem;

    return menuItem;
}
resource function get [string restaurantId]/menu()
        returns MenuItem[]|http:NotFound {

    if !restaurants.hasKey(restaurantId) {
        return <http:NotFound>{
            body: {
                message: "Restaurant not found"
            }
        };
    }

    MenuItem[] restaurantMenu = [];

    foreach MenuItem item in menuItems {
        if item.restaurantId == restaurantId {
            restaurantMenu.push(item);
        }
    }

    return restaurantMenu;
}
    resource function get health() returns string {
        return "Restaurant Service is running";
    }

    resource function post .(@http:Payload NewRestaurant newRestaurant)
            returns Restaurant {

        string restaurantId = string `RES-${restaurantCounter}`;
        restaurantCounter += 1;

        Restaurant restaurant = {
            restaurantId: restaurantId,
            name: newRestaurant.name,
            location: newRestaurant.location,
            openingTime: newRestaurant.openingTime,
            closingTime: newRestaurant.closingTime
        };

        restaurants[restaurantId] = restaurant;

        return restaurant;
    }

    resource function get [string restaurantId]()
            returns Restaurant|http:NotFound {

        Restaurant? restaurant = restaurants[restaurantId];

        if restaurant is Restaurant {
            return restaurant;
        }

        return <http:NotFound>{
            body: {
                message: "Restaurant not found"
            }
        };
    }
}
