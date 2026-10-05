CREATE DATABASE IF NOT EXISTS food_delivery;
USE food_delivery;

CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(150) NOT NULL UNIQUE,
    phone VARCHAR(30) NOT NULL
);

CREATE TABLE IF NOT EXISTS addresses (
    address_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    label VARCHAR(50) NOT NULL,
    address VARCHAR(255) NOT NULL,
    FOREIGN KEY (customer_id)
        REFERENCES customers(customer_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS restaurants (
    restaurant_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    location VARCHAR(150) NOT NULL,
    opening_time TIME NOT NULL,
    closing_time TIME NOT NULL
);

CREATE TABLE IF NOT EXISTS menu_items (
    item_id VARCHAR(20) PRIMARY KEY,
    restaurant_id VARCHAR(20) NOT NULL,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    quantity INT NOT NULL DEFAULT 0,
    available BOOLEAN NOT NULL DEFAULT TRUE,
    FOREIGN KEY (restaurant_id)
        REFERENCES restaurants(restaurant_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    restaurant_id VARCHAR(20) NOT NULL,
    item_id VARCHAR(20) NOT NULL,
    quantity INT NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'CREATED',

    FOREIGN KEY (customer_id)
        REFERENCES customers(customer_id),

    FOREIGN KEY (restaurant_id)
        REFERENCES restaurants(restaurant_id),

    FOREIGN KEY (item_id)
        REFERENCES menu_items(item_id)
);