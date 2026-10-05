-- Delivery Service Schema (MySQL 8.0+)
CREATE DATABASE IF NOT EXISTS delivery_db
    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE delivery_db;

CREATE TABLE IF NOT EXISTS drivers (
    driver_id          VARCHAR(20)  PRIMARY KEY,
    name               VARCHAR(100) NOT NULL,
    phone              VARCHAR(20)  NOT NULL UNIQUE,
    vehicle_type       VARCHAR(30)  NOT NULL,
    status             VARCHAR(20)  NOT NULL DEFAULT 'OFFLINE',
    current_lat        DECIMAL(9,6),
    current_lng        DECIMAL(9,6),
    rating             DECIMAL(3,2) DEFAULT 5.00,
    active_delivery_id VARCHAR(30),
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                       ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT chk_driver_status
        CHECK (status IN ('AVAILABLE','BUSY','OFFLINE'))
) ENGINE=InnoDB;

CREATE INDEX idx_drivers_status ON drivers(status);

CREATE TABLE IF NOT EXISTS deliveries (
    delivery_id     VARCHAR(30) PRIMARY KEY,
    order_id        VARCHAR(30) NOT NULL UNIQUE,
    customer_id     VARCHAR(30) NOT NULL,
    restaurant_id   VARCHAR(30) NOT NULL,
    driver_id       VARCHAR(20),
    status          VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    pickup_lat      DECIMAL(9,6),
    pickup_lng      DECIMAL(9,6),
    pickup_address  TEXT,
    dropoff_lat     DECIMAL(9,6),
    dropoff_lng     DECIMAL(9,6),
    dropoff_address TEXT,
    distance_km     DECIMAL(6,2),
    estimated_minutes INT,
    assigned_at     TIMESTAMP NULL,
    picked_up_at    TIMESTAMP NULL,
    delivered_at    TIMESTAMP NULL,
    failure_reason  TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT chk_delivery_status
        CHECK (status IN ('PENDING','ASSIGNED','PICKED_UP','IN_TRANSIT',
                          'DELIVERED','FAILED','CANCELLED')),
    CONSTRAINT fk_deliveries_driver
        FOREIGN KEY (driver_id) REFERENCES drivers(driver_id)
) ENGINE=InnoDB;

CREATE INDEX idx_deliveries_status ON deliveries(status);
CREATE INDEX idx_deliveries_driver ON deliveries(driver_id);

CREATE TABLE IF NOT EXISTS delivery_events (
    event_id    BIGINT AUTO_INCREMENT PRIMARY KEY,
    delivery_id VARCHAR(30) NOT NULL,
    status      VARCHAR(20) NOT NULL,
    notes       TEXT,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_events_delivery
        FOREIGN KEY (delivery_id) REFERENCES deliveries(delivery_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS processed_events (
    event_id     VARCHAR(80) PRIMARY KEY,
    topic        VARCHAR(60) NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;