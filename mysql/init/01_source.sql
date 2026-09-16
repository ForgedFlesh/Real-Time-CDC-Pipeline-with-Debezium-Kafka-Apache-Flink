CREATE DATABASE IF NOT EXISTS shop;
USE shop;

CREATE TABLE IF NOT EXISTS customers (
    customer_id BIGINT PRIMARY KEY,
    full_name VARCHAR(150) NOT NULL,
    email VARCHAR(255),
    country_code CHAR(2),
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
        ON UPDATE CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS orders (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT,
    status VARCHAR(30),
    amount DECIMAL(12,2),
    currency VARCHAR(3),
    shipping_address VARCHAR(500),
    created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
        ON UPDATE CURRENT_TIMESTAMP(3),
    INDEX idx_orders_customer_id (customer_id),
    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dbz_heartbeat (
    id INT PRIMARY KEY,
    updated_at TIMESTAMP(3) NOT NULL
) ENGINE=InnoDB;

INSERT INTO customers (customer_id, full_name, email, country_code)
VALUES
    (1001, 'Aditi Sharma', 'aditi@example.com', 'IN'),
    (1002, 'Rahul Das', 'rahul@example.com', 'IN'),
    (1003, 'Meera Iyer', 'meera@example.com', 'IN')
ON DUPLICATE KEY UPDATE
    full_name = VALUES(full_name),
    email = VALUES(email),
    country_code = VALUES(country_code);

INSERT INTO orders
    (order_id, customer_id, status, amount, currency, shipping_address)
VALUES
    (5001, 1001, 'CREATED', 1250.00, 'INR', 'Kolkata'),
    (5002, 1002, 'PAID', 2599.50, 'INR', 'Bengaluru'),
    (5003, 1003, 'SHIPPED', 899.00, 'INR', 'Chennai')
ON DUPLICATE KEY UPDATE
    customer_id = VALUES(customer_id),
    status = VALUES(status),
    amount = VALUES(amount),
    currency = VALUES(currency),
    shipping_address = VALUES(shipping_address);

CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY 'dbz';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE,
      REPLICATION CLIENT, LOCK TABLES
ON *.* TO 'debezium'@'%';
GRANT INSERT, UPDATE ON shop.dbz_heartbeat TO 'debezium'@'%';
FLUSH PRIVILEGES;
