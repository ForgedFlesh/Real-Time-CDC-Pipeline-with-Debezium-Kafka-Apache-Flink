USE shop;

-- INSERT: target should receive a new row.
INSERT INTO orders (
    order_id, customer_id, status, amount, currency, shipping_address
)
VALUES (6001, 1001, 'CREATED', 799.00, 'INR', 'Kolkata');

-- UPDATE: target should upsert the same primary key.
UPDATE orders
SET status = 'PAID',
    amount = 849.00
WHERE order_id = 6001;

-- DELETE: target should delete the row.
DELETE FROM orders
WHERE order_id = 5003;

-- Invalid business record: exact replica receives it, validated table excludes it,
-- and cdc_rejects records INVALID_STATUS.
INSERT INTO orders (
    order_id, customer_id, status, amount, currency, shipping_address
)
VALUES (6002, 1002, 'BROKEN_STATUS', 450.00, 'INR', 'Bengaluru');

-- Another invalid event: negative amount.
INSERT INTO orders (
    order_id, customer_id, status, amount, currency, shipping_address
)
VALUES (6003, 1003, 'CREATED', -10.00, 'INR', 'Chennai');

-- Additive schema change:
-- Debezium captures the DDL and emits the new field.
-- Existing Flink SQL ignores the extra JSON field until its DDL and PostgreSQL
-- target are deliberately upgraded.
ALTER TABLE orders
ADD COLUMN discount_code VARCHAR(30) NULL;

UPDATE orders
SET discount_code = 'WELCOME10'
WHERE order_id = 5001;
