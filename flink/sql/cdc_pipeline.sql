SET 'execution.runtime-mode' = 'streaming';
SET 'table.local-time-zone' = 'UTC';
SET 'parallelism.default' = '3';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.min-pause' = '5s';
SET 'execution.checkpointing.timeout' = '2min';
SET 'execution.checkpointing.max-concurrent-checkpoints' = '1';
-- the below line means that debezium can redelicer events again in case of failues,bur as be have mentioned order_id as the pk
-- Flink can introduce stateful deduplication and normalize the CDC changelog using that key.
SET 'table.exec.source.cdc-events-duplicate' = 'true';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'pipeline.name' = 'mysql-orders-cdc-to-postgres';

-- Normalized Debezium changelog source. 
-- Flink interprets c/r/u/d as INSERT/UPDATE/DELETE records.
CREATE TABLE orders_cdc (
    -- this does not create a physical table anywhere..it says 
    -- “Create a logical table abstraction over Kafka topic mysql.shop.orders.”
    /*
    {
  "before": {
    "order_id": 101,
    "customer_id": 20,
    "status": "PAID",
    "amount": "500.00",
    "currency": "INR"
  },

  "after": {
    "order_id": 101,
    "customer_id": 20,
    "status": "SHIPPED",
    "amount": "500.00",
    "currency": "INR"
  },

  "source": {
    "version": "3.6.0",
    "connector": "mysql",
    "name": "mysql",
    "ts_ms": 1789434000000,
    "snapshot": "false",
    "db": "shop",
    "table": "orders",
    "server_id": 223344,
    "gtid": "abc:123",
    "file": "mysql-bin.000004",
    "pos": 18293,
    "row": 0
  },

  "op": "u",
  "ts_ms": 1789434000500
}
so this is an example of what an event inside kafka will look like..VIRTUAL means:It is available to Flink queries, but it is not an actual business column inside your orders row.
the last 3 neradata variables come from They come from Kafka itself:

which partition?
which offset?
when did Kafka receive/create the record?
    */

    source_event_ts TIMESTAMP_LTZ(3)
        METADATA FROM 'value.source.timestamp' VIRTUAL,
    source_database STRING
        METADATA FROM 'value.source.database' VIRTUAL,
    source_table STRING
        METADATA FROM 'value.source.table' VIRTUAL,
    source_properties MAP<STRING, STRING>
        METADATA FROM 'value.source.properties' VIRTUAL,
    kafka_partition INT
        METADATA FROM 'partition' VIRTUAL,
    kafka_offset BIGINT
        METADATA FROM 'offset' VIRTUAL,
    kafka_timestamp TIMESTAMP_LTZ(3)
        METADATA FROM 'timestamp' VIRTUAL,
-- these are our actual business columns,notice the amount is in string..cause later we will try try_cast(),
-- we dont want the job to fail here before the actual validation

    order_id BIGINT,
    customer_id BIGINT,
    status STRING,
    amount STRING,
    currency STRING,
    shipping_address STRING,
    created_at TIMESTAMP_LTZ(3),
    updated_at TIMESTAMP_LTZ(3),

    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'mysql.shop.orders',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-orders-current-v1',
    'properties.isolation.level' = 'read_committed',
    -- When this consumer group starts for the first time, read the topic from the beginning.
    'scan.startup.mode' = 'earliest-offset',
    'scan.topic-partition-discovery.interval' = '30s',
    -- because of this we get the dynamic behaviour otherwise flink will just treat it as a normal json
    'value.format' = 'debezium-json',
    'value.debezium-json.schema-include' = 'true',
    'value.debezium-json.ignore-parse-errors' = 'false',
    'value.debezium-json.timestamp-format.standard' = 'ISO-8601'
);
/*
SELECT * FROM orders_cdc;

is conceptually querying a table that keeps changing continuously.
*/

-- Raw event source used for immutable auditing and reject routing.
-- This reads the same topic with a separate consumer group.
CREATE TABLE orders_raw (
    payload ROW<
        `before` ROW<
            order_id BIGINT,
            customer_id BIGINT,
            status STRING,
            amount STRING,
            currency STRING,
            shipping_address STRING,
            created_at STRING,
            updated_at STRING
        >,
        `after` ROW<
            order_id BIGINT,
            customer_id BIGINT,
            status STRING,
            amount STRING,
            currency STRING,
            shipping_address STRING,
            created_at STRING,
            updated_at STRING
        >,
        `source` ROW<
            version STRING,
            connector STRING,
            name STRING,
            ts_ms BIGINT,
            snapshot STRING,
            db STRING,
            `table` STRING,
            server_id BIGINT,
            gtid STRING,
            file STRING,
            pos BIGINT,
            `row` INT
        >,
        op STRING,
        ts_ms BIGINT
    >,

    topic_name STRING METADATA FROM 'topic' VIRTUAL,
    kafka_partition INT METADATA FROM 'partition' VIRTUAL,
    kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
    kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
)
WITH (
    'connector' = 'kafka',
    'topic' = 'mysql.shop.orders',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-orders-audit-v2',
    'properties.isolation.level' = 'read_committed',
    'scan.startup.mode' = 'earliest-offset',
    'scan.topic-partition-discovery.interval' = '30s',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- Byte-for-byte Kafka value capture. This branch remains readable even when
-- the structured JSON branch cannot parse a new or malformed payload.
CREATE TABLE orders_raw_payload (
    payload STRING,
    topic_name STRING METADATA FROM 'topic' VIRTUAL,
    kafka_partition INT METADATA FROM 'partition' VIRTUAL,
    kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
    kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
) WITH (
    'connector' = 'kafka',
    'topic' = 'mysql.shop.orders',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-orders-raw-payload-v1',
    'properties.isolation.level' = 'read_committed',
    'scan.startup.mode' = 'earliest-offset',
    'scan.topic-partition-discovery.interval' = '30s',
    -- Do not interpret the Debezium JSON structure at all. Give me the Kafka value itself.
    'value.format' = 'raw',
    'value.raw.charset' = 'UTF-8'
);

-- this does not create any physical postgre table anywhere,This Flink table maps to that external PostgreSQL table.
/*
pg_orders_current is a Flink sink table definition mapped to the physical PostgreSQL table. So when your Flink job does something like:

INSERT INTO pg_orders_current
SELECT ...
FROM orders_cdc;

Flink sends those INSERT/UPDATE/DELETE changes through JDBC, and the actual PostgreSQL table orders_current is updated.

*/
CREATE TABLE pg_orders_current (
    order_id BIGINT,
    customer_id BIGINT,
    status STRING,
    amount DECIMAL(12,2),
    currency STRING,
    shipping_address STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    source_event_ts TIMESTAMP(3),
    kafka_partition INT,
    kafka_offset BIGINT,
    PRIMARY KEY (order_id) NOT ENFORCED
    -- Because this JDBC sink receives a changelog, not merely inserts.
    -- Flink's JDBC sink can operate in upsert mode rather than append-only mode.
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/analytics',
    'table-name' = 'orders_current',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver',
    'sink.buffer-flush.max-rows' = '500',
    'sink.buffer-flush.interval' = '1s',
    'sink.max-retries' = '5'
);
-- same like above 
CREATE TABLE pg_orders_valid_current (
    order_id BIGINT,
    customer_id BIGINT,
    status STRING,
    amount DECIMAL(12,2),
    currency STRING,
    shipping_address STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    source_event_ts TIMESTAMP(3),
    kafka_partition INT,
    kafka_offset BIGINT,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/analytics',
    'table-name' = 'orders_valid_current',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver',
    'sink.buffer-flush.max-rows' = '500',
    'sink.buffer-flush.interval' = '1s',
    'sink.max-retries' = '5'
);

CREATE TABLE pg_cdc_event_audit (
    event_id STRING,
    topic_name STRING,
    kafka_partition INT,
    kafka_offset BIGINT,
    operation STRING,
    order_id BIGINT,
    source_file STRING,
    source_position BIGINT,
    source_row INT,
    source_gtid STRING,
    source_ts_ms BIGINT,
    kafka_ts_ms BIGINT,
    validation_status STRING,
    validation_reason STRING,
    before_state STRING,
    after_state STRING,
    PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/analytics',
    'table-name' = 'cdc_event_audit',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver',
    'sink.buffer-flush.max-rows' = '500',
    'sink.buffer-flush.interval' = '1s',
    'sink.max-retries' = '5'
);

CREATE TABLE pg_cdc_rejects (
    event_id STRING,
    order_id BIGINT,
    operation STRING,
    reason STRING,
    amount_raw STRING,
    status_raw STRING,
    currency_raw STRING,
    source_file STRING,
    source_position BIGINT,
    kafka_partition INT,
    kafka_offset BIGINT,
    PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/analytics',
    'table-name' = 'cdc_rejects',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver',
    'sink.buffer-flush.max-rows' = '200',
    'sink.buffer-flush.interval' = '1s',
    'sink.max-retries' = '5'
);


CREATE TABLE pg_cdc_raw_events (
    event_id STRING,
    topic_name STRING,
    kafka_partition INT,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP(3),
    payload STRING,
    is_tombstone BOOLEAN,
    PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/analytics',
    'table-name' = 'cdc_raw_events',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver',
    'sink.buffer-flush.max-rows' = '500',
    'sink.buffer-flush.interval' = '1s',
    'sink.max-retries' = '5'
);
/*
A Flink VIEW here is a logical continuous transformation.

It doesn't create a physical intermediate database table.

Every change flowing through orders_cdc flows through this transformation
Input:

"500.25"

becomes:

500.25 DECIMAL But if Kafka contains:"abc" then:TRY_CAST(...)returns:NULL instead of failing the entire Flink job.That's why later you can reject the row cleanly.

*/
CREATE VIEW orders_typed AS
SELECT
    order_id,
    customer_id,
    status,
    TRY_CAST(amount AS DECIMAL(12,2)) AS amount,
    currency,
    shipping_address,
    CAST(created_at AS TIMESTAMP(3)) AS created_at,
    CAST(updated_at AS TIMESTAMP(3)) AS updated_at,
    source_event_ts,
    kafka_partition,
    kafka_offset
FROM orders_cdc;

CREATE VIEW orders_validated AS
SELECT *
FROM orders_typed
WHERE
    order_id IS NOT NULL
    AND customer_id IS NOT NULL
    AND amount IS NOT NULL
    AND amount >= CAST(0 AS DECIMAL(12,2))
    AND currency IN ('INR', 'USD', 'EUR')
    AND status IN (
        'CREATED', 'PAID', 'PROCESSING',
        'SHIPPED', 'DELIVERED', 'CANCELLED'
    );

CREATE VIEW order_event_enriched AS
SELECT
    MD5(
        CONCAT(
            topic_name, '|',
            CAST(kafka_partition AS STRING), '|',
            CAST(kafka_offset AS STRING)
        )
    ) AS event_id,

    topic_name,
    kafka_partition,
    kafka_offset,

    CASE payload.op
        WHEN 'c' THEN 'CREATE'
        WHEN 'r' THEN 'SNAPSHOT_READ'
        WHEN 'u' THEN 'UPDATE'
        WHEN 'd' THEN 'DELETE'
        ELSE COALESCE(payload.op, 'UNKNOWN')
    END AS operation,

    COALESCE(
        payload.`after`.order_id,
        payload.`before`.order_id
    ) AS order_id,

    payload.source.file AS source_file,
    payload.source.pos AS source_position,
    payload.source.`row` AS source_row,
    payload.source.gtid AS source_gtid,
    payload.source.ts_ms AS source_ts_ms,

    CAST(
        UNIX_TIMESTAMP(CAST(kafka_timestamp AS STRING)) * 1000
        AS BIGINT
    ) AS kafka_ts_ms,

    CASE
        WHEN payload.op = 'd' THEN 'PASS'
        WHEN payload.`after`.order_id IS NULL THEN 'FAIL'
        WHEN payload.`after`.customer_id IS NULL THEN 'FAIL'
        WHEN TRY_CAST(
            payload.`after`.amount AS DECIMAL(12,2)
        ) IS NULL THEN 'FAIL'
        WHEN TRY_CAST(
            payload.`after`.amount AS DECIMAL(12,2)
        ) < CAST(0 AS DECIMAL(12,2)) THEN 'FAIL'
        WHEN payload.`after`.currency NOT IN (
            'INR', 'USD', 'EUR'
        ) THEN 'FAIL'
        WHEN payload.`after`.status NOT IN (
            'CREATED',
            'PAID',
            'PROCESSING',
            'SHIPPED',
            'DELIVERED',
            'CANCELLED'
        ) THEN 'FAIL'
        ELSE 'PASS'
    END AS validation_status,

    CASE
        WHEN payload.op = 'd' THEN 'DELETE_EVENT'
        WHEN payload.`after`.order_id IS NULL THEN 'ORDER_ID_NULL'
        WHEN payload.`after`.customer_id IS NULL THEN 'CUSTOMER_ID_NULL'
        WHEN TRY_CAST(
            payload.`after`.amount AS DECIMAL(12,2)
        ) IS NULL THEN 'AMOUNT_NOT_NUMERIC'
        WHEN TRY_CAST(
            payload.`after`.amount AS DECIMAL(12,2)
        ) < CAST(0 AS DECIMAL(12,2)) THEN 'NEGATIVE_AMOUNT'
        WHEN payload.`after`.currency NOT IN (
            'INR', 'USD', 'EUR'
        ) THEN 'INVALID_CURRENCY'
        WHEN payload.`after`.status NOT IN (
            'CREATED',
            'PAID',
            'PROCESSING',
            'SHIPPED',
            'DELIVERED',
            'CANCELLED'
        ) THEN 'INVALID_STATUS'
        ELSE 'VALID'
    END AS validation_reason,

    CONCAT(
        'order_id=',
        COALESCE(
            CAST(payload.`before`.order_id AS STRING),
            'null'
        ),

        ',customer_id=',
        COALESCE(
            CAST(payload.`before`.customer_id AS STRING),
            'null'
        ),

        ',status=',
        COALESCE(
            payload.`before`.status,
            'null'
        ),

        ',amount=',
        COALESCE(
            payload.`before`.amount,
            'null'
        ),

        ',currency=',
        COALESCE(
            payload.`before`.currency,
            'null'
        )
    ) AS before_state,

    CONCAT(
        'order_id=',
        COALESCE(
            CAST(payload.`after`.order_id AS STRING),
            'null'
        ),

        ',customer_id=',
        COALESCE(
            CAST(payload.`after`.customer_id AS STRING),
            'null'
        ),

        ',status=',
        COALESCE(
            payload.`after`.status,
            'null'
        ),

        ',amount=',
        COALESCE(
            payload.`after`.amount,
            'null'
        ),

        ',currency=',
        COALESCE(
            payload.`after`.currency,
            'null'
        )
    ) AS after_state,

    payload.`after`.amount AS amount_raw,
    payload.`after`.status AS status_raw,
    payload.`after`.currency AS currency_raw

FROM orders_raw

WHERE payload.op IS NOT NULL;


CREATE VIEW raw_payload_enriched AS
SELECT
    MD5(
        CONCAT(
            topic_name, '|',
            CAST(kafka_partition AS STRING), '|',
            CAST(kafka_offset AS STRING)
        )
    ) AS event_id,
    topic_name,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    payload,
    payload IS NULL AS is_tombstone
FROM orders_raw_payload;

EXECUTE STATEMENT SET
BEGIN
    INSERT INTO pg_orders_current
    SELECT
        order_id,
        customer_id,
        status,
        amount,
        currency,
        shipping_address,
        created_at,
        updated_at,
        CAST(source_event_ts AS TIMESTAMP(3)),
        kafka_partition,
        kafka_offset
    FROM orders_typed;

    INSERT INTO pg_orders_valid_current
    SELECT
        order_id,
        customer_id,
        status,
        amount,
        currency,
        shipping_address,
        created_at,
        updated_at,
        CAST(source_event_ts AS TIMESTAMP(3)),
        kafka_partition,
        kafka_offset
    FROM orders_validated;


    INSERT INTO pg_cdc_raw_events
    SELECT
        event_id,
        topic_name,
        kafka_partition,
        kafka_offset,
        CAST(kafka_timestamp AS TIMESTAMP(3)),
        payload,
        is_tombstone
    FROM raw_payload_enriched;

    INSERT INTO pg_cdc_event_audit
    SELECT
        event_id,
        topic_name,
        kafka_partition,
        kafka_offset,
        operation,
        order_id,
        source_file,
        source_position,
        source_row,
        source_gtid,
        source_ts_ms,
        kafka_ts_ms,
        validation_status,
        validation_reason,
        before_state,
        after_state
    FROM order_event_enriched;

    INSERT INTO pg_cdc_rejects
    SELECT
        event_id,
        order_id,
        operation,
        validation_reason,
        amount_raw,
        status_raw,
        currency_raw,
        source_file,
        source_position,
        kafka_partition,
        kafka_offset
    FROM order_event_enriched
    WHERE validation_status = 'FAIL';
END;
