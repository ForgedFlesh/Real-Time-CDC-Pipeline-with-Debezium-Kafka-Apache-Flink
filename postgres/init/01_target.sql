CREATE TABLE IF NOT EXISTS orders_current (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT,
    status VARCHAR(30),
    amount NUMERIC(12,2),
    currency VARCHAR(3),
    shipping_address TEXT,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    source_event_ts TIMESTAMPTZ,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    replicated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders_valid_current (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    status VARCHAR(30) NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    shipping_address TEXT,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    source_event_ts TIMESTAMPTZ,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    validated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS cdc_raw_events (
    event_id VARCHAR(64) PRIMARY KEY,
    topic_name VARCHAR(255) NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    kafka_timestamp TIMESTAMPTZ,
    payload TEXT,
    is_tombstone BOOLEAN NOT NULL DEFAULT FALSE,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (topic_name, kafka_partition, kafka_offset)
);

CREATE TABLE IF NOT EXISTS cdc_event_audit (
    event_id VARCHAR(64) PRIMARY KEY,
    topic_name VARCHAR(255) NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    operation VARCHAR(20),
    order_id BIGINT,
    source_file VARCHAR(255),
    source_position BIGINT,
    source_row INTEGER,
    source_gtid TEXT,
    source_ts_ms BIGINT,
    kafka_ts_ms BIGINT,
    validation_status VARCHAR(20),
    validation_reason TEXT,
    before_state TEXT,
    after_state TEXT,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (topic_name, kafka_partition, kafka_offset)
);

CREATE TABLE IF NOT EXISTS cdc_rejects (
    event_id VARCHAR(64) PRIMARY KEY,
    order_id BIGINT,
    operation VARCHAR(20),
    reason TEXT NOT NULL,
    amount_raw TEXT,
    status_raw TEXT,
    currency_raw TEXT,
    source_file VARCHAR(255),
    source_position BIGINT,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_reconciliation (
    check_id BIGSERIAL PRIMARY KEY,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_count BIGINT,
    target_count BIGINT,
    valid_target_count BIGINT,
    missing_in_target BIGINT,
    extra_in_target BIGINT,
    mismatched_rows BIGINT,
    reject_count BIGINT,
    audit_count BIGINT,
    raw_event_count BIGINT,
    status VARCHAR(20) NOT NULL,
    details JSONB
);

CREATE INDEX IF NOT EXISTS idx_audit_order_id
    ON cdc_event_audit(order_id);

CREATE INDEX IF NOT EXISTS idx_audit_source_ts
    ON cdc_event_audit(source_ts_ms);

CREATE INDEX IF NOT EXISTS idx_rejects_order_id
    ON cdc_rejects(order_id);

CREATE INDEX IF NOT EXISTS idx_raw_events_kafka_position
    ON cdc_raw_events(topic_name, kafka_partition, kafka_offset);
