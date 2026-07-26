-- V1__create_schema.sql
-- Database schema for Execution Reports Service

-- 1. orders: Stores the current calculated state of each order
CREATE TABLE orders (
    numeric_order_id            BIGINT PRIMARY KEY,
    market_order_id             VARCHAR(64),
    ticker                      VARCHAR(32),
    side                        VARCHAR(16),
    security_type               VARCHAR(32),
    status                      VARCHAR(20) NOT NULL,
    order_price                 NUMERIC(18, 6),
    nominal_amounts             NUMERIC(18, 6),
    leaves_nominal_amount       NUMERIC(18, 6),
    accumulative_nominal_amount NUMERIC(18, 6),
    avg_price                   NUMERIC(18, 6),
    executions_applied_count    INT NOT NULL DEFAULT 0,
    last_applied_fix_id         BIGINT,
    version                     INT NOT NULL DEFAULT 0,
    created_at                  TIMESTAMP NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMP NOT NULL DEFAULT now()
);

-- 2. order_ledger: Immutable history of applied ExecutionReports
CREATE TABLE order_ledger (
    id                BIGSERIAL PRIMARY KEY,
    numeric_order_id  BIGINT NOT NULL REFERENCES orders(numeric_order_id),
    fix_id            BIGINT NOT NULL UNIQUE,
    status            VARCHAR(20) NOT NULL,
    payload           JSONB NOT NULL,
    applied_at        TIMESTAMP NOT NULL DEFAULT now()
);

-- Index for querying ledger entries by order in order of insertion
CREATE INDEX idx_ledger_order_id ON order_ledger(numeric_order_id, id ASC);

-- 3. settlement_outbox: Transactional outbox for settlement events
CREATE TABLE settlement_outbox (
    id                BIGSERIAL PRIMARY KEY,
    numeric_order_id  BIGINT NOT NULL,
    payload           JSONB NOT NULL,
    status            VARCHAR(10) NOT NULL DEFAULT 'PENDING',
    created_at        TIMESTAMP NOT NULL DEFAULT now(),
    sent_at           TIMESTAMP
);

-- Index for fetching pending outbox records efficiently
CREATE INDEX idx_outbox_pending ON settlement_outbox(id) WHERE status = 'PENDING';
