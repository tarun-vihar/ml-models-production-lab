CREATE TABLE IF NOT EXISTS transactions (
  event_id VARCHAR(100) PRIMARY KEY,
  transaction_id VARCHAR(100) NOT NULL UNIQUE,
  account_id VARCHAR(100) NOT NULL,
  event_timestamp TIMESTAMPTZ NOT NULL,
  amount NUMERIC(19,4) NOT NULL,
  currency CHAR(3) NOT NULL,
  event_json JSONB NOT NULL,
  enriched_json JSONB,
  status VARCHAR(24) NOT NULL DEFAULT 'RECEIVED',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS processing_outbox (
  event_id VARCHAR(100) PRIMARY KEY,
  transaction_id VARCHAR(100) NOT NULL,
  payload JSONB NOT NULL,
  published BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS transaction_audit (
  audit_id BIGSERIAL PRIMARY KEY,
  event_id VARCHAR(100) NOT NULL,
  transaction_id VARCHAR(100) NOT NULL,
  status VARCHAR(24) NOT NULL,
  details JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(event_id, status)
);
