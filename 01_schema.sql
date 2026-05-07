/*
 * 01_schema.sql
 * Demonstrates: Schema design, Data types, Constraints (PK, FK, CHECK, UNIQUE), Defaults
 * Description: Sets up the core database structure for the Financial Transactions Fraud Detector.
 */

CREATE SCHEMA IF NOT EXISTS fraud_detection;
SET search_path TO fraud_detection;

-- Drop tables in correct order if they exist
DROP TABLE IF EXISTS scan_log CASCADE;
DROP TABLE IF EXISTS audit_log CASCADE;
DROP TABLE IF EXISTS fraud_flags CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS merchants CASCADE;
DROP TABLE IF EXISTS accounts CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

-- Customers Table
CREATE TABLE customers (
  customer_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  full_name VARCHAR(255) NOT NULL,
  email VARCHAR(255) UNIQUE NOT NULL,
  phone VARCHAR(50),
  date_of_birth DATE,
  kyc_verified BOOLEAN DEFAULT false,
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE customers IS 'Stores customer profiles and KYC status.';
COMMENT ON COLUMN customers.customer_id IS 'Unique identifier for the customer (UUID).';
COMMENT ON COLUMN customers.kyc_verified IS 'Boolean flag indicating if Know Your Customer process is complete.';

-- Accounts Table
CREATE TABLE accounts (
  account_id SERIAL PRIMARY KEY,
  customer_id UUID NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
  account_type VARCHAR(20) NOT NULL CHECK (account_type IN ('savings', 'current', 'wallet')),
  balance NUMERIC(15,2) NOT NULL DEFAULT 0.00 CHECK (balance >= 0),
  currency CHAR(3) DEFAULT 'INR',
  is_active BOOLEAN DEFAULT true,
  opened_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE accounts IS 'Stores account details associated with customers.';
COMMENT ON COLUMN accounts.balance IS 'Current balance of the account. Must be non-negative.';
COMMENT ON COLUMN accounts.account_type IS 'Type of the account (savings, current, wallet).';

-- Merchants Table
CREATE TABLE merchants (
  merchant_id SERIAL PRIMARY KEY,
  merchant_name VARCHAR(255) NOT NULL,
  category VARCHAR(50) NOT NULL CHECK (category IN ('food', 'electronics', 'gambling', 'travel', 'utility')),
  city VARCHAR(100),
  country VARCHAR(100),
  is_flagged BOOLEAN DEFAULT false
);
COMMENT ON TABLE merchants IS 'Stores merchant details and categorization.';
COMMENT ON COLUMN merchants.is_flagged IS 'Indicates if the merchant is globally flagged for suspicious activity.';

-- Transactions Table
CREATE TABLE transactions (
  transaction_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  account_id INT NOT NULL REFERENCES accounts(account_id),
  merchant_id INT REFERENCES merchants(merchant_id),
  amount NUMERIC(15,2) NOT NULL CHECK (amount > 0),
  currency CHAR(3) DEFAULT 'INR',
  txn_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  txn_type VARCHAR(20) NOT NULL CHECK (txn_type IN ('debit', 'credit', 'reversal')),
  device_id VARCHAR(100),
  ip_address INET,
  location_city VARCHAR(100),
  location_country VARCHAR(100),
  status VARCHAR(20) NOT NULL CHECK (status IN ('success', 'failed', 'pending', 'reversed'))
);
COMMENT ON TABLE transactions IS 'Stores all financial transactions.';
COMMENT ON COLUMN transactions.amount IS 'Transaction amount. Must be strictly positive.';
COMMENT ON COLUMN transactions.txn_time IS 'Exact timestamp of the transaction.';
COMMENT ON COLUMN transactions.status IS 'Current status of the transaction.';

-- Fraud Flags Table
CREATE TABLE fraud_flags (
  flag_id SERIAL PRIMARY KEY,
  transaction_id UUID NOT NULL REFERENCES transactions(transaction_id) ON DELETE CASCADE,
  rule_triggered VARCHAR(100) NOT NULL,
  risk_score INTEGER NOT NULL CHECK (risk_score >= 0 AND risk_score <= 100),
  flagged_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  reviewed BOOLEAN DEFAULT false,
  reviewer_notes TEXT
);
COMMENT ON TABLE fraud_flags IS 'Stores flagged transactions and their associated risk scores.';
COMMENT ON COLUMN fraud_flags.rule_triggered IS 'Name of the detection rule that triggered this flag.';
COMMENT ON COLUMN fraud_flags.risk_score IS 'Calculated risk score (0-100). >=40 is high risk.';

-- Audit Log Table
CREATE TABLE audit_log (
  log_id SERIAL PRIMARY KEY,
  table_name VARCHAR(100) NOT NULL,
  operation VARCHAR(10) NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
  record_id VARCHAR(100) NOT NULL,
  changed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  changed_by VARCHAR(100) DEFAULT CURRENT_USER,
  old_data JSONB,
  new_data JSONB
);
COMMENT ON TABLE audit_log IS 'Stores an immutable audit trail of critical record changes.';
COMMENT ON COLUMN audit_log.old_data IS 'State of the row before the change (JSON format).';
COMMENT ON COLUMN audit_log.new_data IS 'State of the row after the change (JSON format).';

-- Scan Log Table
CREATE TABLE scan_log (
  scan_id SERIAL PRIMARY KEY,
  scan_start TIMESTAMPTZ,
  scan_end TIMESTAMPTZ,
  rows_inserted INTEGER,
  status VARCHAR(50),
  error_message TEXT
);
COMMENT ON TABLE scan_log IS 'Logs the execution of the batch fraud scan procedure.';
