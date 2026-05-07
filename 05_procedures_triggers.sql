/*
 * 05_procedures_triggers.sql
 * Demonstrates: Stored Procedures, Triggers, JSONB conversion, Error Handling
 * Description: Real-time and batch processing logic for fraud detection and auditing.
 */

SET search_path TO fraud_detection;

-- 1. Batch Fraud Scan Procedure
CREATE OR REPLACE PROCEDURE run_fraud_scan()
LANGUAGE plpgsql
AS $$
DECLARE
  v_start_time TIMESTAMPTZ := CURRENT_TIMESTAMP;
  v_end_time TIMESTAMPTZ;
  v_rows_inserted INTEGER := 0;
BEGIN
  WITH base_txns AS (
    SELECT t.* FROM transactions t
    WHERE t.status = 'success'
      AND NOT EXISTS (
        SELECT 1 FROM fraud_flags ff WHERE ff.transaction_id = t.transaction_id AND ff.reviewed = true
      )
  ),
  velocity_check AS (
    SELECT transaction_id, COUNT(*) OVER (PARTITION BY account_id ORDER BY txn_time RANGE BETWEEN INTERVAL '10 minutes' PRECEDING AND CURRENT ROW) as txn_count_10m
    FROM base_txns
  ),
  rolling_avg AS (
    SELECT transaction_id, AVG(amount) OVER (PARTITION BY account_id ORDER BY txn_time ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) as avg_amount_30_txns
    FROM base_txns
  ),
  geo_lag AS (
    SELECT transaction_id, location_city, LAG(location_city) OVER (PARTITION BY account_id ORDER BY txn_time) as prev_city, LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time) as prev_time
    FROM base_txns
  ),
  flagged_merchant_txns AS (
    SELECT t.transaction_id, COUNT(*) OVER (PARTITION BY t.account_id ORDER BY t.txn_time RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW) as flagged_txn_count_7d
    FROM base_txns t JOIN merchants m ON t.merchant_id = m.merchant_id WHERE m.is_flagged = true
  ),
  dormancy_check AS (
    SELECT transaction_id, LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time) as prev_txn_time
    FROM base_txns
  ),
  round_txns AS (
    SELECT transaction_id, COUNT(*) OVER (PARTITION BY account_id ORDER BY txn_time RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW) as round_txn_count_24h
    FROM base_txns WHERE MOD(amount, 1000) = 0
  ),
  risk_engine AS (
    SELECT 
      t.transaction_id,
      CASE WHEN v.txn_count_10m > 5 THEN 25 ELSE 0 END AS score_velocity,
      CASE WHEN t.amount > (3 * r.avg_amount_30_txns) AND r.avg_amount_30_txns IS NOT NULL THEN 20 ELSE 0 END AS score_amount,
      CASE WHEN g.prev_city IS NOT NULL AND t.location_city != g.prev_city AND (t.txn_time - g.prev_time) <= INTERVAL '30 minutes' THEN 30 ELSE 0 END AS score_geo,
      CASE WHEN COALESCE(f.flagged_txn_count_7d, 0) > 2 THEN 15 ELSE 0 END AS score_merchant,
      CASE WHEN d.prev_txn_time IS NOT NULL AND (t.txn_time - d.prev_txn_time) > INTERVAL '60 days' AND t.amount > 10000 THEN 20 ELSE 0 END AS score_dormancy,
      CASE WHEN COALESCE(rt.round_txn_count_24h, 0) >= 3 THEN 10 ELSE 0 END AS score_round
    FROM base_txns t
    LEFT JOIN velocity_check v ON t.transaction_id = v.transaction_id
    LEFT JOIN rolling_avg r ON t.transaction_id = r.transaction_id
    LEFT JOIN geo_lag g ON t.transaction_id = g.transaction_id
    LEFT JOIN flagged_merchant_txns f ON t.transaction_id = f.transaction_id
    LEFT JOIN dormancy_check d ON t.transaction_id = d.transaction_id
    LEFT JOIN round_txns rt ON t.transaction_id = rt.transaction_id
  ),
  scored_transactions AS (
    SELECT 
      transaction_id,
      (score_velocity + score_amount + score_geo + score_merchant + score_dormancy + score_round) AS total_risk_score,
      LTRIM(
        CASE WHEN score_velocity > 0 THEN ', VELOCITY ABUSE' ELSE '' END ||
        CASE WHEN score_amount > 0 THEN ', AMOUNT ANOMALY' ELSE '' END ||
        CASE WHEN score_geo > 0 THEN ', GEOGRAPHIC IMPOSSIBILITY' ELSE '' END ||
        CASE WHEN score_merchant > 0 THEN ', FLAGGED MERCHANT REPEAT' ELSE '' END ||
        CASE WHEN score_dormancy > 0 THEN ', DORMANT ACCOUNT SPIKE' ELSE '' END ||
        CASE WHEN score_round > 0 THEN ', ROUND AMOUNT CLUSTERING' ELSE '' END,
        ', '
      ) AS rules_triggered
    FROM risk_engine
  )
  INSERT INTO fraud_flags (transaction_id, rule_triggered, risk_score, flagged_at)
  SELECT 
    transaction_id,
    rules_triggered,
    LEAST(total_risk_score, 100),
    CURRENT_TIMESTAMP
  FROM scored_transactions
  WHERE total_risk_score >= 40
    AND NOT EXISTS (SELECT 1 FROM fraud_flags ff WHERE ff.transaction_id = scored_transactions.transaction_id);
    
  GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;
  v_end_time := CURRENT_TIMESTAMP;

  INSERT INTO scan_log (scan_start, scan_end, rows_inserted, status)
  VALUES (v_start_time, v_end_time, v_rows_inserted, 'SUCCESS');

EXCEPTION WHEN OTHERS THEN
  v_end_time := CURRENT_TIMESTAMP;
  INSERT INTO scan_log (scan_start, scan_end, rows_inserted, status, error_message)
  VALUES (v_start_time, v_end_time, 0, 'FAILED', SQLERRM);
END;
$$;


-- 2. Real-Time Auto Fraud Check Trigger
CREATE OR REPLACE FUNCTION auto_fraud_check()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
  v_velocity_score INTEGER := 0;
  v_amount_score INTEGER := 0;
  v_total_score INTEGER := 0;
  v_rules TEXT := '';
  v_avg_30d NUMERIC;
  v_txn_count_10m INTEGER;
BEGIN
  IF NEW.status != 'success' THEN
    RETURN NEW;
  END IF;

  -- Lightweight Rule 1: VELOCITY ABUSE
  SELECT COUNT(*) INTO v_txn_count_10m
  FROM transactions
  WHERE account_id = NEW.account_id 
    AND txn_time >= NEW.txn_time - INTERVAL '10 minutes'
    AND txn_time <= NEW.txn_time
    AND status = 'success';
    
  IF v_txn_count_10m > 5 THEN
    v_velocity_score := 25;
    v_rules := 'VELOCITY ABUSE';
  END IF;

  -- Lightweight Rule 2: AMOUNT ANOMALY
  SELECT AVG(amount) INTO v_avg_30d
  FROM transactions
  WHERE account_id = NEW.account_id
    AND txn_time >= NEW.txn_time - INTERVAL '30 days'
    AND txn_time < NEW.txn_time
    AND status = 'success';
    
  IF v_avg_30d IS NOT NULL AND NEW.amount > (3 * v_avg_30d) THEN
    v_amount_score := 20;
    IF LENGTH(v_rules) > 0 THEN v_rules := v_rules || ', '; END IF;
    v_rules := v_rules || 'AMOUNT ANOMALY';
  END IF;

  v_total_score := v_velocity_score + v_amount_score;

  IF v_total_score > 30 THEN
    INSERT INTO fraud_flags (transaction_id, rule_triggered, risk_score, flagged_at)
    VALUES (NEW.transaction_id, v_rules, v_total_score, CURRENT_TIMESTAMP);
  END IF;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_auto_fraud_check ON transactions;
CREATE TRIGGER trg_auto_fraud_check
AFTER INSERT ON transactions
FOR EACH ROW EXECUTE FUNCTION auto_fraud_check();


-- 3. Audit Log Trigger for Accounts
CREATE OR REPLACE FUNCTION audit_account_changes()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO audit_log (table_name, operation, record_id, changed_by, old_data, new_data)
  VALUES (
    TG_TABLE_NAME, 
    TG_OP, 
    NEW.account_id::text, 
    CURRENT_USER, 
    row_to_json(OLD)::jsonb, 
    row_to_json(NEW)::jsonb
  );
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_audit_accounts ON accounts;
CREATE TRIGGER trg_audit_accounts
AFTER UPDATE ON accounts
FOR EACH ROW EXECUTE FUNCTION audit_account_changes();
