/*
 * 04_risk_engine.sql
 * Demonstrates: Master CTEs, CASE WHEN logic, Risk Scoring
 * Description: Combines all detection rules into a single risk scoring engine. 
 * Calculates weighted scores and inserts high-risk flags into the fraud_flags table.
 */

SET search_path TO fraud_detection;

WITH base_txns AS (
  SELECT * FROM transactions WHERE status = 'success'
),
velocity_check AS (
  SELECT 
    transaction_id,
    COUNT(*) OVER (PARTITION BY account_id ORDER BY txn_time RANGE BETWEEN INTERVAL '10 minutes' PRECEDING AND CURRENT ROW) as txn_count_10m
  FROM base_txns
),
rolling_avg AS (
  SELECT 
    transaction_id,
    AVG(amount) OVER (PARTITION BY account_id ORDER BY txn_time ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) as avg_amount_30_txns
  FROM base_txns
),
geo_lag AS (
  SELECT 
    transaction_id,
    location_city,
    LAG(location_city) OVER (PARTITION BY account_id ORDER BY txn_time) as prev_city,
    LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time) as prev_time
  FROM base_txns
),
flagged_merchant_txns AS (
  SELECT 
    t.transaction_id,
    COUNT(*) OVER (PARTITION BY t.account_id ORDER BY t.txn_time RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW) as flagged_txn_count_7d
  FROM base_txns t
  JOIN merchants m ON t.merchant_id = m.merchant_id
  WHERE m.is_flagged = true
),
dormancy_check AS (
  SELECT 
    transaction_id,
    LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time) as prev_txn_time
  FROM base_txns
),
round_txns AS (
  SELECT 
    transaction_id,
    COUNT(*) OVER (PARTITION BY account_id ORDER BY txn_time RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW) as round_txn_count_24h
  FROM base_txns
  WHERE MOD(amount, 1000) = 0
),
risk_engine AS (
  SELECT 
    t.transaction_id,
    t.account_id,
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
    account_id,
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
  AND NOT EXISTS (
    SELECT 1 FROM fraud_flags ff WHERE ff.transaction_id = scored_transactions.transaction_id
  );
