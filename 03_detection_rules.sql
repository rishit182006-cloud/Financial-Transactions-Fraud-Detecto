/*
 * 03_detection_rules.sql
 * Demonstrates: Advanced Window Functions, CTEs, Time-series analysis, Partitions
 * Description: Individual queries for the 6 core fraud detection rules. 
 * Note: These are written as standalone SELECTs for demonstration/testing. 
 * Layer 4 combines these into the master risk engine.
 */

SET search_path TO fraud_detection;

-- Rule 1: VELOCITY ABUSE
-- Flag any account that makes more than 5 transactions within any 10-minute window.
WITH velocity_check AS (
  SELECT 
    transaction_id,
    account_id,
    amount,
    txn_time,
    COUNT(*) OVER (
      PARTITION BY account_id 
      ORDER BY txn_time 
      RANGE BETWEEN INTERVAL '10 minutes' PRECEDING AND CURRENT ROW
    ) as txn_count_10m
  FROM transactions
  WHERE status = 'success'
)
SELECT 
  transaction_id,
  account_id,
  amount,
  txn_time,
  'VELOCITY ABUSE' AS rule_triggered
FROM velocity_check
WHERE txn_count_10m > 5;


-- Rule 2: AMOUNT ANOMALY
-- Flag transactions where the amount is more than 3x the customer's rolling 30-day average spend.
WITH rolling_avg AS (
  SELECT 
    transaction_id,
    account_id,
    amount,
    txn_time,
    AVG(amount) OVER (
      PARTITION BY account_id 
      ORDER BY txn_time 
      ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
    ) as avg_amount_30_txns
  FROM transactions
  WHERE status = 'success'
)
SELECT 
  transaction_id,
  account_id,
  amount,
  txn_time,
  'AMOUNT ANOMALY' AS rule_triggered
FROM rolling_avg
WHERE amount > (3 * avg_amount_30_txns) AND avg_amount_30_txns IS NOT NULL;


-- Rule 3: GEOGRAPHIC IMPOSSIBILITY
-- Flag cases where the same account has two transactions in different cities within 30 minutes of each other.
WITH geo_lag AS (
  SELECT 
    transaction_id,
    account_id,
    amount,
    txn_time,
    location_city,
    LAG(location_city) OVER (PARTITION BY account_id ORDER BY txn_time) as prev_city,
    LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time) as prev_time
  FROM transactions
  WHERE status = 'success' AND location_city IS NOT NULL
)
SELECT 
  transaction_id,
  account_id,
  amount,
  txn_time,
  'GEOGRAPHIC IMPOSSIBILITY' AS rule_triggered
FROM geo_lag
WHERE prev_city IS NOT NULL 
  AND location_city != prev_city 
  AND (txn_time - prev_time) <= INTERVAL '30 minutes';


-- Rule 4: FLAGGED MERCHANT REPEAT
-- Flag accounts that transact with a merchant where is_flagged = true more than twice in 7 days.
WITH flagged_merchant_txns AS (
  SELECT 
    t.transaction_id,
    t.account_id,
    t.amount,
    t.txn_time,
    m.is_flagged,
    COUNT(*) OVER (
      PARTITION BY t.account_id 
      ORDER BY t.txn_time 
      RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW
    ) as flagged_txn_count_7d
  FROM transactions t
  JOIN merchants m ON t.merchant_id = m.merchant_id
  WHERE t.status = 'success' AND m.is_flagged = true
)
SELECT 
  transaction_id,
  account_id,
  amount,
  txn_time,
  'FLAGGED MERCHANT REPEAT' AS rule_triggered
FROM flagged_merchant_txns
WHERE flagged_txn_count_7d > 2;


-- Rule 5: DORMANT ACCOUNT SPIKE
-- Flag transactions on accounts that had zero transactions in the previous 60 days but suddenly have a transaction above 10,000.
WITH dormancy_check AS (
  SELECT 
    transaction_id,
    account_id,
    amount,
    txn_time,
    LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time) as prev_txn_time
  FROM transactions
  WHERE status = 'success'
)
SELECT 
  transaction_id,
  account_id,
  amount,
  txn_time,
  'DORMANT ACCOUNT SPIKE' AS rule_triggered
FROM dormancy_check
WHERE prev_txn_time IS NOT NULL 
  AND (txn_time - prev_txn_time) > INTERVAL '60 days'
  AND amount > 10000;


-- Rule 6: ROUND AMOUNT CLUSTERING
-- Flag accounts that make 3 or more transactions with perfectly round amounts (divisible by 1000) within 24 hours
WITH round_txns AS (
  SELECT 
    transaction_id,
    account_id,
    amount,
    txn_time,
    COUNT(*) OVER (
      PARTITION BY account_id 
      ORDER BY txn_time 
      RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW
    ) as round_txn_count_24h
  FROM transactions
  WHERE status = 'success' 
    AND MOD(amount, 1000) = 0
)
SELECT 
  transaction_id,
  account_id,
  amount,
  txn_time,
  'ROUND AMOUNT CLUSTERING' AS rule_triggered
FROM round_txns
WHERE round_txn_count_24h >= 3;
