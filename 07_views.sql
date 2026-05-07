/*
 * 07_views.sql
 * Demonstrates: Views, Materialized Views, Advanced Aggregations, Window Functions
 * Description: Analytical reporting layer for dashboards and data analysts.
 */

SET search_path TO fraud_detection;

-- 1. High Risk Transactions View
CREATE OR REPLACE VIEW v_high_risk_transactions AS
SELECT 
  t.transaction_id,
  c.full_name AS customer_name,
  a.account_type,
  m.merchant_name,
  t.amount,
  t.txn_time,
  ff.rule_triggered AS rules_triggered,
  ff.risk_score
FROM fraud_flags ff
JOIN transactions t ON ff.transaction_id = t.transaction_id
JOIN accounts a ON t.account_id = a.account_id
JOIN customers c ON a.customer_id = c.customer_id
LEFT JOIN merchants m ON t.merchant_id = m.merchant_id
WHERE ff.risk_score >= 40
ORDER BY ff.risk_score DESC;


-- 2. Fraud by Merchant Category View
CREATE OR REPLACE VIEW v_fraud_by_merchant_category AS
SELECT 
  m.category,
  COUNT(ff.flag_id) AS total_flags,
  ROUND(AVG(ff.risk_score), 2) AS average_risk_score,
  SUM(t.amount) AS total_flagged_amount
FROM fraud_flags ff
JOIN transactions t ON ff.transaction_id = t.transaction_id
JOIN merchants m ON t.merchant_id = m.merchant_id
GROUP BY m.category
ORDER BY total_flags DESC;


-- 3. Customer Risk Profile View
CREATE OR REPLACE VIEW v_customer_risk_profile AS
WITH customer_stats AS (
  SELECT 
    c.customer_id,
    c.full_name,
    COUNT(t.transaction_id) AS total_transactions,
    COUNT(ff.flag_id) AS total_flagged_transactions,
    MAX(ff.risk_score) AS highest_risk_score,
    MODE() WITHIN GROUP (ORDER BY ff.rule_triggered) AS most_common_rule
  FROM customers c
  JOIN accounts a ON c.customer_id = a.customer_id
  LEFT JOIN transactions t ON a.account_id = t.account_id
  LEFT JOIN fraud_flags ff ON t.transaction_id = ff.transaction_id
  GROUP BY c.customer_id, c.full_name
)
SELECT 
  customer_id,
  full_name,
  total_transactions,
  total_flagged_transactions,
  highest_risk_score,
  most_common_rule,
  CASE 
    WHEN total_transactions = 0 THEN 'LOW'
    WHEN (total_flagged_transactions::numeric / total_transactions) > 0.1 OR highest_risk_score >= 80 THEN 'HIGH'
    WHEN (total_flagged_transactions::numeric / total_transactions) > 0.05 OR highest_risk_score >= 40 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS risk_level
FROM customer_stats;


-- 4. Daily Fraud Trend View
CREATE OR REPLACE VIEW v_daily_fraud_trend AS
WITH daily_stats AS (
  SELECT 
    DATE(ff.flagged_at) AS flag_day,
    COUNT(ff.flag_id) AS daily_flag_count,
    ROUND(AVG(ff.risk_score), 2) AS daily_avg_risk_score
  FROM fraud_flags ff
  GROUP BY DATE(ff.flagged_at)
)
SELECT 
  flag_day,
  daily_flag_count,
  daily_avg_risk_score,
  ROUND(AVG(daily_flag_count) OVER (ORDER BY flag_day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS rolling_7d_avg_flags
FROM daily_stats
ORDER BY flag_day DESC;


-- 5. Materialized View for Monthly Summary
DROP MATERIALIZED VIEW IF EXISTS mv_fraud_summary CASCADE;
CREATE MATERIALIZED VIEW mv_fraud_summary AS
SELECT 
  DATE_TRUNC('month', ff.flagged_at) AS month,
  m.category AS merchant_category,
  COUNT(ff.flag_id) AS total_flags,
  SUM(t.amount) AS total_flagged_amount,
  ROUND(AVG(ff.risk_score), 2) AS average_risk_score
FROM fraud_flags ff
JOIN transactions t ON ff.transaction_id = t.transaction_id
JOIN merchants m ON t.merchant_id = m.merchant_id
GROUP BY DATE_TRUNC('month', ff.flagged_at), m.category;

-- Unique index required for CONCURRENT refresh
CREATE UNIQUE INDEX idx_mv_fraud_summary_unique 
ON mv_fraud_summary(month, merchant_category);

/*
 * To keep the materialized view up to date without locking out reads, 
 * schedule the following command to run via a pg_cron job or external scheduler
 * (e.g., every night at 2:00 AM):
 * 
 * REFRESH MATERIALIZED VIEW CONCURRENTLY mv_fraud_summary;
 */
