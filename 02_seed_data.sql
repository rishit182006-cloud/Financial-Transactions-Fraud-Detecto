/*
 * 02_seed_data.sql
 * Demonstrates: Data generation, generate_series, random data seeding, PL/pgSQL blocks
 * Description: Populates the tables with realistic data for 50+ customers, 80+ accounts, 30+ merchants, and 500+ transactions, deliberately including anomalies to trigger detection rules.
 */

SET search_path TO fraud_detection;

-- Generate 55 Customers
INSERT INTO customers (customer_id, full_name, email, phone, date_of_birth, kyc_verified, created_at)
SELECT
  gen_random_uuid(),
  'Customer ' || i,
  'customer' || i || '@example.com',
  '555-' || lpad(i::text, 4, '0'),
  CURRENT_DATE - (INTERVAL '1 year' * (18 + floor(random() * 50))),
  (random() > 0.1),
  CURRENT_TIMESTAMP - (INTERVAL '1 day' * floor(random() * 365))
FROM generate_series(1, 55) s(i);

-- Generate 85 Accounts
INSERT INTO accounts (customer_id, account_type, balance, currency, is_active, opened_at)
SELECT
  (SELECT customer_id FROM customers ORDER BY random() LIMIT 1),
  CASE WHEN random() < 0.6 THEN 'savings' WHEN random() < 0.9 THEN 'current' ELSE 'wallet' END,
  floor(random() * 100000) + 1000,
  'INR',
  true,
  CURRENT_TIMESTAMP - (INTERVAL '1 day' * floor(random() * 300))
FROM generate_series(1, 85);

-- Generate 35 Merchants (First 5 are gambling, 2 and 4 are flagged)
INSERT INTO merchants (merchant_name, category, city, country, is_flagged)
SELECT
  'Merchant ' || i,
  CASE
    WHEN i <= 5 THEN 'gambling'
    WHEN i <= 15 THEN 'food'
    WHEN i <= 25 THEN 'electronics'
    WHEN i <= 30 THEN 'travel'
    ELSE 'utility'
  END,
  CASE WHEN random() < 0.5 THEN 'Mumbai' ELSE 'Delhi' END,
  'India',
  (i IN (2, 4))
FROM generate_series(1, 35) s(i);

-- Generate Base Transactions (about 500 random ones spread over 90 days)
INSERT INTO transactions (account_id, merchant_id, amount, txn_time, txn_type, status, location_city, location_country)
SELECT
  (SELECT account_id FROM accounts ORDER BY random() LIMIT 1),
  (SELECT merchant_id FROM merchants ORDER BY random() LIMIT 1),
  round((random() * 5000 + 100)::numeric, 2),
  CURRENT_TIMESTAMP - (INTERVAL '1 day' * (random() * 90)),
  'debit',
  'success',
  'Mumbai',
  'India'
FROM generate_series(1, 500);

-- Plant Anomalies for Detection Rules

-- Rule 1: VELOCITY ABUSE (More than 5 transactions within 10 minutes)
DO $$
DECLARE
  v_account_id INT;
  v_time TIMESTAMPTZ := CURRENT_TIMESTAMP - INTERVAL '5 days';
BEGIN
  SELECT account_id INTO v_account_id FROM accounts LIMIT 1;
  FOR i IN 1..7 LOOP
    INSERT INTO transactions (account_id, merchant_id, amount, txn_time, txn_type, status, location_city, location_country)
    VALUES (v_account_id, 1, 50.00, v_time + (INTERVAL '1 minute' * i), 'debit', 'success', 'Mumbai', 'India');
  END LOOP;
END $$;

-- Rule 2: AMOUNT ANOMALY (Amount > 3x rolling 30-day avg)
DO $$
DECLARE
  v_account_id INT;
  v_time TIMESTAMPTZ := CURRENT_TIMESTAMP - INTERVAL '10 days';
BEGIN
  SELECT account_id INTO v_account_id FROM accounts OFFSET 1 LIMIT 1;
  FOR i IN 1..5 LOOP
    INSERT INTO transactions (account_id, merchant_id, amount, txn_time, txn_type, status, location_city, location_country)
    VALUES (v_account_id, 1, 100.00, v_time - (INTERVAL '1 day' * i), 'debit', 'success', 'Mumbai', 'India');
  END LOOP;
  INSERT INTO transactions (account_id, merchant_id, amount, txn_time, txn_type, status, location_city, location_country)
  VALUES (v_account_id, 1, 5000.00, v_time, 'debit', 'success', 'Mumbai', 'India');
END $$;

-- Rule 3: GEOGRAPHIC IMPOSSIBILITY (Same account, 2 cities within 30 mins)
DO $$
DECLARE
  v_account_id INT;
  v_time TIMESTAMPTZ := CURRENT_TIMESTAMP - INTERVAL '15 days';
BEGIN
  SELECT account_id INTO v_account_id FROM accounts OFFSET 2 LIMIT 1;
  INSERT INTO transactions (account_id, merchant_id, amount, txn_time, txn_type, status, location_city, location_country)
  VALUES (v_account_id, 1, 500.00, v_time, 'debit', 'success', 'Mumbai', 'India');

  INSERT INTO transactions (account_id, merchant_id, amount, txn_time, txn_type, status, location_city, location_country)
  VALUES (v_account_id, 2, 600.00, v_time + INTERVAL '10 minutes', 'debit', 'success', 'New York', 'USA');
END $$;

-- Rule 4: FLAGGED MERCHANT REPEAT (Transact with flagged merchant > twice in 7 days)
DO $$
DECLARE
  v_account_id INT;
  v_time TIMESTAMPTZ := CURRENT_TIMESTAMP - INTERVAL '20 days';
BEGIN
  SELECT account_id INTO v_account_id FROM accounts OFFSET 3 LIMIT 1;
  FOR i IN 1..4 LOOP
    INSERT INTO transactions (account_id, merchant_id, amount, txn_time, txn_type, status, location_city, location_country)
    VALUES (v_account_id, 2, 100.00, v_time + (INTERVAL '1 day' * i), 'debit', 'success', 'Mumbai', 'India');
  END LOOP;
END $$;

-- Rule 5: DORMANT ACCOUNT SPIKE (No txns for 60 days, then > 10,000)
DO $$
DECLARE
  v_account_id INT;
  v_time TIMESTAMPTZ := CURRENT_TIMESTAMP;
BEGIN
  SELECT account_id INTO v_account_id FROM accounts OFFSET 4 LIMIT 1;
  -- Remove recent transactions to make it dormant
  DELETE FROM transactions WHERE account_id = v_account_id AND txn_time > v_time - INTERVAL '65 days';
  
  -- Add an old transaction
  INSERT INTO transactions (account_id, merchant_id, amount, txn_time, txn_type, status, location_city, location_country)
  VALUES (v_account_id, 1, 50.00, v_time - INTERVAL '65 days', 'debit', 'success', 'Mumbai', 'India');

  -- Add the spike
  INSERT INTO transactions (account_id, merchant_id, amount, txn_time, txn_type, status, location_city, location_country)
  VALUES (v_account_id, 1, 15000.00, v_time - INTERVAL '1 day', 'debit', 'success', 'Mumbai', 'India');
END $$;

-- Rule 6: ROUND AMOUNT CLUSTERING (3+ txns with amount % 1000 = 0 in 24 hours)
DO $$
DECLARE
  v_account_id INT;
  v_time TIMESTAMPTZ := CURRENT_TIMESTAMP - INTERVAL '25 days';
BEGIN
  SELECT account_id INTO v_account_id FROM accounts OFFSET 5 LIMIT 1;
  FOR i IN 1..4 LOOP
    INSERT INTO transactions (account_id, merchant_id, amount, txn_time, txn_type, status, location_city, location_country)
    VALUES (v_account_id, 1, 2000.00, v_time + (INTERVAL '2 hours' * i), 'debit', 'success', 'Mumbai', 'India');
  END LOOP;
END $$;
