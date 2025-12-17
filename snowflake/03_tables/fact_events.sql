CREATE OR REPLACE TABLE FACT_EVENTS (
  event_id STRING,
  session_id STRING,
  event_timestamp TIMESTAMP,
  event_type STRING,
  product_id STRING,
  qty INTEGER,
  cart_size INTEGER,
  payment STRING,
  discount_pct NUMBER(5,2),
  amount_usd NUMBER(10,2)
);
