CREATE OR REPLACE TABLE FACT_ORDERS (
  order_id STRING,
  customer_id STRING,
  order_time TIMESTAMP,
  payment_method STRING,
  discount_pct NUMBER(5,2),
  subtotal_usd NUMBER(10,2),
  total_usd NUMBER(10,2),
  country STRING,
  device STRING,
  source STRING
);
