CREATE OR REPLACE TABLE FACT_ORDER_ITEMS (
  order_id STRING,
  product_id STRING,
  unit_price_usd NUMBER(10,2),
  quantity INTEGER,
  line_total_usd NUMBER(10,2)
);
