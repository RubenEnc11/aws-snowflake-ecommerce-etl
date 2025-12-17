CREATE OR REPLACE VIEW VW_TOP_PRODUCTS_REVENUE AS
SELECT
  p.product_id,
  p.name,
  SUM(i.line_total_usd) AS revenue
FROM FACT_ORDER_ITEMS i
JOIN DIM_PRODUCTS p
  ON i.product_id = p.product_id
GROUP BY p.product_id, p.name;
