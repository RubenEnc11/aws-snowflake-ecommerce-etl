SELECT
  o.order_id,
  o.total_usd,
  SUM(i.line_total_usd) AS items_total
FROM FACT_ORDERS o
JOIN FACT_ORDER_ITEMS i
  ON o.order_id = i.order_id
GROUP BY o.order_id, o.total_usd
HAVING ABS(o.total_usd - SUM(i.line_total_usd)) > 0.01;