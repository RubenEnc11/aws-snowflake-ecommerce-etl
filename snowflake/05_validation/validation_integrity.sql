SELECT COUNT(*) AS null_customers_pk
FROM DIM_CUSTOMERS
WHERE customer_id IS NULL;