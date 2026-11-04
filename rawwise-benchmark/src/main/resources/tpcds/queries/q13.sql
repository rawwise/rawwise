SELECT
    c.c_birth_month,
    COUNT(*) AS sales_count,
    SUM(ss.ss_sales_price) AS total_sales
FROM store_sales ss
JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
GROUP BY c.c_birth_month
ORDER BY c.c_birth_month
