SELECT
    c.c_gender,
    COUNT(*) AS sales_count,
    AVG(ss.ss_sales_price) AS avg_sale_price
FROM store_sales ss
JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
GROUP BY c.c_gender
ORDER BY c.c_gender
