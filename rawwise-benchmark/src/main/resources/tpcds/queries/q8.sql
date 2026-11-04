SELECT
    EXTRACT(MONTH FROM d.d_date) AS sale_month,
    COUNT(DISTINCT ss.ss_customer_sk) AS unique_customers,
    SUM(ss.ss_sales_price) AS total_sales
FROM store_sales ss
JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
WHERE d.d_year = 2000
GROUP BY EXTRACT(MONTH FROM d.d_date)
ORDER BY sale_month
