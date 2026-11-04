SELECT
    s.s_state,
    COUNT(DISTINCT ss.ss_customer_sk) AS customer_cnt,
    SUM(ss.ss_sales_price) AS total_sales,
    AVG(ss.ss_net_profit) AS avg_profit
FROM store_sales ss
JOIN store s ON ss.ss_store_sk = s.s_store_sk
JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
WHERE d.d_year = 2000
GROUP BY s.s_state
HAVING SUM(ss.ss_sales_price) > 10000
ORDER BY total_sales DESC
