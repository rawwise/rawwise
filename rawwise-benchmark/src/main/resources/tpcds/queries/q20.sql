SELECT
    s.s_state,
    SUM(ss.ss_sales_price) AS total_sales,
    RANK() OVER (ORDER BY SUM(ss.ss_sales_price) DESC) AS sales_rank
FROM store_sales ss
JOIN store s ON ss.ss_store_sk = s.s_store_sk
JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
WHERE d.d_year = 2000
GROUP BY s.s_state
ORDER BY sales_rank
