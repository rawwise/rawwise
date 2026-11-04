SELECT
    s.s_state,
    COUNT(*) AS sales_count,
    SUM(ss.ss_sales_price) AS total_sales
FROM store_sales ss
JOIN store s ON ss.ss_store_sk = s.s_store_sk
GROUP BY s.s_state
ORDER BY total_sales DESC
