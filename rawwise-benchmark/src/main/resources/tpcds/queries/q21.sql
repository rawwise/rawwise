SELECT
    d.d_year,
    SUM(ss.ss_net_profit) AS total_profit
FROM store_sales ss
JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
WHERE ss.ss_store_sk IN (
    SELECT s_store_sk FROM store WHERE s_state = 'A'
)
GROUP BY d.d_year
ORDER BY d.d_year
