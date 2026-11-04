SELECT
    d.d_year,
    c.c_gender,
    s.s_state,
    SUM(ss.ss_net_profit) AS total_profit
FROM store_sales ss
JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
JOIN store s ON ss.ss_store_sk = s.s_store_sk
WHERE d.d_year = 2000
GROUP BY d.d_year, c.c_gender, s.s_state
ORDER BY d.d_year, c.c_gender, s.s_state
