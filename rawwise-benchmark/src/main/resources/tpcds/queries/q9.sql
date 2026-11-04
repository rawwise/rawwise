SELECT
    c.c_gender,
    d.d_year,
    SUM(ss.ss_quantity) AS total_qty,
    SUM(ss.ss_net_profit) AS total_profit
FROM store_sales ss
JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
GROUP BY c.c_gender, d.d_year
ORDER BY c.c_gender, d.d_year
