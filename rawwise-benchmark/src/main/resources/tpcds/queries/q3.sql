SELECT
    d.d_year,
    d.d_month_seq,
    SUM(ss.ss_sales_price) AS monthly_sales,
    SUM(ss.ss_net_profit) AS monthly_profit
FROM store_sales ss
JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
WHERE d.d_year BETWEEN 1999 AND 2001
GROUP BY d.d_year, d.d_month_seq
ORDER BY d.d_year, d.d_month_seq
