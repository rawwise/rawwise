SELECT
    d_year,
    d_month_seq,
    COUNT(*) AS cnt,
    SUM(ss_quantity) AS total_qty,
    SUM(ss_sales_price) AS total_sales,
    SUM(ss_net_profit) AS total_profit
FROM store_sales ss
JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
WHERE d_year = 2000
GROUP BY d_year, d_month_seq
ORDER BY d_year, d_month_seq
