SELECT
    d.d_year,
    d.d_month_seq,
    SUM(ss.ss_sales_price) AS monthly_sales,
    LAG(SUM(ss.ss_sales_price)) OVER (ORDER BY d.d_year, d.d_month_seq) AS prev_month_sales,
    SUM(ss.ss_sales_price) - LAG(SUM(ss.ss_sales_price)) OVER (ORDER BY d.d_year, d.d_month_seq) AS sales_change
FROM store_sales ss
JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
WHERE d.d_year = 2000
GROUP BY d.d_year, d.d_month_seq
ORDER BY d.d_year, d.d_month_seq
