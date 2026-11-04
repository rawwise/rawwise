SELECT
    d.d_day_of_week,
    AVG(ss.ss_sales_price) AS avg_sale_price,
    AVG(ss.ss_quantity) AS avg_qty
FROM store_sales ss
JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
WHERE d.d_year = 2000
GROUP BY d.d_day_of_week
ORDER BY d.d_day_of_week
