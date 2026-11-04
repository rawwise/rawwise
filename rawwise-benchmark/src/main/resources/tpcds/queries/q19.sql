SELECT
    d.d_year,
    SUM(ss.ss_sales_price) AS yearly_sales,
    SUM(SUM(ss.ss_sales_price)) OVER (ORDER BY d.d_year) AS cumulative_sales
FROM store_sales ss
JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
WHERE d.d_year BETWEEN 1998 AND 2002
GROUP BY d.d_year
ORDER BY d.d_year
