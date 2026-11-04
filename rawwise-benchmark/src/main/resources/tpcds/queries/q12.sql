SELECT
    d.d_year,
    SUM(CASE WHEN c.c_gender = 'M' THEN ss.ss_sales_price ELSE 0 END) AS male_sales,
    SUM(CASE WHEN c.c_gender = 'F' THEN ss.ss_sales_price ELSE 0 END) AS female_sales,
    SUM(ss.ss_sales_price) AS total_sales
FROM store_sales ss
JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
GROUP BY d.d_year
ORDER BY d.d_year
