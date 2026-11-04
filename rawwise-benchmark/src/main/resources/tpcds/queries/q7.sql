SELECT
    i.i_item_id,
    SUM(ss.ss_quantity) AS total_qty,
    SUM(ss.ss_sales_price) AS total_sales
FROM store_sales ss
JOIN item i ON ss.ss_item_sk = i.i_item_sk
JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
WHERE d.d_year = 2000
GROUP BY i.i_item_id
ORDER BY total_sales DESC
LIMIT 100
