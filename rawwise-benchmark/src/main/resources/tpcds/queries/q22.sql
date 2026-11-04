SELECT
    i.i_item_id,
    SUM(ss.ss_quantity) AS total_qty,
    SUM(ss.ss_sales_price) AS total_sales,
    SUM(ss.ss_net_profit) AS total_profit
FROM store_sales ss
JOIN item i ON ss.ss_item_sk = i.i_item_sk
WHERE NOT EXISTS (
    SELECT 1 FROM store_sales ss2
    WHERE ss2.ss_item_sk = ss.ss_item_sk
    AND ss2.ss_net_profit < 0
)
GROUP BY i.i_item_id
ORDER BY total_sales DESC
LIMIT 50
