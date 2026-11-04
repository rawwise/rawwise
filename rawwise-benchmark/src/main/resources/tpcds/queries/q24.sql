SELECT
    s.s_store_id,
    SUM(ss.ss_quantity) AS total_qty,
    SUM(ss.ss_sales_price) AS total_sales,
    SUM(ss.ss_net_profit) AS total_profit
FROM store_sales ss
JOIN store s ON ss.ss_store_sk = s.s_store_sk
JOIN item i ON ss.ss_item_sk = i.i_item_sk
WHERE i.i_current_price BETWEEN 10.0 AND 100.0
GROUP BY s.s_store_id
ORDER BY total_profit DESC
LIMIT 50
