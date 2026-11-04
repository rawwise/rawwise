SELECT
    i.i_item_id,
    s.s_store_id,
    SUM(ss.ss_quantity) AS total_qty,
    SUM(ss.ss_net_profit) AS total_profit
FROM store_sales ss
JOIN item i ON ss.ss_item_sk = i.i_item_sk
JOIN store s ON ss.ss_store_sk = s.s_store_sk
WHERE ss.ss_sold_date_sk IN (
    SELECT d_date_sk FROM date_dim WHERE d_year = 2000
)
GROUP BY i.i_item_id, s.s_store_id
ORDER BY total_profit DESC
LIMIT 100
