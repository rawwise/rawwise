SELECT
    i.i_item_id,
    i.i_current_price,
    SUM(ss.ss_quantity) AS total_qty
FROM store_sales ss
JOIN item i ON ss.ss_item_sk = i.i_item_sk
WHERE i.i_current_price > 100.0
GROUP BY i.i_item_id, i.i_current_price
ORDER BY total_qty DESC
LIMIT 50
