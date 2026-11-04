SELECT
    i.i_item_id,
    SUM(ss.ss_sales_price) AS total_sales,
    SUM(ss.ss_net_profit) AS total_profit,
    SUM(ss.ss_net_profit) / SUM(ss.ss_sales_price) AS profit_ratio
FROM store_sales ss
JOIN item i ON ss.ss_item_sk = i.i_item_sk
WHERE i.i_current_price > 50.0
GROUP BY i.i_item_id
ORDER BY profit_ratio DESC
LIMIT 50
