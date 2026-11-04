SELECT
    d.d_year,
    s.s_state,
    COUNT(DISTINCT i.i_item_sk) AS item_count,
    SUM(ss.ss_quantity) AS total_qty
FROM store_sales ss
JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
JOIN store s ON ss.ss_store_sk = s.s_store_sk
JOIN item i ON ss.ss_item_sk = i.i_item_sk
WHERE d.d_year BETWEEN 1999 AND 2001
GROUP BY d.d_year, s.s_state
ORDER BY d.d_year, s.s_state
