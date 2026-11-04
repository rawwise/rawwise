SELECT
    o.o_orderstatus,
    COUNT(*) AS order_count,
    SUM(CASE WHEN l.l_returnflag = 'R' THEN 1 ELSE 0 END) AS returned_items,
    SUM(CASE WHEN l.l_linestatus = 'F' THEN 1 ELSE 0 END) AS final_items
FROM orders o
JOIN lineitem l ON o.o_orderkey = l.l_orderkey
WHERE NOT EXISTS (
    SELECT 1 FROM lineitem l2
    WHERE l2.l_orderkey = o.o_orderkey
    AND l2.l_shipdate > DATE '1997-01-01'
)
GROUP BY o.o_orderstatus
ORDER BY o.o_orderstatus
