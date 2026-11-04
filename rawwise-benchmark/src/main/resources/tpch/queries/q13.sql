SELECT
    o.o_orderstatus,
    COUNT(*) AS order_count
FROM orders o
WHERE NOT EXISTS (
    SELECT 1 FROM lineitem l
    WHERE l.l_orderkey = o.o_orderkey
    AND l.l_returnflag = 'R'
)
GROUP BY o.o_orderstatus
ORDER BY order_count DESC
