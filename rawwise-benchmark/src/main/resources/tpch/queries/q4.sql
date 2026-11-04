SELECT
    o.o_orderstatus,
    COUNT(*) AS order_count
FROM orders o
WHERE EXISTS (
    SELECT 1 FROM lineitem l
    WHERE l.l_orderkey = o.o_orderkey
    AND l.l_shipdate < DATE '1995-01-01'
)
GROUP BY o.o_orderstatus
ORDER BY o.o_orderstatus
