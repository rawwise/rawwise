SELECT
    o.o_orderkey,
    o.o_orderstatus,
    SUM(l.l_quantity) AS total_qty
FROM orders o
JOIN lineitem l ON o.o_orderkey = l.l_orderkey
GROUP BY o.o_orderkey, o.o_orderstatus
HAVING SUM(l.l_quantity) > 300
ORDER BY total_qty DESC
LIMIT 100
