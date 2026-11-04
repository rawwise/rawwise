SELECT
    o.o_orderkey,
    o.o_orderstatus,
    SUM(l.l_extendedprice) AS revenue,
    l.l_shipdate
FROM orders o
JOIN lineitem l ON o.o_orderkey = l.l_orderkey
WHERE l.l_shipdate > DATE '1995-03-15'
GROUP BY o.o_orderkey, o.o_orderstatus, l.l_shipdate
ORDER BY revenue DESC
LIMIT 10
