SELECT
    o.o_orderstatus,
    l.l_returnflag,
    SUM(l.l_extendedprice) AS revenue,
    COUNT(*) AS item_count
FROM orders o
JOIN lineitem l ON o.o_orderkey = l.l_orderkey
WHERE l.l_shipdate >= DATE '1993-10-01'
AND l.l_shipdate < DATE '1994-01-01'
GROUP BY o.o_orderstatus, l.l_returnflag
ORDER BY o.o_orderstatus, l.l_returnflag
