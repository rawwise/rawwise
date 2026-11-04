SELECT
    o.o_orderstatus,
    SUM(CASE WHEN l.l_returnflag = 'R' THEN l.l_extendedprice ELSE 0 END) AS returned_revenue,
    SUM(l.l_extendedprice) AS total_revenue,
    SUM(CASE WHEN l.l_returnflag = 'R' THEN l.l_extendedprice ELSE 0 END) / SUM(l.l_extendedprice) AS return_ratio
FROM orders o
JOIN lineitem l ON o.o_orderkey = l.l_orderkey
WHERE l.l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
GROUP BY o.o_orderstatus
ORDER BY o.o_orderstatus
