SELECT
    l.l_returnflag,
    SUM(l.l_extendedprice) AS base_price,
    AVG(l.l_quantity) AS avg_qty
FROM lineitem l
WHERE l.l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1995-12-31'
GROUP BY l.l_returnflag
ORDER BY l.l_returnflag
