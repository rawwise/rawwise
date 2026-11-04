SELECT
    l.l_returnflag,
    l.l_linestatus,
    COUNT(DISTINCT l.l_orderkey) AS distinct_orders,
    SUM(l.l_quantity) AS total_qty,
    AVG(l.l_extendedprice) AS avg_price
FROM lineitem l
WHERE l.l_shipdate >= DATE '1995-01-01'
AND l.l_shipdate < DATE '1996-01-01'
GROUP BY l.l_returnflag, l.l_linestatus
ORDER BY l.l_returnflag, l.l_linestatus
