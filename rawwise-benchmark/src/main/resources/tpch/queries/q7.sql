SELECT
    l.l_returnflag,
    l.l_linestatus,
    SUM(l.l_quantity) AS sum_qty,
    SUM(l.l_extendedprice) AS sum_base_price,
    AVG(l.l_quantity) AS avg_qty
FROM lineitem l
WHERE l.l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
GROUP BY l.l_returnflag, l.l_linestatus
ORDER BY l.l_returnflag, l.l_linestatus
