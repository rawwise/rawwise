SELECT
    l.l_returnflag,
    l.l_linestatus,
    SUM(l.l_extendedprice) AS revenue
FROM lineitem l
WHERE l.l_shipdate >= DATE '1995-01-01'
AND l.l_shipdate < DATE '1995-04-01'
AND (
    (l.l_returnflag = 'A' AND l.l_quantity >= 1 AND l.l_quantity <= 20)
    OR (l.l_returnflag = 'R' AND l.l_quantity >= 10 AND l.l_quantity <= 30)
    OR (l.l_linestatus = 'O' AND l.l_quantity >= 20 AND l.l_quantity <= 40)
)
GROUP BY l.l_returnflag, l.l_linestatus
ORDER BY l.l_returnflag, l.l_linestatus
