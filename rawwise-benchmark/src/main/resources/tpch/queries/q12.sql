SELECT
    l.l_linestatus,
    SUM(CASE WHEN l.l_shipdate > DATE '1997-01-01' THEN 1 ELSE 0 END) AS high_priority_count,
    SUM(CASE WHEN l.l_shipdate <= DATE '1997-01-01' THEN 1 ELSE 0 END) AS low_priority_count,
    SUM(l.l_quantity) AS total_qty
FROM lineitem l
WHERE l.l_returnflag IN ('A', 'R')
GROUP BY l.l_linestatus
ORDER BY l.l_linestatus
