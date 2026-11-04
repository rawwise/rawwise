SELECT
    l.l_returnflag,
    SUM(l.l_extendedprice) AS total_revenue
FROM lineitem l
WHERE l.l_shipdate >= DATE '1996-01-01'
AND l.l_shipdate < DATE '1996-04-01'
GROUP BY l.l_returnflag
ORDER BY total_revenue DESC
LIMIT 1
