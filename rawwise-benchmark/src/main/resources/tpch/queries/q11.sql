SELECT
    l.l_returnflag,
    SUM(l.l_extendedprice) AS total_revenue
FROM lineitem l
WHERE l.l_shipdate >= DATE '1994-01-01'
AND l.l_shipdate < DATE '1995-01-01'
GROUP BY l.l_returnflag
HAVING SUM(l.l_extendedprice) > (
    SELECT AVG(total_rev) FROM (
        SELECT SUM(l2.l_extendedprice) AS total_rev
        FROM lineitem l2
        WHERE l2.l_shipdate >= DATE '1994-01-01'
        AND l2.l_shipdate < DATE '1995-01-01'
        GROUP BY l2.l_returnflag
    )
)
ORDER BY total_revenue DESC
