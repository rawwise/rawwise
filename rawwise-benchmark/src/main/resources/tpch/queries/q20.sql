SELECT
    l.l_orderkey,
    SUM(l.l_quantity) AS total_qty,
    SUM(l.l_extendedprice) AS total_revenue
FROM lineitem l
WHERE l.l_shipdate >= DATE '1994-01-01'
AND l.l_shipdate < DATE '1995-01-01'
AND EXISTS (
    SELECT 1 FROM lineitem l2
    WHERE l2.l_orderkey = l.l_orderkey
    AND l2.l_quantity > 50
)
GROUP BY l.l_orderkey
ORDER BY total_revenue DESC
LIMIT 100
