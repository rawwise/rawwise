SELECT
    SUBSTRING(CAST(o.o_orderkey AS STRING), 1, 1) AS order_prefix,
    COUNT(*) AS order_count,
    SUM(l.l_extendedprice) AS total_revenue,
    AVG(l.l_quantity) AS avg_qty
FROM orders o
JOIN lineitem l ON o.o_orderkey = l.l_orderkey
WHERE l.l_shipdate >= DATE '1996-01-01'
AND l.l_shipdate < DATE '1997-01-01'
GROUP BY SUBSTRING(CAST(o.o_orderkey AS STRING), 1, 1)
ORDER BY total_revenue DESC
