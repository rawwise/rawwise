SELECT
    o.o_orderkey,
    o.o_orderstatus,
    SUM(l.l_extendedprice) AS total_price
FROM orders o
JOIN lineitem l ON o.o_orderkey = l.l_orderkey
GROUP BY o.o_orderkey, o.o_orderstatus
ORDER BY total_price DESC
LIMIT 100
