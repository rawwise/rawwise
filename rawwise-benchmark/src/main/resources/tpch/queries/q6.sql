SELECT
    SUM(l.l_extendedprice * 0.05) AS revenue
FROM lineitem l
WHERE l.l_shipdate >= DATE '1994-01-01'
AND l.l_shipdate < DATE '1995-01-01'
AND l.l_quantity < 24
