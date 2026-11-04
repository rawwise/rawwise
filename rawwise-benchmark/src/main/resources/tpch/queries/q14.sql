SELECT
    100.00 * SUM(CASE WHEN l.l_returnflag = 'R' THEN l.l_extendedprice ELSE 0 END)
        / SUM(l.l_extendedprice) AS promo_revenue
FROM lineitem l
WHERE l.l_shipdate >= DATE '1995-09-01'
AND l.l_shipdate < DATE '1995-10-01'
