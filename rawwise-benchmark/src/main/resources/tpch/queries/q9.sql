SELECT
    EXTRACT(YEAR FROM l.l_shipdate) AS l_year,
    SUM(l.l_extendedprice) AS revenue
FROM lineitem l
WHERE l.l_returnflag = 'R'
GROUP BY EXTRACT(YEAR FROM l.l_shipdate)
ORDER BY l_year
