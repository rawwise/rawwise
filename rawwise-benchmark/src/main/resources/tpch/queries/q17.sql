SELECT
    AVG(l.l_quantity) AS avg_quantity,
    SUM(l.l_extendedprice) AS total_revenue
FROM lineitem l
WHERE l.l_quantity < (
    SELECT AVG(l2.l_quantity) * 0.2
    FROM lineitem l2
    WHERE l2.l_returnflag = l.l_returnflag
)
