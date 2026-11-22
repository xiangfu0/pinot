SELECT /*+ joinOptions(left_distribution_type='random', right_distribution_type='broadcast') */
       c.k2, p.k2, COUNT(*), SUM(f.amount)
FROM fact f JOIN customerDim c ON f.k1=c.k1
JOIN productDim p ON f.k2=p.k1
GROUP BY c.k2,p.k2 LIMIT 1000
