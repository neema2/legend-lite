SELECT t6.firm AS firm, t6.city AS city, t6.headcount AS headcount, t6.avgAge AS avgAge, t6.youngest AS youngest, t6.oldest AS oldest, CASE WHEN t6.avgAge IS NOT NULL AND t6.avgAge >= 32 THEN 'seasoned' ELSE 'young' END AS band
FROM (
  SELECT t3.p_legalname AS firm, t3.HQ_p_name AS city, COUNT(1) AS headcount, AVG(1.0 * t0.p_age) AS avgAge, MIN(t0.p_age) AS youngest, MAX(t0.p_age) AS oldest
  FROM n_person AS t0
  LEFT OUTER JOIN (
    SELECT t1.id AS id, t1.p_legalname AS p_legalname, t1.r_hq AS r_hq, t2.p_name AS HQ_p_name
    FROM n_firm AS t1
    LEFT OUTER JOIN n_city AS t2 ON t1.r_hq = t2.id
  ) AS t3 ON t0.r_works_at = t3.id
  WHERE EXISTS (SELECT * FROM n_firm AS t4 WHERE t0.r_works_at = t4.id) AND EXISTS (SELECT * FROM n_city AS t5 WHERE t3.r_hq = t5.id) AND t0.p_age IS NOT NULL AND t3.HQ_p_name IS NOT NULL AND (t3.HQ_p_name IS DISTINCT FROM 'Nowhere')
  GROUP BY t3.p_legalname, t3.HQ_p_name
  HAVING COUNT(1) >= 1
) AS t6
ORDER BY t6.headcount DESC NULLS FIRST, t6.firm NULLS LAST
