WITH __e_0 AS (SELECT value AS __o, ROW_NUMBER() OVER () AS __rn
  FROM (
    SELECT 'a' AS value
  ) AS side
  WHERE value IS NOT NULL)
SELECT 0 AS __m, w.__h AS __s0, CASE WHEN w.__h THEN NULL ELSE w.__x0 END AS __s1, CASE WHEN w.__h THEN NULL ELSE w.__x1 END AS __s2
FROM (
  SELECT (a.__v IS NOT DISTINCT FROM e.__v) AS __h, e.__v AS __x0, a.__v AS __x1
  FROM (
    SELECT MIN(__a.__o) AS __v
    FROM __e_0 AS __a
  ) AS a
  CROSS JOIN (
    SELECT MIN(__e.__o) AS __v
    FROM __e_0 AS __e
  ) AS e
) AS w
