WITH __e_0 AS (SELECT value AS __o, ROW_NUMBER() OVER () AS __rn
  FROM (
    SELECT 3 AS value
  ) AS side
  WHERE value IS NOT NULL)
SELECT q.__same AS m0__verdict, CASE WHEN q.__same THEN NULL ELSE e.__l END AS m0__e, CASE WHEN q.__same THEN NULL ELSE a.__l END AS m0__a
FROM (
  SELECT LIST(__a.__o ORDER BY __a.__rn ASC) AS __l
  FROM __e_0 AS __a
) AS a
CROSS JOIN (
  SELECT LIST(__e.__o ORDER BY __e.__rn ASC) AS __l
  FROM __e_0 AS __e
) AS e
CROSS JOIN (
  SELECT ce.__k = ca.__k AND cm.__k = ce.__k AS __same
  FROM (
    SELECT COUNT(__e.__rn) AS __k
    FROM __e_0 AS __e
  ) AS ce
  CROSS JOIN (
    SELECT COUNT(__a.__rn) AS __k
    FROM __e_0 AS __a
  ) AS ca
  CROSS JOIN (
    SELECT COUNT(x.__rn) AS __k
    FROM __e_0 AS x
    JOIN __e_0 AS y ON x.__rn = y.__rn AND (x.__o IS NOT DISTINCT FROM y.__o)
  ) AS cm
) AS q
