WITH frame_r AS MATERIALIZED (SELECT frame_r__t0.ID AS id, frame_r__t0.NAME AS name
  FROM T AS frame_r__t0
  ORDER BY frame_r__t0.ID NULLS LAST), __e_0 AS (SELECT value AS __o, ROW_NUMBER() OVER () AS __rn
  FROM (
    SELECT 'a' AS value
  ) AS side
  WHERE value IS NOT NULL), __a_0 AS (SELECT value AS __o, ROW_NUMBER() OVER () AS __rn
  FROM (
    SELECT (SELECT t1.name FROM ( SELECT frame_r_t0.id AS id, frame_r_t0.name AS name FROM frame_r AS frame_r_t0 ORDER BY frame_r_t0.id NULLS LAST LIMIT 1 ) AS t1) AS value
  ) AS side
  WHERE value IS NOT NULL)
SELECT q.__same AS m0__verdict, CASE WHEN q.__same THEN NULL ELSE e.__l END AS m0__e, CASE WHEN q.__same THEN NULL ELSE a.__l END AS m0__a
FROM (
  SELECT LIST(__a.__o ORDER BY __a.__rn ASC) AS __l
  FROM __a_0 AS __a
) AS a
CROSS JOIN (
  SELECT LIST(__e.__o ORDER BY __e.__rn ASC) AS __l
  FROM __e_0 AS __e
) AS e
CROSS JOIN (
  SELECT d.__k = 0 AS __same
  FROM (
    SELECT COUNT(*) AS __k
    FROM (
      SELECT u.__o AS __o
      FROM (
        SELECT __e.__o AS __o, 1 AS __g
        FROM __e_0 AS __e
        UNION ALL
        SELECT __a.__o AS __o, -1 AS __g
        FROM __a_0 AS __a
      ) AS u
      GROUP BY u.__o
      HAVING SUM(u.__g) <> 0
    ) AS h
  ) AS d
) AS q
