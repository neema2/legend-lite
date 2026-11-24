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
SELECT 0 AS __m, w.__h AS __s0, CASE WHEN w.__h THEN NULL ELSE w.__x0 END AS __s1, CASE WHEN w.__h THEN NULL ELSE w.__x1 END AS __s2
FROM (
  SELECT (e.__l IS NOT DISTINCT FROM a.__l) AS __h, e.__l AS __x0, a.__l AS __x1
  FROM (
    SELECT LIST(__a.__o ORDER BY __a.__rn ASC) AS __l
    FROM __a_0 AS __a
  ) AS a
  CROSS JOIN (
    SELECT LIST(__e.__o ORDER BY __e.__rn ASC) AS __l
    FROM __e_0 AS __e
  ) AS e
) AS w
