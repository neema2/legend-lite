WITH frame_r AS MATERIALIZED (SELECT frame_r__t0.ID AS id, frame_r__t0.NAME AS name
  FROM T AS frame_r__t0
  ORDER BY frame_r__t0.ID NULLS LAST), __v0_0 AS (SELECT value AS __o, ROW_NUMBER() OVER () AS __rn
  FROM (
    SELECT 3 AS value
  ) AS side
  WHERE value IS NOT NULL), __v1_0 AS (SELECT value AS __o, ROW_NUMBER() OVER () AS __rn
  FROM (
    SELECT (SELECT COUNT(*) FROM ( SELECT 1 AS cnt FROM frame_r AS frame_r_t0 ORDER BY frame_r_t0.id NULLS LAST ) AS t1) AS value
  ) AS side
  WHERE value IS NOT NULL)
SELECT 0 AS __m, v1.__v = v0.__v AS __s0, CASE WHEN v1.__v = v0.__v THEN NULL ELSE v0.__v END AS __s1, CASE WHEN v1.__v = v0.__v THEN NULL ELSE v1.__v END AS __s2
FROM (
  SELECT MIN(__v0.__o) AS __v
  FROM __v0_0 AS __v0
) AS v0
CROSS JOIN (
  SELECT MIN(__v1.__o) AS __v
  FROM __v1_0 AS __v1
) AS v1
