WITH frame_r AS MATERIALIZED (SELECT frame_r__t0.ID AS id, frame_r__t0.NAME AS name
  FROM T AS frame_r__t0
  ORDER BY frame_r__t0.ID NULLS LAST), __a_0 AS (SELECT CAST(NULL AS VARCHAR) AS __c, ROW_NUMBER() OVER (ORDER BY w.__ord NULLS LAST) AS __rn, CAST(NULL AS DOUBLE) AS __v
  FROM (
    SELECT frame_r_t0.id AS id, frame_r_t0.name AS name, ROW_NUMBER() OVER (ORDER BY frame_r_t0.id NULLS LAST) AS __ord
    FROM frame_r AS frame_r_t0
    ORDER BY frame_r_t0.id NULLS LAST
  ) AS w), __n_0 AS (SELECT value AS __o, ROW_NUMBER() OVER () AS __rn
  FROM (
    SELECT 3 AS value
  ) AS side
  WHERE value IS NOT NULL), __e_1 AS (SELECT value AS __o, ROW_NUMBER() OVER () AS __rn
  FROM (
    SELECT 'a' AS value
  ) AS side
  WHERE value IS NOT NULL), __a_1 AS (SELECT value AS __o, ROW_NUMBER() OVER () AS __rn
  FROM (
    SELECT (SELECT t1.name FROM ( SELECT frame_r_t0.id AS id, frame_r_t0.name AS name FROM frame_r AS frame_r_t0 ORDER BY frame_r_t0.id NULLS LAST LIMIT 1 ) AS t1) AS value
  ) AS side
  WHERE value IS NOT NULL)
SELECT 0 AS __m, (k.__k IS NOT DISTINCT FROM CAST(n.__v AS BIGINT)) AS __s0, CASE WHEN (k.__k IS NOT DISTINCT FROM CAST(n.__v AS BIGINT)) THEN NULL ELSE CAST(n.__v AS BIGINT) END AS __s1, CASE WHEN (k.__k IS NOT DISTINCT FROM CAST(n.__v AS BIGINT)) THEN NULL ELSE k.__k END AS __s2, NULL AS __s3, NULL AS __s4
FROM (
  SELECT COUNT(__a.__rn) AS __k
  FROM __a_0 AS __a
) AS k
CROSS JOIN (
  SELECT MIN(__n.__o) AS __v
  FROM __n_0 AS __n
) AS n
UNION ALL
SELECT 1 AS __m, w.__h AS __s0, CAST(NULL AS BIGINT) AS __s1, CAST(NULL AS BIGINT) AS __s2, CASE WHEN w.__h THEN NULL ELSE w.__x0 END AS __s3, CASE WHEN w.__h THEN NULL ELSE w.__x1 END AS __s4
FROM (
  SELECT (e.__l IS NOT DISTINCT FROM a.__l) AS __h, e.__l AS __x0, a.__l AS __x1
  FROM (
    SELECT LIST(__a.__o ORDER BY __a.__rn ASC) AS __l
    FROM __a_1 AS __a
  ) AS a
  CROSS JOIN (
    SELECT LIST(__e.__o ORDER BY __e.__rn ASC) AS __l
    FROM __e_1 AS __e
  ) AS e
) AS w
