WITH frame_r AS MATERIALIZED (SELECT frame_r__t0.ID AS id, frame_r__t0.NAME AS name
  FROM T AS frame_r__t0
  ORDER BY frame_r__t0.ID NULLS LAST), __a_0 AS (SELECT CAST(NULL AS VARCHAR) AS __c, ROW_NUMBER() OVER (ORDER BY w.__ord NULLS LAST) AS __rn, CAST(NULL AS DOUBLE) AS __v
  FROM (
    SELECT frame_r_t0.id AS id, frame_r_t0.name AS name, ROW_NUMBER() OVER (ORDER BY frame_r_t0.id NULLS LAST) AS __ord
    FROM frame_r AS frame_r_t0
    ORDER BY frame_r_t0.id NULLS LAST
  ) AS w), __p_0 AS (SELECT c.__n AS __c_n, n.__c AS __n_c, n.value AS __n_value
  FROM (
    SELECT COUNT(__a.__rn) AS __n
    FROM __a_0 AS __a
  ) AS c
  CROSS JOIN (
    SELECT CAST(value AS VARCHAR) AS __c, value AS value
    FROM (
      SELECT 1 AS __one
    ) AS __one
    LEFT OUTER JOIN (
      SELECT 3 AS value
    ) AS side ON TRUE
  ) AS n), __e_1 AS (SELECT value AS __o, ROW_NUMBER() OVER () AS __rn
  FROM (
    SELECT 'a' AS value
  ) AS side
  WHERE value IS NOT NULL), __a_1 AS (SELECT value AS __o, ROW_NUMBER() OVER () AS __rn
  FROM (
    SELECT (SELECT t1.name FROM ( SELECT frame_r_t0.id AS id, frame_r_t0.name AS name FROM frame_r AS frame_r_t0 ORDER BY frame_r_t0.id NULLS LAST LIMIT 1 ) AS t1) AS value
  ) AS side
  WHERE value IS NOT NULL)
SELECT 0 AS __m, coalesce((p.__c_n IS NOT DISTINCT FROM CAST(p.__n_c AS BIGINT)), FALSE) AS __s0, CAST(CAST(p.__n_c AS BIGINT) AS VARCHAR) AS __s1, CAST(p.__c_n AS VARCHAR) AS __s2, CAST(NULL AS VARCHAR) AS __s3, FALSE AS __s4, coalesce((p.__c_n IS NOT DISTINCT FROM CAST(p.__n_c AS BIGINT)), FALSE) AS __s5, concat('expected size ', CAST(CAST(p.__n_c AS BIGINT) AS VARCHAR), ', got ', CAST(p.__c_n AS VARCHAR)) AS __s6, NULL AS __s7, NULL AS __s8
FROM __p_0 AS p
UNION ALL
SELECT 1 AS __m, w.__h AS __s0, CAST(NULL AS VARCHAR) AS __s1, CAST(NULL AS VARCHAR) AS __s2, CAST(NULL AS VARCHAR) AS __s3, CAST(NULL AS BOOLEAN) AS __s4, CAST(NULL AS BOOLEAN) AS __s5, CAST(NULL AS VARCHAR) AS __s6, CASE WHEN w.__h THEN NULL ELSE w.__x0 END AS __s7, CASE WHEN w.__h THEN NULL ELSE w.__x1 END AS __s8
FROM (
  SELECT (list_sort(e.__l) IS NOT DISTINCT FROM list_sort(a.__l)) AS __h, e.__l AS __x0, a.__l AS __x1
  FROM (
    SELECT LIST(__a.__o ORDER BY __a.__rn ASC) AS __l
    FROM __a_1 AS __a
  ) AS a
  CROSS JOIN (
    SELECT LIST(__e.__o ORDER BY __e.__rn ASC) AS __l
    FROM __e_1 AS __e
  ) AS e
) AS w
