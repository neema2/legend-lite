WITH frame_r AS MATERIALIZED (SELECT frame_r__t0.ID AS id, frame_r__t0.NAME AS name
  FROM T AS frame_r__t0
  ORDER BY frame_r__t0.ID NULLS LAST), __a_0 AS (SELECT CAST(NULL AS VARCHAR) AS __c, ROW_NUMBER() OVER () AS __rn, CAST(NULL AS DOUBLE) AS __v
  FROM (
    SELECT frame_r_t0.id AS id, frame_r_t0.name AS name
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
  ) AS n), __se_1 AS (SELECT coalesce(CAST(value AS VARCHAR), '[]') AS __text, CASE WHEN value IS NOT NULL AND CAST(value AS VARCHAR) IS NULL THEN 1 ELSE 0 END AS __nulls, CASE WHEN strpos(CAST(value AS VARCHAR), 'tree') > 0 THEN 1 ELSE 0 END AS __trees, CASE WHEN value IS NOT NULL THEN 1 ELSE 0 END AS __n
  FROM (
    SELECT 1 AS __one
  ) AS __one
  LEFT OUTER JOIN (
    SELECT 'a' AS value
  ) AS side ON TRUE), __sa_1 AS (SELECT coalesce(CAST(value AS VARCHAR), '[]') AS __text, CASE WHEN value IS NOT NULL AND CAST(value AS VARCHAR) IS NULL THEN 1 ELSE 0 END AS __nulls, CASE WHEN strpos(CAST(value AS VARCHAR), 'tree') > 0 THEN 1 ELSE 0 END AS __trees, CASE WHEN value IS NOT NULL THEN 1 ELSE 0 END AS __n
  FROM (
    SELECT 1 AS __one
  ) AS __one
  LEFT OUTER JOIN (
    SELECT (SELECT t1.name FROM ( SELECT frame_r_t0.id AS id, frame_r_t0.name AS name FROM frame_r AS frame_r_t0 ORDER BY frame_r_t0.id NULLS LAST LIMIT 1 ) AS t1) AS value
  ) AS side ON TRUE)
SELECT 0 AS __ix, coalesce((p.__c_n IS NOT DISTINCT FROM CAST(p.__n_c AS BIGINT)), FALSE) AS __verdict, CAST(CAST(p.__n_c AS BIGINT) AS VARCHAR) AS __expected, CAST(p.__c_n AS VARCHAR) AS __actual, CAST(NULL AS VARCHAR) AS __unjudged, FALSE AS __lenient
FROM __p_0 AS p
UNION ALL
SELECT 1 AS __ix, (e.__text IS NOT DISTINCT FROM a.__text) AS __verdict, e.__text AS __expected, a.__text AS __actual, CASE WHEN e.__nulls > 0 OR a.__nulls > 0 THEN 'null-canon-cell' WHEN e.__trees > 0 OR a.__trees > 0 THEN 'unclaimable tree cell' END AS __unjudged, FALSE AS __lenient
FROM __se_1 AS e
CROSS JOIN __sa_1 AS a
