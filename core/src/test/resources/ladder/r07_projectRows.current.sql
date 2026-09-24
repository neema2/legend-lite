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
  ) AS n)
SELECT 0 AS __ix, coalesce((p.__c_n IS NOT DISTINCT FROM CAST(p.__n_c AS BIGINT)), FALSE) AS __verdict, CAST(CAST(p.__n_c AS BIGINT) AS VARCHAR) AS __expected, CAST(p.__c_n AS VARCHAR) AS __actual, CAST(NULL AS VARCHAR) AS __unjudged, FALSE AS __lenient
FROM __p_0 AS p
