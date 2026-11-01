WITH frame_r AS MATERIALIZED (SELECT frame_r__t0.*
  FROM T AS frame_r__t0), frame_s AS MATERIALIZED (SELECT frame_s__t0.*
  FROM T AS frame_s__t0
  WHERE frame_s__t0.AMOUNT > 1.0), __p_0 AS (SELECT c.__n AS __c_n, n.__c AS __n_c, n.value AS __n_value
  FROM (
    SELECT COUNT(*) AS __n
    FROM frame_r AS frame_r_t0
  ) AS c
  CROSS JOIN (
    SELECT CAST(value AS VARCHAR) AS __c, value AS value
    FROM (
      SELECT 1 AS __one
    ) AS __one
    LEFT OUTER JOIN (
      SELECT 3 AS value
    ) AS side ON TRUE
  ) AS n), __p_1 AS (SELECT c.__n AS __c_n, n.__c AS __n_c, n.value AS __n_value
  FROM (
    SELECT COUNT(*) AS __n
    FROM frame_s AS frame_s_t0
  ) AS c
  CROSS JOIN (
    SELECT CAST(value AS VARCHAR) AS __c, value AS value
    FROM (
      SELECT 1 AS __one
    ) AS __one
    LEFT OUTER JOIN (
      SELECT 2 AS value
    ) AS side ON TRUE
  ) AS n)
SELECT 0 AS __ix, coalesce((p.__c_n IS NOT DISTINCT FROM CAST(p.__n_c AS BIGINT)), FALSE) AS __verdict, CAST(CAST(p.__n_c AS BIGINT) AS VARCHAR) AS __expected, CAST(p.__c_n AS VARCHAR) AS __actual, CAST(NULL AS VARCHAR) AS __unjudged, FALSE AS __lenient, coalesce((p.__c_n IS NOT DISTINCT FROM CAST(p.__n_c AS BIGINT)), FALSE) AS __holds, concat('assertSize
expected: ', coalesce(CAST(CAST(p.__n_c AS BIGINT) AS VARCHAR), 'null'), '
actual:   ', coalesce(CAST(p.__c_n AS VARCHAR), 'null')) AS __message
FROM __p_0 AS p
UNION ALL
SELECT 1 AS __ix, coalesce((p.__c_n IS NOT DISTINCT FROM CAST(p.__n_c AS BIGINT)), FALSE) AS __verdict, CAST(CAST(p.__n_c AS BIGINT) AS VARCHAR) AS __expected, CAST(p.__c_n AS VARCHAR) AS __actual, CAST(NULL AS VARCHAR) AS __unjudged, FALSE AS __lenient, coalesce((p.__c_n IS NOT DISTINCT FROM CAST(p.__n_c AS BIGINT)), FALSE) AS __holds, concat('assertSize
expected: ', coalesce(CAST(CAST(p.__n_c AS BIGINT) AS VARCHAR), 'null'), '
actual:   ', coalesce(CAST(p.__c_n AS VARCHAR), 'null')) AS __message
FROM __p_1 AS p
