WITH frame_r AS MATERIALIZED (SELECT frame_r__t0.*
  FROM T AS frame_r__t0), __p_0 AS (SELECT c.__n AS __c_n, n.__c AS __n_c, n.value AS __n_value
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
  ) AS n)
SELECT coalesce((p.__c_n IS NOT DISTINCT FROM CAST(p.__n_c AS BIGINT)), FALSE) AS m0__verdict, CAST(CAST(p.__n_c AS BIGINT) AS VARCHAR) AS m0__expected, CAST(p.__c_n AS VARCHAR) AS m0__actual, CAST(NULL AS VARCHAR) AS m0__unjudged, FALSE AS m0__lenient, coalesce((p.__c_n IS NOT DISTINCT FROM CAST(p.__n_c AS BIGINT)), FALSE) AS m0__holds, concat('expected ', coalesce(CAST(CAST(p.__n_c AS BIGINT) AS VARCHAR), '[]'), ', got ', coalesce(CAST(p.__c_n AS VARCHAR), '[]')) AS m0__message
FROM __p_0 AS p
