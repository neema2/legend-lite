WITH frame_r AS MATERIALIZED (SELECT frame_r__t0.*
  FROM T AS frame_r__t0), frame_s AS MATERIALIZED (SELECT frame_s__t0.*
  FROM T AS frame_s__t0
  WHERE frame_s__t0.AMOUNT > 1.0E0), __p_0 AS (SELECT c.__n AS __c_n, n.__c AS __n_c, n.value AS __n_value
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
SELECT m0.__verdict AS m0__verdict, m0.__expected AS m0__expected, m0.__actual AS m0__actual, m0.__unjudged AS m0__unjudged, m0.__lenient AS m0__lenient, m0.__holds AS m0__holds, m0.__message AS m0__message, m1.__verdict AS m1__verdict, m1.__expected AS m1__expected, m1.__actual AS m1__actual, m1.__unjudged AS m1__unjudged, m1.__lenient AS m1__lenient, m1.__holds AS m1__holds, m1.__message AS m1__message
FROM (
  SELECT coalesce((p.__c_n IS NOT DISTINCT FROM CAST(p.__n_c AS BIGINT)), FALSE) AS __verdict, CAST(CAST(p.__n_c AS BIGINT) AS VARCHAR) AS __expected, CAST(p.__c_n AS VARCHAR) AS __actual, CAST(NULL AS VARCHAR) AS __unjudged, FALSE AS __lenient, coalesce((p.__c_n IS NOT DISTINCT FROM CAST(p.__n_c AS BIGINT)), FALSE) AS __holds, concat('expected ', coalesce(CAST(CAST(p.__n_c AS BIGINT) AS VARCHAR), '[]'), ', got ', coalesce(CAST(p.__c_n AS VARCHAR), '[]')) AS __message
  FROM __p_0 AS p
) AS m0
CROSS JOIN (
  SELECT coalesce((p.__c_n IS NOT DISTINCT FROM CAST(p.__n_c AS BIGINT)), FALSE) AS __verdict, CAST(CAST(p.__n_c AS BIGINT) AS VARCHAR) AS __expected, CAST(p.__c_n AS VARCHAR) AS __actual, CAST(NULL AS VARCHAR) AS __unjudged, FALSE AS __lenient, coalesce((p.__c_n IS NOT DISTINCT FROM CAST(p.__n_c AS BIGINT)), FALSE) AS __holds, concat('expected ', coalesce(CAST(CAST(p.__n_c AS BIGINT) AS VARCHAR), '[]'), ', got ', coalesce(CAST(p.__c_n AS VARCHAR), '[]')) AS __message
  FROM __p_1 AS p
) AS m1
