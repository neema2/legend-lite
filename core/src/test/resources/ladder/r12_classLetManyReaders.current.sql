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
  ) AS n), __e_1 AS (SELECT CAST(value AS VARCHAR) AS __c, ROW_NUMBER() OVER () AS __rn
  FROM (
    SELECT UNNEST(list_filter(['a', 'b', 'c'], x -> x IS NOT NULL)) AS value
  ) AS side
  WHERE value IS NOT NULL), __a_1 AS (SELECT CAST(u_map__name AS VARCHAR) AS __c, ROW_NUMBER() OVER () AS __rn
  FROM (
    SELECT frame_r_t0.NAME AS u_map__name
    FROM frame_r AS frame_r_t0
    WHERE frame_r_t0.NAME IS NOT NULL
  ) AS side
  WHERE u_map__name IS NOT NULL), __se_1 AS (SELECT CASE WHEN s.__n = 0 THEN '[]' WHEN s.__n = 1 THEN s.__one ELSE concat('[', s.__joined, ']') END AS __text, s.__nulls AS __nulls, s.__trees AS __trees, s.__n AS __n
  FROM (
    SELECT COUNT(__e.__rn) AS __n, MIN(CASE WHEN __e.__rn = 1 THEN __e.__c END) AS __one, STRING_AGG(__e.__c, ', ' ORDER BY __e.__c ASC) AS __joined, COUNT(CASE WHEN __e.__c IS NULL THEN 1 END) AS __nulls, COUNT(CASE WHEN strpos(__e.__c, 'tree') > 0 THEN 1 END) AS __trees
    FROM __e_1 AS __e
  ) AS s), __sa_1 AS (SELECT CASE WHEN s.__n = 0 THEN '[]' WHEN s.__n = 1 THEN s.__one ELSE concat('[', s.__joined, ']') END AS __text, s.__nulls AS __nulls, s.__trees AS __trees, s.__n AS __n
  FROM (
    SELECT COUNT(__a.__rn) AS __n, MIN(CASE WHEN __a.__rn = 1 THEN __a.__c END) AS __one, STRING_AGG(__a.__c, ', ' ORDER BY __a.__c ASC) AS __joined, COUNT(CASE WHEN __a.__c IS NULL THEN 1 END) AS __nulls, COUNT(CASE WHEN strpos(__a.__c, 'tree') > 0 THEN 1 END) AS __trees
    FROM __a_1 AS __a
  ) AS s), __se_2 AS (SELECT coalesce(CAST(value AS VARCHAR), '[]') AS __text, CASE WHEN value IS NOT NULL AND CAST(value AS VARCHAR) IS NULL THEN 1 ELSE 0 END AS __nulls, CASE WHEN strpos(CAST(value AS VARCHAR), 'tree') > 0 THEN 1 ELSE 0 END AS __trees, CASE WHEN value IS NOT NULL THEN 1 ELSE 0 END AS __n
  FROM (
    SELECT 1 AS __one
  ) AS __one
  LEFT OUTER JOIN (
    SELECT 3 AS value
  ) AS side ON TRUE), __sa_2 AS (SELECT coalesce(CAST(value AS VARCHAR), '[]') AS __text, CASE WHEN value IS NOT NULL AND CAST(value AS VARCHAR) IS NULL THEN 1 ELSE 0 END AS __nulls, CASE WHEN strpos(CAST(value AS VARCHAR), 'tree') > 0 THEN 1 ELSE 0 END AS __trees, CASE WHEN value IS NOT NULL THEN 1 ELSE 0 END AS __n
  FROM (
    SELECT 1 AS __one
  ) AS __one
  LEFT OUTER JOIN (
    SELECT (SELECT COUNT(1) FROM frame_r AS frame_r_t0 WHERE frame_r_t0.AMOUNT > 0.0) AS value
  ) AS side ON TRUE)
SELECT 0 AS __ix, coalesce((p.__c_n IS NOT DISTINCT FROM CAST(p.__n_c AS BIGINT)), FALSE) AS __verdict, CAST(CAST(p.__n_c AS BIGINT) AS VARCHAR) AS __expected, CAST(p.__c_n AS VARCHAR) AS __actual, CAST(NULL AS VARCHAR) AS __unjudged, FALSE AS __lenient, coalesce((p.__c_n IS NOT DISTINCT FROM CAST(p.__n_c AS BIGINT)), FALSE) AS __holds, concat('assertSize
expected: ', coalesce(CAST(CAST(p.__n_c AS BIGINT) AS VARCHAR), 'null'), '
actual:   ', coalesce(CAST(p.__c_n AS VARCHAR), 'null')) AS __message
FROM __p_0 AS p
UNION ALL
SELECT 1 AS __ix, (e.__text IS NOT DISTINCT FROM a.__text) AS __verdict, e.__text AS __expected, a.__text AS __actual, CASE WHEN e.__nulls > 0 OR a.__nulls > 0 THEN 'null-canon-cell' WHEN e.__trees > 0 OR a.__trees > 0 THEN 'unclaimable tree cell' END AS __unjudged, FALSE AS __lenient, (e.__text IS NOT DISTINCT FROM a.__text) AS __holds, concat('assertSameElements
expected: ', coalesce(CAST(e.__text AS VARCHAR), 'null'), '
actual:   ', coalesce(CAST(a.__text AS VARCHAR), 'null')) AS __message
FROM __se_1 AS e
CROSS JOIN __sa_1 AS a
UNION ALL
SELECT 2 AS __ix, (e.__text IS NOT DISTINCT FROM a.__text) AS __verdict, e.__text AS __expected, a.__text AS __actual, CASE WHEN e.__nulls > 0 OR a.__nulls > 0 THEN 'null-canon-cell' WHEN e.__trees > 0 OR a.__trees > 0 THEN 'unclaimable tree cell' END AS __unjudged, FALSE AS __lenient, (e.__text IS NOT DISTINCT FROM a.__text) AS __holds, concat('assertEquals
expected: ', coalesce(CAST(e.__text AS VARCHAR), 'null'), '
actual:   ', coalesce(CAST(a.__text AS VARCHAR), 'null')) AS __message
FROM __se_2 AS e
CROSS JOIN __sa_2 AS a
