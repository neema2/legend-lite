WITH frame_r AS MATERIALIZED (SELECT frame_r__t0.ID AS id, frame_r__t0.NAME AS name
  FROM T AS frame_r__t0
  ORDER BY frame_r__t0.ID NULLS LAST), __se_0 AS (SELECT coalesce(CAST(value AS VARCHAR), '[]') AS __text, CASE WHEN value IS NOT NULL AND CAST(value AS VARCHAR) IS NULL THEN 1 ELSE 0 END AS __nulls, CASE WHEN strpos(CAST(value AS VARCHAR), 'tree') > 0 THEN 1 ELSE 0 END AS __trees, CASE WHEN value IS NOT NULL THEN 1 ELSE 0 END AS __n
  FROM (
    SELECT 1 AS __one
  ) AS __one
  LEFT OUTER JOIN (
    SELECT 'a' AS value
  ) AS side ON TRUE), __sa_0 AS (SELECT coalesce(CAST(value AS VARCHAR), '[]') AS __text, CASE WHEN value IS NOT NULL AND CAST(value AS VARCHAR) IS NULL THEN 1 ELSE 0 END AS __nulls, CASE WHEN strpos(CAST(value AS VARCHAR), 'tree') > 0 THEN 1 ELSE 0 END AS __trees, CASE WHEN value IS NOT NULL THEN 1 ELSE 0 END AS __n
  FROM (
    SELECT 1 AS __one
  ) AS __one
  LEFT OUTER JOIN (
    SELECT (SELECT t1.name FROM ( SELECT frame_r_t0.id AS id, frame_r_t0.name AS name FROM frame_r AS frame_r_t0 ORDER BY frame_r_t0.id NULLS LAST LIMIT 1 ) AS t1) AS value
  ) AS side ON TRUE)
SELECT 0 AS __ix, (e.__text IS NOT DISTINCT FROM a.__text) AS __verdict, e.__text AS __expected, a.__text AS __actual, CASE WHEN e.__nulls > 0 OR a.__nulls > 0 THEN 'null-canon-cell' WHEN e.__trees > 0 OR a.__trees > 0 THEN 'unclaimable tree cell' END AS __unjudged, FALSE AS __lenient, (e.__text IS NOT DISTINCT FROM a.__text) AS __holds, concat('assertEquals
expected: ', coalesce(CAST(e.__text AS VARCHAR), 'null'), '
actual:   ', coalesce(CAST(a.__text AS VARCHAR), 'null')) AS __message
FROM __se_0 AS e
CROSS JOIN __sa_0 AS a
