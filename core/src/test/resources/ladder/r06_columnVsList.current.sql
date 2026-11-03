WITH frame_r AS MATERIALIZED (SELECT frame_r__t0.*
  FROM T AS frame_r__t0), __e_0 AS (SELECT CAST(value AS VARCHAR) AS __c, ROW_NUMBER() OVER () AS __rn
  FROM (
    SELECT UNNEST(list_filter(['a', 'b', 'c'], x -> x IS NOT NULL)) AS value
  ) AS side
  WHERE value IS NOT NULL), __a_0 AS (SELECT CAST(u_map__name AS VARCHAR) AS __c, ROW_NUMBER() OVER () AS __rn
  FROM (
    SELECT frame_r_t0.NAME AS u_map__name
    FROM frame_r AS frame_r_t0
    WHERE frame_r_t0.NAME IS NOT NULL
  ) AS side
  WHERE u_map__name IS NOT NULL), __se_0 AS (SELECT CASE WHEN s.__n = 0 THEN '[]' WHEN s.__n = 1 THEN s.__one ELSE concat('[', s.__joined, ']') END AS __text, s.__nulls AS __nulls, s.__trees AS __trees, s.__n AS __n
  FROM (
    SELECT COUNT(__e.__rn) AS __n, MIN(CASE WHEN __e.__rn = 1 THEN __e.__c END) AS __one, STRING_AGG(__e.__c, ', ' ORDER BY __e.__c ASC) AS __joined, COUNT(CASE WHEN __e.__c IS NULL THEN 1 END) AS __nulls, COUNT(CASE WHEN strpos(__e.__c, 'tree') > 0 THEN 1 END) AS __trees
    FROM __e_0 AS __e
  ) AS s), __sa_0 AS (SELECT CASE WHEN s.__n = 0 THEN '[]' WHEN s.__n = 1 THEN s.__one ELSE concat('[', s.__joined, ']') END AS __text, s.__nulls AS __nulls, s.__trees AS __trees, s.__n AS __n
  FROM (
    SELECT COUNT(__a.__rn) AS __n, MIN(CASE WHEN __a.__rn = 1 THEN __a.__c END) AS __one, STRING_AGG(__a.__c, ', ' ORDER BY __a.__c ASC) AS __joined, COUNT(CASE WHEN __a.__c IS NULL THEN 1 END) AS __nulls, COUNT(CASE WHEN strpos(__a.__c, 'tree') > 0 THEN 1 END) AS __trees
    FROM __a_0 AS __a
  ) AS s)
SELECT 0 AS __ix, (e.__text IS NOT DISTINCT FROM a.__text) AS __verdict, e.__text AS __expected, a.__text AS __actual, CASE WHEN e.__nulls > 0 OR a.__nulls > 0 THEN 'null-canon-cell' WHEN e.__trees > 0 OR a.__trees > 0 THEN 'unclaimable tree cell' END AS __unjudged, FALSE AS __lenient, (e.__text IS NOT DISTINCT FROM a.__text) AS __holds, concat('
expected: ', coalesce(replace(CAST(e.__text AS VARCHAR), '
', '\n'), 'null'), '
actual:   ', coalesce(replace(CAST(a.__text AS VARCHAR), '
', '\n'), 'null')) AS __message
FROM __se_0 AS e
CROSS JOIN __sa_0 AS a
