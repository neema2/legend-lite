WITH __se_0 AS (SELECT coalesce(__once.__oc, '[]') AS __text, CASE WHEN __once.__ov IS NOT NULL AND __once.__oc IS NULL THEN 1 ELSE 0 END AS __nulls, CASE WHEN strpos(__once.__oc, 'tree') > 0 THEN 1 ELSE 0 END AS __trees, CASE WHEN __once.__ov IS NOT NULL THEN 1 ELSE 0 END AS __n, concat('''', replace(replace(replace(CAST(__once.__ov AS VARCHAR), '\', '\\'), '''', '\'''), '
', '\n'), '''') AS __onew, CASE WHEN __once.__ov IS NULL THEN '[]' ELSE concat('[', '''', replace(replace(replace(CAST(__once.__ov AS VARCHAR), '\', '\\'), '''', '\'''), '
', '\n'), '''', ']') END AS __listw
  FROM (
    SELECT CAST(value AS VARCHAR) AS __oc, value AS __ov
    FROM (
      SELECT 1 AS __one
    ) AS __one
    LEFT OUTER JOIN (
      SELECT 'a' AS value
    ) AS side ON TRUE
  ) AS __once)
SELECT 0 AS __ix, (e.__text IS NOT DISTINCT FROM a.__text) AS __verdict, concat('''', replace(replace(CAST(e.__text AS VARCHAR), '\', '\\'), '''', '\'''), '''') AS __expected, concat('''', replace(replace(CAST(a.__text AS VARCHAR), '\', '\\'), '''', '\'''), '''') AS __actual, CASE WHEN e.__nulls > 0 OR a.__nulls > 0 THEN 'null-canon-cell' WHEN e.__trees > 0 OR a.__trees > 0 THEN 'unclaimable tree cell' END AS __unjudged, FALSE AS __lenient, (e.__text IS NOT DISTINCT FROM a.__text) AS __holds, concat('
expected: ', CASE WHEN e.__n = 1 AND a.__n = 1 THEN e.__onew ELSE e.__listw END, '
actual:   ', CASE WHEN e.__n = 1 AND a.__n = 1 THEN a.__onew ELSE a.__listw END) AS __message
FROM __se_0 AS e
CROSS JOIN __se_0 AS a
