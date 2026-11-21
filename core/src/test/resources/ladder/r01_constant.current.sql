WITH __se_0 AS (SELECT coalesce(__once.__oc, '[]') AS __text, CASE WHEN __once.__ov IS NOT NULL AND __once.__oc IS NULL THEN 1 ELSE 0 END AS __nulls, CASE WHEN strpos(__once.__oc, 'tree') > 0 THEN 1 ELSE 0 END AS __trees, CASE WHEN __once.__ov IS NOT NULL THEN 1 ELSE 0 END AS __n, CAST(__once.__ov AS VARCHAR) AS __onew, CASE WHEN __once.__ov IS NULL THEN '[]' ELSE concat('[', CAST(__once.__ov AS VARCHAR), ']') END AS __listw
  FROM (
    SELECT CAST(value AS VARCHAR) AS __oc, value AS __ov
    FROM (
      SELECT 1 AS __one
    ) AS __one
    LEFT OUTER JOIN (
      SELECT 1 AS value
    ) AS side ON TRUE
  ) AS __once)
SELECT (e.__text IS NOT DISTINCT FROM a.__text) AS m0__verdict, e.__text AS m0__expected, a.__text AS m0__actual, CASE WHEN e.__nulls > 0 OR a.__nulls > 0 THEN 'null-canon-cell' WHEN e.__trees > 0 OR a.__trees > 0 THEN 'unclaimable tree cell' END AS m0__unjudged, FALSE AS m0__lenient, (e.__text IS NOT DISTINCT FROM a.__text) AS m0__holds, CASE WHEN CASE WHEN e.__n = 1 AND a.__n = 1 THEN e.__onew ELSE e.__listw END = CASE WHEN e.__n = 1 AND a.__n = 1 THEN a.__onew ELSE a.__listw END THEN concat('expected ', CASE WHEN e.__n = 1 AND a.__n = 1 THEN e.__onew ELSE e.__listw END, ' (', 'Integer', '), got ', CASE WHEN e.__n = 1 AND a.__n = 1 THEN a.__onew ELSE a.__listw END, ' (', 'Integer', ')') ELSE concat('expected ', CASE WHEN e.__n = 1 AND a.__n = 1 THEN e.__onew ELSE e.__listw END, ', got ', CASE WHEN e.__n = 1 AND a.__n = 1 THEN a.__onew ELSE a.__listw END) END AS m0__message
FROM __se_0 AS a
CROSS JOIN __se_0 AS e
