WITH __e_0 AS (SELECT 'a' AS __v), __a_0 AS (SELECT side.value AS __v
  FROM (
    SELECT 1 AS __one
  ) AS __one
  LEFT OUTER JOIN (
    SELECT 'a' AS value
  ) AS side ON TRUE)
SELECT 0 AS __ix, (e.__v IS NOT DISTINCT FROM a.__v) AS __verdict, CAST(e.__v AS VARCHAR) AS __expected, CAST(a.__v AS VARCHAR) AS __actual
FROM __e_0 AS e
CROSS JOIN __a_0 AS a
