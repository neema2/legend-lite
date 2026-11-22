WITH frame_r AS MATERIALIZED (SELECT frame_r__t0.*
  FROM T AS frame_r__t0), __e_0 AS (SELECT value AS __o, ROW_NUMBER() OVER () AS __rn
  FROM (
    SELECT UNNEST(list_filter([0.5E0, 1.5E0, 2.5E0], x -> x IS NOT NULL)) AS value
  ) AS side
  WHERE value IS NOT NULL), __a_0 AS (SELECT u_map__amount AS __o, ROW_NUMBER() OVER () AS __rn
  FROM (
    SELECT frame_r_t0.AMOUNT AS u_map__amount
    FROM frame_r AS frame_r_t0
    WHERE frame_r_t0.AMOUNT IS NOT NULL
  ) AS side
  WHERE u_map__amount IS NOT NULL)
SELECT 0 AS __m, w.__h AS __s0, CASE WHEN w.__h THEN NULL ELSE w.__x0 END AS __s1, CASE WHEN w.__h THEN NULL ELSE w.__x1 END AS __s2
FROM (
  SELECT (list_sort(e.__l) IS NOT DISTINCT FROM list_sort(a.__l)) AS __h, e.__l AS __x0, a.__l AS __x1
  FROM (
    SELECT LIST(__a.__o ORDER BY __a.__rn ASC) AS __l
    FROM __a_0 AS __a
  ) AS a
  CROSS JOIN (
    SELECT LIST(__e.__o ORDER BY __e.__rn ASC) AS __l
    FROM __e_0 AS __e
  ) AS e
) AS w
