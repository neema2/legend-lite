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
  ) AS n), __e_1 AS (SELECT value AS __o, ROW_NUMBER() OVER () AS __rn
  FROM (
    SELECT UNNEST(list_filter(['a', 'b', 'c'], x -> x IS NOT NULL)) AS value
  ) AS side
  WHERE value IS NOT NULL), __a_1 AS (SELECT u_map__name AS __o, ROW_NUMBER() OVER () AS __rn
  FROM (
    SELECT frame_r_t0.NAME AS u_map__name
    FROM frame_r AS frame_r_t0
    WHERE frame_r_t0.NAME IS NOT NULL
  ) AS side
  WHERE u_map__name IS NOT NULL), __e_2 AS (SELECT value AS __o, ROW_NUMBER() OVER () AS __rn
  FROM (
    SELECT 3 AS value
  ) AS side
  WHERE value IS NOT NULL), __a_2 AS (SELECT value AS __o, ROW_NUMBER() OVER () AS __rn
  FROM (
    SELECT (SELECT COUNT(1) FROM frame_r AS frame_r_t0 WHERE frame_r_t0.AMOUNT > 0.0E0) AS value
  ) AS side
  WHERE value IS NOT NULL)
SELECT m0.__verdict AS m0__verdict, m0.__expected AS m0__expected, m0.__actual AS m0__actual, m0.__unjudged AS m0__unjudged, m0.__lenient AS m0__lenient, m0.__holds AS m0__holds, m0.__message AS m0__message, m1.__verdict AS m1__verdict, m1.__e AS m1__e, m1.__a AS m1__a, m2.__verdict AS m2__verdict, m2.__e AS m2__e, m2.__a AS m2__a
FROM (
  SELECT coalesce((p.__c_n IS NOT DISTINCT FROM CAST(p.__n_c AS BIGINT)), FALSE) AS __verdict, CAST(CAST(p.__n_c AS BIGINT) AS VARCHAR) AS __expected, CAST(p.__c_n AS VARCHAR) AS __actual, CAST(NULL AS VARCHAR) AS __unjudged, FALSE AS __lenient, coalesce((p.__c_n IS NOT DISTINCT FROM CAST(p.__n_c AS BIGINT)), FALSE) AS __holds, concat('expected ', coalesce(CAST(CAST(p.__n_c AS BIGINT) AS VARCHAR), '[]'), ', got ', coalesce(CAST(p.__c_n AS VARCHAR), '[]')) AS __message
  FROM __p_0 AS p
) AS m0
CROSS JOIN (
  SELECT q.__same AS __verdict, CASE WHEN q.__same THEN NULL ELSE e.__l END AS __e, CASE WHEN q.__same THEN NULL ELSE a.__l END AS __a
  FROM (
    SELECT LIST(__a.__o ORDER BY __a.__rn ASC) AS __l
    FROM __a_1 AS __a
  ) AS a
  CROSS JOIN (
    SELECT LIST(__e.__o ORDER BY __e.__rn ASC) AS __l
    FROM __e_1 AS __e
  ) AS e
  CROSS JOIN (
    SELECT d.__k = 0 AS __same
    FROM (
      SELECT COUNT(*) AS __k
      FROM (
        SELECT u.__o AS __o
        FROM (
          SELECT __e.__o AS __o, 1 AS __g
          FROM __e_1 AS __e
          UNION ALL
          SELECT __a.__o AS __o, -1 AS __g
          FROM __a_1 AS __a
        ) AS u
        GROUP BY u.__o
        HAVING SUM(u.__g) <> 0
      ) AS h
    ) AS d
  ) AS q
) AS m1
CROSS JOIN (
  SELECT q.__same AS __verdict, CASE WHEN q.__same THEN NULL ELSE e.__l END AS __e, CASE WHEN q.__same THEN NULL ELSE a.__l END AS __a
  FROM (
    SELECT LIST(__a.__o ORDER BY __a.__rn ASC) AS __l
    FROM __a_2 AS __a
  ) AS a
  CROSS JOIN (
    SELECT LIST(__e.__o ORDER BY __e.__rn ASC) AS __l
    FROM __e_2 AS __e
  ) AS e
  CROSS JOIN (
    SELECT ce.__k = ca.__k AND cm.__k = ce.__k AS __same
    FROM (
      SELECT COUNT(__e.__rn) AS __k
      FROM __e_2 AS __e
    ) AS ce
    CROSS JOIN (
      SELECT COUNT(__a.__rn) AS __k
      FROM __a_2 AS __a
    ) AS ca
    CROSS JOIN (
      SELECT COUNT(x.__rn) AS __k
      FROM __e_2 AS x
      JOIN __a_2 AS y ON x.__rn = y.__rn AND (x.__o IS NOT DISTINCT FROM y.__o)
    ) AS cm
  ) AS q
) AS m2
