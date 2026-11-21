WITH frame_r AS MATERIALIZED (SELECT frame_r__t0.*
  FROM T AS frame_r__t0), __e_0 AS (SELECT UNNEST(['a', 'b', 'c']) AS __v), __a_0 AS (SELECT frame_r_t0.NAME AS __v
  FROM frame_r AS frame_r_t0
  WHERE frame_r_t0.NAME IS NOT NULL)
SELECT 0 AS __ix, NOT EXISTS (SELECT 1
  FROM (
    SELECT __v, 1 AS __s FROM __e_0
    UNION ALL
    SELECT __v, -1 AS __s FROM __a_0
  ) AS d
  GROUP BY d.__v
  HAVING SUM(d.__s) <> 0) AS __verdict, (SELECT CAST(list(__v ORDER BY __v) AS VARCHAR) FROM __e_0) AS __expected, (SELECT CAST(list(__v ORDER BY __v) AS VARCHAR) FROM __a_0) AS __actual
