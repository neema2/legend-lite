WITH frame_r AS MATERIALIZED (SELECT frame_r__t0.ID AS id, frame_r__t0.NAME AS name
  FROM T AS frame_r__t0
  ORDER BY frame_r__t0.ID NULLS LAST), __a_0 AS (SELECT COUNT(*) AS __v
  FROM frame_r AS frame_r_t0)
SELECT 0 AS __ix, (a.__v = 3) AS __verdict, CAST(3 AS VARCHAR) AS __expected, CAST(a.__v AS VARCHAR) AS __actual
FROM __a_0 AS a
