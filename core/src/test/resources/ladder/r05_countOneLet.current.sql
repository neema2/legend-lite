WITH frame_r AS MATERIALIZED (SELECT frame_r__t0.*
  FROM T AS frame_r__t0), __n_0 AS (SELECT value AS __o, ROW_NUMBER() OVER () AS __rn
  FROM (
    SELECT 3 AS value
  ) AS side
  WHERE value IS NOT NULL)
SELECT 0 AS __m, ((SELECT COUNT(*) AS __n FROM frame_r AS frame_r_t0) IS NOT DISTINCT FROM CAST(n.__v AS BIGINT)) AS __s0, CASE WHEN ((SELECT COUNT(*) AS __n FROM frame_r AS frame_r_t0) IS NOT DISTINCT FROM CAST(n.__v AS BIGINT)) THEN NULL ELSE CAST(n.__v AS BIGINT) END AS __s1, CASE WHEN ((SELECT COUNT(*) AS __n FROM frame_r AS frame_r_t0) IS NOT DISTINCT FROM CAST(n.__v AS BIGINT)) THEN NULL ELSE (SELECT COUNT(*) AS __n FROM frame_r AS frame_r_t0) END AS __s2
FROM (
  SELECT MIN(__n.__o) AS __v
  FROM __n_0 AS __n
) AS n
