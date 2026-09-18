SET threads=1;
CREATE OR REPLACE TABLE fact AS
SELECT (i%5) AS region_id, (i%100) AS country_id, (i%2000) AS city_id,
       CASE WHEN i%37=0 THEN NULL ELSE (i*7919)%100000/100.0 END AS amount
FROM range(5000000) t(i);
.timer on
SELECT 0 AS lvl, NULL::INTEGER r, NULL::INTEGER c, NULL::INTEGER y, sum(amount) s, count(amount) n FROM fact
UNION ALL SELECT 1, region_id, NULL, NULL, sum(amount), count(amount) FROM fact GROUP BY region_id
UNION ALL SELECT 2, region_id, country_id, NULL, sum(amount), count(amount) FROM fact GROUP BY region_id, country_id
UNION ALL SELECT 3, region_id, country_id, city_id, sum(amount), count(amount) FROM fact GROUP BY region_id, country_id, city_id
ORDER BY r NULLS FIRST, c NULLS FIRST, y NULLS FIRST;
