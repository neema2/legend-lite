SELECT t0.p_legalname AS firm, t1.p_name AS city, NOT CASE WHEN NOT EXISTS (SELECT * FROM n_city AS t6 WHERE t0.r_hq = t6.id) THEN CAST(NULL AS JSON) ELSE CAST(concat('{"@labels":', '["City"]', CASE WHEN CASE WHEN NOT EXISTS (SELECT * FROM n_city AS t7 WHERE t0.r_hq = t7.id) THEN NULL WHEN t1.id IS NOT NULL THEN coalesce(list_aggregate(list_filter(['g::City:', CAST(t1.id AS VARCHAR)], x -> x IS NOT NULL), 'string_agg', ''), '') ELSE NULL END IS NULL THEN '' ELSE concat(',"@id":', '"', replace(replace(CASE WHEN NOT EXISTS (SELECT * FROM n_city AS t8 WHERE t0.r_hq = t8.id) THEN NULL WHEN t1.id IS NOT NULL THEN coalesce(list_aggregate(list_filter(['g::City:', CAST(t1.id AS VARCHAR)], x -> x IS NOT NULL), 'string_agg', ''), '') ELSE NULL END, '\', '\\'), '"', '\"'), '"') END, CASE WHEN t1.p_name IS NULL THEN '' ELSE concat(',"name":', '"', replace(replace(t1.p_name, '\', '\\'), '"', '\"'), '"') END, '}') AS JSON) END IS NOT NULL AS noHq, CASE WHEN t3.agg_0 IS NOT NULL THEN t3.agg_0 ELSE 0 END AS headcount, CAST(concat('[', substr(t5.agg_0, 2), ']') AS JSON) AS seniors, CASE WHEN t0.p_founded IS NOT NULL AND t0.p_founded IS NOT NULL AND t0.p_founded IS NOT NULL AND t0.p_founded < 2000 THEN TRUE WHEN NOT t0.p_founded IS NOT NULL OR t0.p_founded IS NOT NULL AND NOT (t0.p_founded IS NOT NULL AND t0.p_founded < 2000) THEN FALSE ELSE NULL END AS old, CASE WHEN NOT CASE WHEN NOT EXISTS (SELECT * FROM n_city AS t9 WHERE t0.r_hq = t9.id) THEN CAST(NULL AS JSON) ELSE CAST(concat('{"@labels":', '["City"]', CASE WHEN CASE WHEN NOT EXISTS (SELECT * FROM n_city AS t10 WHERE t0.r_hq = t10.id) THEN NULL WHEN t1.id IS NOT NULL THEN coalesce(list_aggregate(list_filter(['g::City:', CAST(t1.id AS VARCHAR)], x -> x IS NOT NULL), 'string_agg', ''), '') ELSE NULL END IS NULL THEN '' ELSE concat(',"@id":', '"', replace(replace(CASE WHEN NOT EXISTS (SELECT * FROM n_city AS t11 WHERE t0.r_hq = t11.id) THEN NULL WHEN t1.id IS NOT NULL THEN coalesce(list_aggregate(list_filter(['g::City:', CAST(t1.id AS VARCHAR)], x -> x IS NOT NULL), 'string_agg', ''), '') ELSE NULL END, '\', '\\'), '"', '\"'), '"') END, CASE WHEN t1.p_name IS NULL THEN '' ELSE concat(',"name":', '"', replace(replace(t1.p_name, '\', '\\'), '"', '\"'), '"') END, '}') AS JSON) END IS NOT NULL THEN 'no hq' WHEN t1.p_name IS NULL THEN NULL ELSE concat('in ', t1.p_name) END AS "where"
FROM n_firm AS t0
LEFT OUTER JOIN n_city AS t1 ON t0.r_hq = t1.id
LEFT OUTER JOIN (
  SELECT t2.r_works_at, COUNT(*) AS agg_0
  FROM n_person AS t2
  GROUP BY t2.r_works_at
) AS t3 ON t3.r_works_at = t0.id
LEFT OUTER JOIN (
  SELECT t4.r_works_at, STRING_AGG(concat(',', CASE WHEN t4.p_name IS NULL THEN 'null' ELSE concat('"', replace(replace(t4.p_name, '\', '\\'), '"', '\"'), '"') END), '') AS agg_0
  FROM n_person AS t4
  WHERE t4.p_age IS NOT NULL AND t4.p_age IS NOT NULL AND t4.p_age > 35
  GROUP BY t4.r_works_at
) AS t5 ON t5.r_works_at = t0.id
ORDER BY t0.p_legalname NULLS LAST
