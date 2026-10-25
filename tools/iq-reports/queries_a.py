"""Realistic report queries over the retail schema, part A: sales, customers, products."""

LINE = "oi.quantity * oi.unit_price - oi.line_discount"
SHIPPED = "o.status IN ('shipped', 'returned')"

SALES = [
    ("monthly_revenue_and_orders",
     f"""SELECT YEAR(o.order_ts) AS yr, MONTH(o.order_ts) AS mon, COUNT(DISTINCT o.id) AS orders,
         SUM({LINE}) AS gross, ROUND(SUM(({LINE}) * (1 - o.discount_pct)), 2) AS net
         FROM orders o JOIN order_items oi ON oi.order_id = o.id
         WHERE o.status <> 'cancelled'
         GROUP BY YEAR(o.order_ts), MONTH(o.order_ts) ORDER BY yr, mon"""),
    ("quarterly_revenue_with_growth",
     f"""WITH q AS (SELECT YEAR(o.order_ts) AS yr, DATEPART(QUARTER, o.order_ts) AS qtr, SUM({LINE}) AS revenue
         FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE {SHIPPED}
         GROUP BY YEAR(o.order_ts), DATEPART(QUARTER, o.order_ts))
         SELECT yr, qtr, revenue, LAG(revenue) OVER (ORDER BY yr, qtr) AS prev_revenue,
         CASE WHEN LAG(revenue) OVER (ORDER BY yr, qtr) IS NULL THEN NULL
              ELSE ROUND(100.0 * (revenue - LAG(revenue) OVER (ORDER BY yr, qtr)) / LAG(revenue) OVER (ORDER BY yr, qtr), 1) END AS growth_pct
         FROM q ORDER BY yr, qtr"""),
    ("year_to_date_running_total",
     f"""SELECT YEAR(o.order_ts) AS yr, MONTH(o.order_ts) AS mon, SUM({LINE}) AS revenue,
         SUM(SUM({LINE})) OVER (PARTITION BY YEAR(o.order_ts) ORDER BY MONTH(o.order_ts) ROWS UNBOUNDED PRECEDING) AS ytd
         FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE {SHIPPED}
         GROUP BY YEAR(o.order_ts), MONTH(o.order_ts) ORDER BY yr, mon"""),
    ("revenue_by_category_share",
     f"""SELECT p.category, SUM({LINE}) AS revenue,
         ROUND(100.0 * SUM({LINE}) / SUM(SUM({LINE})) OVER (), 2) AS share_pct
         FROM order_items oi JOIN products p ON p.id = oi.product_id JOIN orders o ON o.id = oi.order_id
         WHERE {SHIPPED} GROUP BY p.category ORDER BY revenue DESC, p.category"""),
    ("top_two_products_per_category",
     f"""SELECT category, name, units, revenue FROM (
           SELECT p.category, p.name, p.id, SUM(oi.quantity) AS units, SUM({LINE}) AS revenue,
                  ROW_NUMBER() OVER (PARTITION BY p.category ORDER BY SUM({LINE}) DESC, p.id) AS rn
           FROM products p JOIN order_items oi ON oi.product_id = p.id JOIN orders o ON o.id = oi.order_id
           WHERE o.status <> 'cancelled' GROUP BY p.category, p.name, p.id) ranked
         WHERE rn <= 2 ORDER BY category, rn"""),
    ("sales_by_channel_and_status_pivot",
     """SELECT o.channel,
         COUNT(*) AS orders,
         SUM(CASE WHEN o.status = 'shipped' THEN 1 ELSE 0 END) AS shipped,
         SUM(CASE WHEN o.status = 'pending' THEN 1 ELSE 0 END) AS pending,
         SUM(CASE WHEN o.status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled,
         SUM(CASE WHEN o.status = 'returned' THEN 1 ELSE 0 END) AS returned,
         ROUND(100.0 * SUM(CASE WHEN o.status = 'cancelled' THEN 1 ELSE 0 END) / COUNT(*), 1) AS cancel_rate
         FROM orders o GROUP BY o.channel ORDER BY o.channel"""),
    ("average_order_value_by_segment",
     f"""WITH totals AS (SELECT o.id, o.customer_id, SUM({LINE}) AS order_total
         FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE o.status <> 'cancelled' GROUP BY o.id, o.customer_id)
         SELECT c.segment, COUNT(*) AS orders, ROUND(AVG(t.order_total), 2) AS avg_order_value,
         MIN(t.order_total) AS smallest, MAX(t.order_total) AS largest
         FROM totals t JOIN customers c ON c.id = t.customer_id GROUP BY c.segment ORDER BY c.segment"""),
    ("weekday_sales_profile",
     f"""SELECT DATENAME(WEEKDAY, o.order_ts) AS weekday, DOW(o.order_ts) AS dow, COUNT(DISTINCT o.id) AS orders,
         SUM(oi.quantity) AS units FROM orders o JOIN order_items oi ON oi.order_id = o.id
         GROUP BY DATENAME(WEEKDAY, o.order_ts), DOW(o.order_ts) ORDER BY dow"""),
    ("hour_of_day_buckets",
     """SELECT CASE WHEN HOUR(order_ts) < 6 THEN '1 night' WHEN HOUR(order_ts) < 12 THEN '2 morning'
              WHEN HOUR(order_ts) < 18 THEN '3 afternoon' ELSE '4 evening' END AS daypart,
         COUNT(*) AS orders, ROUND(AVG(CAST(discount_pct AS DOUBLE)) * 100, 2) AS avg_discount_pct
         FROM orders GROUP BY CASE WHEN HOUR(order_ts) < 6 THEN '1 night' WHEN HOUR(order_ts) < 12 THEN '2 morning'
              WHEN HOUR(order_ts) < 18 THEN '3 afternoon' ELSE '4 evening' END ORDER BY daypart"""),
    ("rollup_region_and_segment",
     f"""SELECT r.name AS region, c.segment, GROUPING(r.name) AS g_region, GROUPING(c.segment) AS g_segment,
         COUNT(*) AS lines, SUM({LINE}) AS revenue
         FROM orders o JOIN customers c ON c.id = o.customer_id JOIN regions r ON r.id = c.region_id
         JOIN order_items oi ON oi.order_id = o.id WHERE {SHIPPED}
         GROUP BY ROLLUP(r.name, c.segment) ORDER BY g_region, region, g_segment, c.segment"""),
    ("fiscal_year_revenue",
     f"""SELECT CASE WHEN MONTH(o.order_ts) >= 7 THEN YEAR(o.order_ts) + 1 ELSE YEAR(o.order_ts) END AS fiscal_year,
         COUNT(DISTINCT o.id) AS orders, SUM({LINE}) AS revenue
         FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE {SHIPPED}
         GROUP BY CASE WHEN MONTH(o.order_ts) >= 7 THEN YEAR(o.order_ts) + 1 ELSE YEAR(o.order_ts) END
         ORDER BY fiscal_year"""),
    ("moving_average_of_monthly_revenue",
     f"""WITH m AS (SELECT DATEFORMAT(o.order_ts, 'YYYY-MM') AS ym, SUM({LINE}) AS revenue
         FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE {SHIPPED} GROUP BY DATEFORMAT(o.order_ts, 'YYYY-MM'))
         SELECT ym, revenue, ROUND(AVG(revenue) OVER (ORDER BY ym ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS ma3
         FROM m ORDER BY ym"""),
    ("discount_impact_by_band",
     f"""SELECT CASE WHEN o.discount_pct = 0 THEN 'none' WHEN o.discount_pct <= 0.10 THEN 'low' ELSE 'high' END AS band,
         COUNT(DISTINCT o.id) AS orders, SUM({LINE}) AS gross,
         ROUND(SUM(({LINE}) * o.discount_pct), 2) AS discount_given
         FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE o.status <> 'cancelled'
         GROUP BY CASE WHEN o.discount_pct = 0 THEN 'none' WHEN o.discount_pct <= 0.10 THEN 'low' ELSE 'high' END
         ORDER BY band"""),
    ("best_month_per_year",
     f"""WITH m AS (SELECT YEAR(o.order_ts) AS yr, MONTH(o.order_ts) AS mon, SUM({LINE}) AS revenue
         FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE {SHIPPED} GROUP BY YEAR(o.order_ts), MONTH(o.order_ts))
         SELECT yr, mon, revenue FROM m m1
         WHERE revenue = (SELECT MAX(m2.revenue) FROM m m2 WHERE m2.yr = m1.yr) ORDER BY yr, mon"""),
]

CUSTOMERS = [
    ("customer_lifetime_value_top_ten",
     f"""SELECT TOP 10 c.id, TRIM(c.first_name) || ' ' || c.last_name AS customer, COUNT(DISTINCT o.id) AS orders,
         SUM({LINE}) AS lifetime_value, MIN(o.order_ts) AS first_order, MAX(o.order_ts) AS last_order
         FROM customers c JOIN orders o ON o.customer_id = c.id JOIN order_items oi ON oi.order_id = o.id
         WHERE o.status <> 'cancelled' GROUP BY c.id, c.first_name, c.last_name
         ORDER BY lifetime_value DESC, c.id"""),
    ("customers_without_orders",
     """SELECT c.id, c.first_name, c.last_name, c.signup_date FROM customers c
         WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id) ORDER BY c.id"""),
    ("customers_without_shipped_orders_left_join",
     """SELECT c.id, c.last_name, COUNT(o.id) AS shipped_orders FROM customers c
         LEFT JOIN orders o ON o.customer_id = c.id AND o.status = 'shipped'
         GROUP BY c.id, c.last_name HAVING COUNT(o.id) = 0 ORDER BY c.id"""),
    ("email_domains",
     """SELECT LOWER(SUBSTRING(email, CHARINDEX('@', email) + 1, LENGTH(email))) AS domain, COUNT(*) AS customers
         FROM customers WHERE email IS NOT NULL
         GROUP BY LOWER(SUBSTRING(email, CHARINDEX('@', email) + 1, LENGTH(email))) ORDER BY customers DESC, domain"""),
    ("normalized_phone_digits",
     """SELECT id, phone, REPLACE(REPLACE(REPLACE(REPLACE(phone, '(', ''), ')', ''), '-', ''), ' ', '') AS digits,
         LENGTH(REPLACE(REPLACE(REPLACE(REPLACE(phone, '(', ''), ')', ''), '-', ''), ' ', '')) AS n_digits
         FROM customers WHERE phone IS NOT NULL ORDER BY id"""),
    ("display_names_and_initials",
     """SELECT id, UPPER(LEFT(TRIM(first_name), 1)) || '. ' || last_name AS short_name,
         LEFT(TRIM(first_name), 1) || LEFT(last_name, 1) AS initials,
         COALESCE(LOWER(email), 'no email on file') AS contact
         FROM customers ORDER BY last_name, id"""),
    ("customer_age_bands",
     """SELECT CASE WHEN birth_date IS NULL THEN 'unknown'
              WHEN DATEDIFF(YEAR, birth_date, '2026-01-01') < 30 THEN 'under 30'
              WHEN DATEDIFF(YEAR, birth_date, '2026-01-01') < 50 THEN '30-49'
              WHEN DATEDIFF(YEAR, birth_date, '2026-01-01') < 65 THEN '50-64' ELSE '65+' END AS age_band,
         COUNT(*) AS customers, ROUND(AVG(credit_limit), 2) AS avg_credit
         FROM customers GROUP BY CASE WHEN birth_date IS NULL THEN 'unknown'
              WHEN DATEDIFF(YEAR, birth_date, '2026-01-01') < 30 THEN 'under 30'
              WHEN DATEDIFF(YEAR, birth_date, '2026-01-01') < 50 THEN '30-49'
              WHEN DATEDIFF(YEAR, birth_date, '2026-01-01') < 65 THEN '50-64' ELSE '65+' END
         ORDER BY age_band"""),
    ("signup_cohorts_by_quarter",
     """SELECT YEAR(c.signup_date) AS cohort_year, DATEPART(QUARTER, c.signup_date) AS cohort_qtr,
         COUNT(DISTINCT c.id) AS customers, COUNT(DISTINCT o.customer_id) AS buyers,
         ROUND(100.0 * COUNT(DISTINCT o.customer_id) / COUNT(DISTINCT c.id), 1) AS conversion_pct
         FROM customers c LEFT JOIN orders o ON o.customer_id = c.id AND o.status <> 'cancelled'
         GROUP BY YEAR(c.signup_date), DATEPART(QUARTER, c.signup_date) ORDER BY cohort_year, cohort_qtr"""),
    ("days_between_orders",
     """SELECT customer_id, id AS order_id, order_ts,
         DATEDIFF(DAY, LAG(order_ts) OVER (PARTITION BY customer_id ORDER BY order_ts, id), order_ts) AS days_since_prev
         FROM orders WHERE status <> 'cancelled' ORDER BY customer_id, order_ts, id"""),
    ("rfm_segmentation",
     f"""WITH rfm AS (SELECT c.id, DATEDIFF(DAY, MAX(o.order_ts), '2026-01-01') AS recency_days,
           COUNT(DISTINCT o.id) AS frequency, SUM({LINE}) AS monetary
           FROM customers c JOIN orders o ON o.customer_id = c.id JOIN order_items oi ON oi.order_id = o.id
           WHERE o.status <> 'cancelled' GROUP BY c.id)
         SELECT id, recency_days, frequency, monetary,
           NTILE(3) OVER (ORDER BY recency_days DESC, id) AS r_score,
           NTILE(3) OVER (ORDER BY frequency, id) AS f_score,
           NTILE(3) OVER (ORDER BY monetary, id) AS m_score
         FROM rfm ORDER BY id"""),
    ("customers_above_region_average",
     f"""WITH spend AS (SELECT c.id, c.region_id, SUM({LINE}) AS total FROM customers c
           JOIN orders o ON o.customer_id = c.id JOIN order_items oi ON oi.order_id = o.id
           WHERE o.status <> 'cancelled' GROUP BY c.id, c.region_id)
         SELECT s.id, s.region_id, s.total FROM spend s
         WHERE s.total > (SELECT AVG(s2.total) FROM spend s2 WHERE s2.region_id = s.region_id)
         ORDER BY s.region_id, s.total DESC, s.id"""),
    ("first_and_latest_order_per_customer",
     """SELECT c.id, c.last_name,
         (SELECT MIN(o.order_ts) FROM orders o WHERE o.customer_id = c.id) AS first_order,
         (SELECT MAX(o.order_ts) FROM orders o WHERE o.customer_id = c.id) AS latest_order,
         (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id AND o.status = 'returned') AS returns
         FROM customers c WHERE c.is_active = 1 ORDER BY c.id"""),
    ("repeat_purchase_rate_by_region",
     """WITH per AS (SELECT c.region_id, c.id, COUNT(o.id) AS n FROM customers c
           LEFT JOIN orders o ON o.customer_id = c.id AND o.status <> 'cancelled' GROUP BY c.region_id, c.id)
         SELECT r.name, COUNT(*) AS customers, SUM(CASE WHEN per.n >= 2 THEN 1 ELSE 0 END) AS repeaters,
         ROUND(100.0 * SUM(CASE WHEN per.n >= 2 THEN 1 ELSE 0 END) / COUNT(*), 1) AS repeat_pct
         FROM per JOIN regions r ON r.id = per.region_id GROUP BY r.name ORDER BY r.name"""),
    ("credit_utilization",
     f"""SELECT c.id, c.credit_limit, COALESCE(SUM(CASE WHEN o.status = 'pending' THEN {LINE} END), 0) AS open_amount,
         CASE WHEN c.credit_limit = 0 THEN NULL ELSE ROUND(100.0 * COALESCE(SUM(CASE WHEN o.status = 'pending' THEN {LINE} END), 0) / c.credit_limit, 2) END AS utilization_pct
         FROM customers c LEFT JOIN orders o ON o.customer_id = c.id LEFT JOIN order_items oi ON oi.order_id = o.id
         GROUP BY c.id, c.credit_limit ORDER BY utilization_pct DESC, c.id"""),
]

PRODUCTS = [
    ("product_margins",
     """SELECT id, sku, name, unit_price, unit_cost, unit_price - unit_cost AS margin,
         ROUND(100.0 * (unit_price - unit_cost) / unit_price, 1) AS margin_pct
         FROM products WHERE discontinued = 0 ORDER BY margin_pct DESC, id"""),
    ("sku_parts",
     """SELECT id, sku, LEFT(sku, 3) AS cat_code, SUBSTRING(sku, 5, 3) AS sub_code,
         CAST(RIGHT(sku, 3) AS INTEGER) AS seq, CASE WHEN UPPER(LEFT(category, 3)) = LEFT(sku, 3) THEN 'yes' ELSE 'no' END AS consistent
         FROM products ORDER BY id"""),
    ("price_bands_with_counts",
     """SELECT CASE WHEN unit_price < 25 THEN 'a: under 25' WHEN unit_price < 100 THEN 'b: 25-99'
              WHEN unit_price < 500 THEN 'c: 100-499' ELSE 'd: 500+' END AS band,
         COUNT(*) AS products, MIN(unit_price) AS lo, MAX(unit_price) AS hi, ROUND(AVG(unit_price), 2) AS mean
         FROM products GROUP BY CASE WHEN unit_price < 25 THEN 'a: under 25' WHEN unit_price < 100 THEN 'b: 25-99'
              WHEN unit_price < 500 THEN 'c: 100-499' ELSE 'd: 500+' END ORDER BY band"""),
    ("products_never_sold",
     """SELECT p.id, p.name, p.category FROM products p
         WHERE p.id NOT IN (SELECT oi.product_id FROM order_items oi JOIN orders o ON o.id = oi.order_id WHERE o.status <> 'cancelled')
         ORDER BY p.id"""),
    ("units_and_revenue_rank_in_category",
     f"""SELECT p.category, p.name, SUM(oi.quantity) AS units,
         RANK() OVER (PARTITION BY p.category ORDER BY SUM(oi.quantity) DESC) AS unit_rank,
         DENSE_RANK() OVER (ORDER BY SUM({LINE}) DESC) AS revenue_rank_overall
         FROM products p JOIN order_items oi ON oi.product_id = p.id GROUP BY p.category, p.name, p.id
         ORDER BY p.category, unit_rank, p.id"""),
    ("discounted_line_share",
     """SELECT p.subcategory, COUNT(*) AS lines, SUM(CASE WHEN oi.line_discount > 0 OR oi.unit_price < p.unit_price THEN 1 ELSE 0 END) AS discounted,
         ROUND(100.0 * SUM(CASE WHEN oi.line_discount > 0 OR oi.unit_price < p.unit_price THEN 1 ELSE 0 END) / COUNT(*), 1) AS discounted_pct
         FROM order_items oi JOIN products p ON p.id = oi.product_id GROUP BY p.subcategory ORDER BY p.subcategory"""),
    ("weight_and_shipping_class",
     """SELECT id, name, weight_kg, CASE WHEN weight_kg IS NULL THEN 'unknown' WHEN weight_kg < 1 THEN 'parcel'
              WHEN weight_kg < 5 THEN 'standard' ELSE 'freight' END AS ship_class,
         ROUND(COALESCE(weight_kg, 0) * 2.20462, 2) AS weight_lb FROM products ORDER BY id"""),
    ("product_age_in_months",
     """SELECT id, name, launched, DATEDIFF(MONTH, launched, '2026-01-01') AS months_on_market,
         CASE WHEN DATEDIFF(MONTH, launched, '2026-01-01') > 48 THEN 'mature' ELSE 'new' END AS lifecycle
         FROM products ORDER BY launched, id"""),
    ("cross_sell_pairs",
     """SELECT a.product_id AS p1, b.product_id AS p2, COUNT(*) AS together FROM order_items a
         JOIN order_items b ON b.order_id = a.order_id AND b.product_id > a.product_id
         GROUP BY a.product_id, b.product_id HAVING COUNT(*) >= 2 ORDER BY together DESC, p1, p2"""),
    ("category_price_spread",
     """SELECT category, COUNT(*) AS n, MIN(unit_price) AS lo, MAX(unit_price) AS hi, MAX(unit_price) - MIN(unit_price) AS spread,
         ROUND(STDDEV(CAST(unit_price AS DOUBLE)), 4) AS sd FROM products GROUP BY category ORDER BY category"""),
]

CASES = [("sales", SALES), ("customers", CUSTOMERS), ("products", PRODUCTS)]
