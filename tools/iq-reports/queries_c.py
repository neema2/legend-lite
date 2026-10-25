"""Realistic report queries over the retail schema, part C: strings, dates, calculations and analyses."""

LINE = "oi.quantity * oi.unit_price - oi.line_discount"
TOTALS = f"""(SELECT o.id, o.customer_id, o.order_ts, o.channel, o.status, o.discount_pct, SUM({LINE}) AS total
    FROM orders o JOIN order_items oi ON oi.order_id = o.id GROUP BY o.id, o.customer_id, o.order_ts, o.channel,
    o.status, o.discount_pct)"""

STRINGS = [
    ("masked_emails",
     """SELECT id, email, LEFT(email, 2) || REPEAT('*', LOCATE(email, '@') - 3) || SUBSTRING(email, LOCATE(email, '@')) AS masked
         FROM customers WHERE email IS NOT NULL AND LOCATE(email, '@') > 3 ORDER BY id"""),
    ("formal_names",
     """SELECT id, UPPER(LEFT(last_name, 1)) || LOWER(SUBSTRING(last_name, 2)) || ', ' || TRIM(first_name) AS formal,
         LENGTH(TRIM(first_name)) + LENGTH(last_name) AS name_length FROM customers ORDER BY last_name, id"""),
    ("phone_area_codes",
     """SELECT LEFT(REPLACE(REPLACE(REPLACE(REPLACE(phone, '(', ''), ')', ''), '-', ''), ' ', ''), 3) AS area_code,
         COUNT(*) AS customers FROM customers WHERE phone IS NOT NULL
         GROUP BY LEFT(REPLACE(REPLACE(REPLACE(REPLACE(phone, '(', ''), ')', ''), '-', ''), ' ', ''), 3)
         ORDER BY customers DESC, area_code"""),
    ("city_labels",
     """SELECT c.id, c.city || ' (' || r.name || ', ' || UPPER(r.country) || ')' AS location,
         CASE WHEN r.country = 'USA' THEN 'domestic' ELSE 'international' END AS market
         FROM customers c JOIN regions r ON r.id = c.region_id ORDER BY r.country, c.city, c.id"""),
    ("product_search",
     """SELECT id, name, category FROM products
         WHERE (LOWER(name) LIKE '%pro%' OR name LIKE 'Phone%' OR name LIKE '%Desk')
           AND discontinued = 0 AND unit_price BETWEEN 10 AND 2000 ORDER BY name, id"""),
    ("sku_validation",
     """SELECT id, sku, CASE WHEN sku LIKE '___-___-___' AND ISNUMERIC(RIGHT(sku, 3)) = 1 THEN 'valid' ELSE 'invalid' END AS check_result,
         LENGTH(sku) AS len FROM products ORDER BY id"""),
    ("order_summary_lines",
     f"""SELECT t.id, 'Order ' || CAST(t.id AS VARCHAR(10)) || ' (' || t.channel || '): ' || CAST(t.total AS VARCHAR(20)) || ' USD' AS summary
         FROM {TOTALS} t WHERE t.status = 'shipped' AND t.total > 2000 ORDER BY t.total DESC, t.id"""),
    ("products_listed_per_order",
     """SELECT o.id, COUNT(*) AS lines, LIST(p.name, '; ' ORDER BY oi.id) AS products
         FROM orders o JOIN order_items oi ON oi.order_id = o.id JOIN products p ON p.id = oi.product_id
         WHERE o.id BETWEEN 1 AND 20 GROUP BY o.id ORDER BY o.id"""),
    ("contact_directory_union",
     """SELECT 'customer' AS kind, id, TRIM(first_name) || ' ' || last_name AS name, COALESCE(email, '-') AS contact FROM customers WHERE is_active = 1
         UNION ALL SELECT 'employee', id, first_name || ' ' || last_name, LOWER(first_name) || '.' || LOWER(last_name) || '@company.example' FROM employees
         ORDER BY 1, 3, 2"""),
    ("email_quality",
     """SELECT CASE WHEN email IS NULL THEN 'missing' WHEN email <> LOWER(email) THEN 'uppercase'
              WHEN email LIKE '%@gmail.com' OR email LIKE '%@yahoo.com' OR email LIKE '%@outlook.com' THEN 'personal'
              ELSE 'business' END AS kind, COUNT(*) AS customers
         FROM customers GROUP BY CASE WHEN email IS NULL THEN 'missing' WHEN email <> LOWER(email) THEN 'uppercase'
              WHEN email LIKE '%@gmail.com' OR email LIKE '%@yahoo.com' OR email LIKE '%@outlook.com' THEN 'personal'
              ELSE 'business' END ORDER BY kind"""),
]

DATES = [
    ("orders_by_week_start",
     """SELECT DATEADD(DAY, 1 - DOW(order_ts), CAST(order_ts AS DATE)) AS week_start, COUNT(*) AS orders
         FROM orders WHERE order_ts >= '2025-10-01' AND order_ts < '2026-01-01'
         GROUP BY DATEADD(DAY, 1 - DOW(order_ts), CAST(order_ts AS DATE)) ORDER BY week_start"""),
    ("age_at_signup",
     """SELECT id, birth_date, signup_date,
         DATEDIFF(YEAR, birth_date, signup_date)
           - CASE WHEN MONTH(signup_date) * 100 + DAY(signup_date) < MONTH(birth_date) * 100 + DAY(birth_date) THEN 1 ELSE 0 END AS age_at_signup
         FROM customers WHERE birth_date IS NOT NULL ORDER BY id"""),
    ("weekend_orders",
     """SELECT CASE WHEN DOW(order_ts) IN (1, 7) THEN 'weekend' ELSE 'weekday' END AS day_type, channel, COUNT(*) AS orders
         FROM orders GROUP BY CASE WHEN DOW(order_ts) IN (1, 7) THEN 'weekend' ELSE 'weekday' END, channel
         ORDER BY day_type, channel"""),
    ("quarter_boundaries",
     """SELECT id, CAST(order_ts AS DATE) AS order_day, YEAR(order_ts) AS yr, DATEPART(QUARTER, order_ts) AS qtr,
         YMD(YEAR(order_ts), 3 * DATEPART(QUARTER, order_ts) - 2, 1) AS quarter_start,
         DATEADD(DAY, -1, DATEADD(MONTH, 3, YMD(YEAR(order_ts), 3 * DATEPART(QUARTER, order_ts) - 2, 1))) AS quarter_end
         FROM orders WHERE id <= 15 ORDER BY id"""),
    ("formatted_timestamps",
     """SELECT id, DATEFORMAT(order_ts, 'YYYY-MM-DD HH:NN') AS stamp, DATEFORMAT(order_ts, 'Mmm DD, YYYY') AS pretty,
         DATENAME(MONTH, order_ts) AS month_name, DATEPART(DAYOFYEAR, order_ts) AS day_of_year
         FROM orders WHERE id <= 12 ORDER BY id"""),
    ("time_to_first_payment",
     """SELECT o.id, DATEDIFF(MINUTE, o.order_ts, MIN(p.paid_ts)) AS minutes,
         CAST(DATEDIFF(MINUTE, o.order_ts, MIN(p.paid_ts)) / 60 AS VARCHAR(10)) || 'h ' ||
         CAST(MOD(DATEDIFF(MINUTE, o.order_ts, MIN(p.paid_ts)), 60) AS VARCHAR(10)) || 'm' AS elapsed
         FROM orders o JOIN payments p ON p.order_id = o.id WHERE o.id <= 40 GROUP BY o.id, o.order_ts ORDER BY o.id"""),
    ("signup_anniversaries_in_december",
     """SELECT id, first_name, signup_date, DATEDIFF(YEAR, signup_date, '2025-12-31') AS years_with_us
         FROM customers WHERE MONTH(signup_date) = 12 OR (MONTH(signup_date) = 11 AND DAY(signup_date) > 15) ORDER BY signup_date, id"""),
    ("ship_within_three_days_by_month",
     """SELECT DATEFORMAT(order_ts, 'YYYY-MM') AS ym, COUNT(*) AS shipped,
         SUM(CASE WHEN DATEDIFF(DAY, order_ts, ship_date) <= 3 THEN 1 ELSE 0 END) AS fast,
         ROUND(100.0 * SUM(CASE WHEN DATEDIFF(DAY, order_ts, ship_date) <= 3 THEN 1 ELSE 0 END) / COUNT(*), 1) AS fast_pct
         FROM orders WHERE ship_date IS NOT NULL AND order_ts >= '2025-01-01' GROUP BY DATEFORMAT(order_ts, 'YYYY-MM') ORDER BY ym"""),
    ("late_payments",
     """SELECT p.id, p.order_id, o.order_ts, p.paid_ts, DATEDIFF(DAY, o.order_ts, p.paid_ts) AS days_late
         FROM payments p JOIN orders o ON o.id = p.order_id WHERE DATEDIFF(DAY, o.order_ts, p.paid_ts) > 7
         ORDER BY days_late DESC, p.id"""),
    ("hire_date_cohorts",
     """SELECT YEAR(hire_date) AS hire_year, COUNT(*) AS hires, MIN(hire_date) AS first_hire,
         ROUND(AVG(salary), 2) AS avg_salary, DATEDIFF(MONTH, MIN(hire_date), MAX(hire_date)) AS span_months
         FROM employees GROUP BY YEAR(hire_date) ORDER BY hire_year"""),
]

ANALYSES = [
    ("weighted_average_price_by_category",
     """SELECT p.category, SUM(oi.quantity) AS units,
         ROUND(SUM(oi.quantity * oi.unit_price) / SUM(oi.quantity), 2) AS weighted_avg_price,
         ROUND(AVG(oi.unit_price), 2) AS simple_avg_price
         FROM order_items oi JOIN products p ON p.id = oi.product_id GROUP BY p.category ORDER BY p.category"""),
    ("new_vs_returning_customers_by_month",
     """WITH firsts AS (SELECT customer_id, MIN(order_ts) AS first_ts FROM orders WHERE status <> 'cancelled' GROUP BY customer_id)
         SELECT DATEFORMAT(o.order_ts, 'YYYY-MM') AS ym,
         COUNT(DISTINCT CASE WHEN DATEFORMAT(f.first_ts, 'YYYY-MM') = DATEFORMAT(o.order_ts, 'YYYY-MM') THEN o.customer_id END) AS new_customers,
         COUNT(DISTINCT CASE WHEN f.first_ts < DATEADD(DAY, 1 - DAY(o.order_ts), CAST(o.order_ts AS DATE)) THEN o.customer_id END) AS returning_customers
         FROM orders o JOIN firsts f ON f.customer_id = o.customer_id WHERE o.status <> 'cancelled'
         GROUP BY DATEFORMAT(o.order_ts, 'YYYY-MM') ORDER BY ym"""),
    ("revenue_concentration_by_quintile",
     f"""WITH spend AS (SELECT t.customer_id, SUM(t.total) AS total FROM {TOTALS} t WHERE t.status <> 'cancelled' GROUP BY t.customer_id),
         q AS (SELECT customer_id, total, NTILE(5) OVER (ORDER BY total DESC, customer_id) AS quintile FROM spend)
         SELECT quintile, COUNT(*) AS customers, SUM(total) AS revenue,
         ROUND(100.0 * SUM(total) / SUM(SUM(total)) OVER (), 1) AS share_pct FROM q GROUP BY quintile ORDER BY quintile"""),
    ("pareto_of_products",
     f"""WITH r AS (SELECT p.id, p.name, SUM({LINE}) AS revenue FROM products p JOIN order_items oi ON oi.product_id = p.id
           JOIN orders o ON o.id = oi.order_id WHERE o.status IN ('shipped', 'returned') GROUP BY p.id, p.name)
         SELECT id, name, revenue,
         SUM(revenue) OVER (ORDER BY revenue DESC, id ROWS UNBOUNDED PRECEDING) AS cumulative,
         ROUND(100.0 * SUM(revenue) OVER (ORDER BY revenue DESC, id ROWS UNBOUNDED PRECEDING) / SUM(revenue) OVER (), 1) AS cumulative_pct
         FROM r ORDER BY revenue DESC, id"""),
    ("line_level_discount_pct",
     """SELECT oi.id, p.name, p.unit_price AS list_price, oi.unit_price AS sold_at,
         ROUND(100.0 * (p.unit_price - oi.unit_price) / p.unit_price, 2) AS price_cut_pct,
         oi.line_discount, oi.quantity * (p.unit_price - oi.unit_price) + oi.line_discount AS total_concession
         FROM order_items oi JOIN products p ON p.id = oi.product_id
         WHERE oi.unit_price < p.unit_price OR oi.line_discount > 0 ORDER BY total_concession DESC, oi.id"""),
    ("lapsed_products",
     """SELECT p.id, p.name, MAX(o.order_ts) AS last_sold FROM products p JOIN order_items oi ON oi.product_id = p.id
         JOIN orders o ON o.id = oi.order_id GROUP BY p.id, p.name
         HAVING MAX(o.order_ts) < DATEADD(DAY, -180, '2026-01-01') ORDER BY last_sold, p.id"""),
    ("management_chain",
     """SELECT e.id, e.last_name AS employee, m.last_name AS manager, mm.last_name AS managers_manager,
         CASE WHEN m.id IS NULL THEN 1 WHEN mm.id IS NULL THEN 2 ELSE 3 END AS level
         FROM employees e LEFT JOIN employees m ON m.id = e.manager_id LEFT JOIN employees mm ON mm.id = m.manager_id
         ORDER BY level, e.id"""),
    ("median_and_spread_of_order_values",
     f"""SELECT t.channel, COUNT(*) AS orders, MEDIAN(t.total) AS median_total, ROUND(AVG(t.total), 2) AS mean_total,
         ROUND(STDDEV(CAST(t.total AS DOUBLE)), 2) AS sd_total
         FROM {TOTALS} t WHERE t.status <> 'cancelled' GROUP BY t.channel ORDER BY t.channel"""),
    ("categories_with_healthy_margins",
     """SELECT category, products_sold, margin_pct FROM (
           SELECT p.category, COUNT(DISTINCT p.id) AS products_sold,
             ROUND(100.0 * SUM(oi.quantity * (oi.unit_price - p.unit_cost)) / SUM(oi.quantity * oi.unit_price), 1) AS margin_pct
           FROM products p JOIN order_items oi ON oi.product_id = p.id GROUP BY p.category HAVING COUNT(DISTINCT p.id) >= 3) m
         WHERE margin_pct > 30 ORDER BY margin_pct DESC, category"""),
    ("customers_buying_every_category",
     """SELECT c.id, c.last_name FROM customers c
         WHERE NOT EXISTS (SELECT 1 FROM products p0 WHERE NOT EXISTS (
           SELECT 1 FROM orders o JOIN order_items oi ON oi.order_id = o.id JOIN products p ON p.id = oi.product_id
           WHERE o.customer_id = c.id AND p.category = p0.category))
         ORDER BY c.id"""),
    ("bought_laptops_and_phones",
     """SELECT c.id, c.last_name FROM customers c
         WHERE c.id IN (SELECT o.customer_id FROM orders o JOIN order_items oi ON oi.order_id = o.id JOIN products p ON p.id = oi.product_id WHERE p.subcategory = 'Laptops')
           AND c.id IN (SELECT o.customer_id FROM orders o JOIN order_items oi ON oi.order_id = o.id JOIN products p ON p.id = oi.product_id WHERE p.subcategory = 'Phones')
         ORDER BY c.id"""),
    ("orders_above_customer_norm",
     f"""SELECT t.id, t.customer_id, t.total,
         ROUND((SELECT AVG(t2.total) FROM {TOTALS} t2 WHERE t2.customer_id = t.customer_id), 2) AS customer_avg
         FROM {TOTALS} t
         WHERE t.total / 1.5 > (SELECT AVG(t2.total) FROM {TOTALS} t2 WHERE t2.customer_id = t.customer_id)
         ORDER BY t.customer_id, t.id"""),
    ("second_largest_order_per_customer",
     f"""SELECT customer_id, id, total FROM (SELECT t.customer_id, t.id, t.total,
           DENSE_RANK() OVER (PARTITION BY t.customer_id ORDER BY t.total DESC) AS dr FROM {TOTALS} t) x
         WHERE dr = 2 ORDER BY customer_id, id"""),
    ("buying_streaks",
     """WITH seq AS (SELECT customer_id, id, order_ts,
           CASE WHEN DATEDIFF(DAY, LAG(order_ts) OVER (PARTITION BY customer_id ORDER BY order_ts, id), order_ts) <= 120 THEN 0 ELSE 1 END AS starts
           FROM orders WHERE status <> 'cancelled')
         SELECT customer_id, id, order_ts, SUM(starts) OVER (PARTITION BY customer_id ORDER BY order_ts, id ROWS UNBOUNDED PRECEDING) AS streak_no
         FROM seq ORDER BY customer_id, order_ts, id"""),
    ("region_channel_grid",
     """SELECT r.name AS region, ch.channel, COUNT(o.id) AS orders FROM regions r
         CROSS JOIN (SELECT DISTINCT channel FROM orders) ch
         LEFT JOIN customers c ON c.region_id = r.id
         LEFT JOIN orders o ON o.customer_id = c.id AND o.channel = ch.channel
         GROUP BY r.name, ch.channel ORDER BY r.name, ch.channel"""),
    ("favorite_category_per_customer",
     """SELECT customer_id, category, n FROM (
           SELECT o.customer_id, p.category, COUNT(*) AS n,
             ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY COUNT(*) DESC, p.category) AS rn
           FROM orders o JOIN order_items oi ON oi.order_id = o.id JOIN products p ON p.id = oi.product_id
           GROUP BY o.customer_id, p.category) f
         WHERE rn = 1 ORDER BY customer_id"""),
    ("top_city_per_region",
     f"""SELECT region, city, revenue FROM (
           SELECT r.name AS region, c.city, SUM({LINE}) AS revenue,
             RANK() OVER (PARTITION BY r.name ORDER BY SUM({LINE}) DESC, c.city) AS rk
           FROM regions r JOIN customers c ON c.region_id = r.id JOIN orders o ON o.customer_id = c.id
           JOIN order_items oi ON oi.order_id = o.id WHERE o.status <> 'cancelled' GROUP BY r.name, c.city) x
         WHERE rk = 1 ORDER BY region"""),
    ("year_over_year_month_pivot",
     f"""SELECT MONTH(o.order_ts) AS mon,
         SUM(CASE WHEN YEAR(o.order_ts) = 2023 THEN {LINE} ELSE 0 END) AS y2023,
         SUM(CASE WHEN YEAR(o.order_ts) = 2024 THEN {LINE} ELSE 0 END) AS y2024,
         SUM(CASE WHEN YEAR(o.order_ts) = 2025 THEN {LINE} ELSE 0 END) AS y2025,
         CASE WHEN SUM(CASE WHEN YEAR(o.order_ts) = 2024 THEN {LINE} ELSE 0 END) = 0 THEN NULL
              ELSE ROUND(100.0 * (SUM(CASE WHEN YEAR(o.order_ts) = 2025 THEN {LINE} ELSE 0 END)
                - SUM(CASE WHEN YEAR(o.order_ts) = 2024 THEN {LINE} ELSE 0 END)) / SUM(CASE WHEN YEAR(o.order_ts) = 2024 THEN {LINE} ELSE 0 END), 1) END AS yoy_pct
         FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE o.status IN ('shipped', 'returned')
         GROUP BY MONTH(o.order_ts) ORDER BY mon"""),
    ("slow_starters",
     """SELECT c.id, c.signup_date FROM customers c
         WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.order_ts < DATEADD(DAY, 90, c.signup_date))
           AND EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)
         ORDER BY c.id"""),
    ("three_level_derived_filters",
     f"""SELECT segment, buyers, avg_spend FROM (
           SELECT segment, COUNT(*) AS buyers, ROUND(AVG(spend), 2) AS avg_spend FROM (
             SELECT c.segment, c.id, SUM(t.total) AS spend FROM customers c JOIN (
               SELECT * FROM {TOTALS} t0 WHERE t0.status <> 'cancelled') t ON t.customer_id = c.id
             WHERE c.is_active = 1 GROUP BY c.segment, c.id) per_customer
           WHERE spend > 100 GROUP BY segment) per_segment
         WHERE buyers >= 2 ORDER BY avg_spend DESC, segment"""),
    ("discount_effectiveness",
     f"""SELECT CASE WHEN t.discount_pct > 0 THEN 'discounted' ELSE 'full price' END AS pricing,
         COUNT(*) AS orders, ROUND(AVG(t.total), 2) AS avg_basket, ROUND(AVG(t.total * (1 - t.discount_pct)), 2) AS avg_paid
         FROM {TOTALS} t WHERE t.status <> 'cancelled'
         GROUP BY CASE WHEN t.discount_pct > 0 THEN 'discounted' ELSE 'full price' END ORDER BY pricing"""),
    ("payment_reconciliation",
     f"""SELECT t.id, t.status, ROUND(t.total * (1 - t.discount_pct), 2) AS billed, COALESCE(SUM(p.amount), 0) AS received,
         CASE WHEN t.status = 'cancelled' AND SUM(p.amount) IS NOT NULL THEN 'refund due'
              WHEN SUM(p.amount) IS NULL THEN 'unpaid'
              WHEN ABS(SUM(p.amount) - ROUND(t.total * (1 - t.discount_pct), 2)) < 0.02 THEN 'settled'
              WHEN SUM(p.amount) < ROUND(t.total * (1 - t.discount_pct), 2) THEN 'short' ELSE 'over' END AS state
         FROM {TOTALS} t LEFT JOIN payments p ON p.order_id = t.id
         GROUP BY t.id, t.status, t.total, t.discount_pct ORDER BY t.id"""),
]

CASES = [("strings", STRINGS), ("dates", DATES), ("analyses", ANALYSES)]
