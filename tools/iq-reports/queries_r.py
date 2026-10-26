"""Realistic report queries over the retail schema, part R: windows and sets as analysts reach for them -- a moving
sum over a RANGE of order ids, LEAD and LAG with defaults, weeks starting on Sunday by DOW, customers who bought in
both years (INTERSECT) and only in one (EXCEPT), a ROLLUP whose totals GROUPING() labels, a two-level management
chain by self-joins, a running count of distinct-day activity, first-touch attribution by FIRST_VALUE, and price
bands by a ratio of each product to its category's average. Quotients a report shows are cast to a fixed NUMERIC,
as such reports do."""

WINDOWS = [
    ("order_value_moving_sum_over_id_range",
     """WITH totals AS (
           SELECT o.id, o.customer_id, SUM(oi.quantity * oi.unit_price) AS total
           FROM orders o JOIN order_items oi ON oi.order_id = o.id GROUP BY o.id, o.customer_id)
         SELECT id, total,
           SUM(total) OVER (ORDER BY id RANGE BETWEEN 4 PRECEDING AND CURRENT ROW) AS last_five_ids_total,
           COUNT(*) OVER (ORDER BY id RANGE BETWEEN 4 PRECEDING AND CURRENT ROW) AS orders_in_range
         FROM totals WHERE id <= 30 ORDER BY id"""),
    ("previous_and_next_order_with_defaults",
     """SELECT o.customer_id, o.id, CAST(o.order_ts AS DATE) AS ordered,
           LAG(o.id, 1, 0) OVER (PARTITION BY o.customer_id ORDER BY o.order_ts, o.id) AS previous_order,
           LEAD(o.id, 1, 0) OVER (PARTITION BY o.customer_id ORDER BY o.order_ts, o.id) AS next_order,
           LEAD(o.status, 1, 'none') OVER (PARTITION BY o.customer_id ORDER BY o.order_ts, o.id) AS next_status
         FROM orders o WHERE o.customer_id <= 6 ORDER BY o.customer_id, o.order_ts, o.id"""),
    ("weekly_orders_weeks_starting_sunday",
     """SELECT DATEADD(DAY, 1 - DOW(CAST(o.order_ts AS DATE)), CAST(o.order_ts AS DATE)) AS week_start,
           COUNT(*) AS orders, COUNT(DISTINCT o.customer_id) AS customers,
           SUM(CASE WHEN o.channel = 'web' THEN 1 ELSE 0 END) AS web_orders
         FROM orders o WHERE o.order_ts >= '2025-09-01' AND o.order_ts < '2025-11-01'
         GROUP BY DATEADD(DAY, 1 - DOW(CAST(o.order_ts AS DATE)), CAST(o.order_ts AS DATE))
         ORDER BY 1"""),
    ("first_touch_attribution",
     """WITH touches AS (
           SELECT w.customer_id, w.event_ts,
                  CASE WHEN w.referrer IS NULL THEN 'direct'
                       WHEN LOCATE(w.referrer, 'google') > 0 THEN 'search'
                       WHEN LOCATE(w.referrer, 'facebook') > 0 THEN 'social'
                       WHEN LOCATE(w.referrer, 'mail') > 0 THEN 'email' ELSE 'partner' END AS channel,
                  FIRST_VALUE(CASE WHEN w.referrer IS NULL THEN 'direct'
                       WHEN LOCATE(w.referrer, 'google') > 0 THEN 'search'
                       WHEN LOCATE(w.referrer, 'facebook') > 0 THEN 'social'
                       WHEN LOCATE(w.referrer, 'mail') > 0 THEN 'email' ELSE 'partner' END)
                    OVER (PARTITION BY w.customer_id ORDER BY w.event_ts, w.id
                          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_touch
           FROM web_events w WHERE w.customer_id IS NOT NULL)
         SELECT first_touch, COUNT(DISTINCT customer_id) AS customers, COUNT(*) AS events
         FROM touches GROUP BY first_touch ORDER BY customers DESC, first_touch"""),
    ("active_days_running_count",
     """WITH days AS (
           SELECT DISTINCT w.customer_id, CAST(w.event_ts AS DATE) AS d
           FROM web_events w WHERE w.customer_id IS NOT NULL AND w.customer_id <= 8)
         SELECT customer_id, d,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY d) AS active_day_number,
           DATEDIFF(DAY, LAG(d) OVER (PARTITION BY customer_id ORDER BY d), d) AS days_since_last
         FROM days ORDER BY customer_id, d"""),
    ("price_bands_against_category_average",
     """SELECT p.category, p.id, p.name, p.unit_price,
           CAST(AVG(p.unit_price) OVER (PARTITION BY p.category) AS NUMERIC(10,2)) AS category_average,
           CAST(p.unit_price / AVG(p.unit_price) OVER (PARTITION BY p.category) AS NUMERIC(6,2)) AS price_index,
           CASE WHEN p.unit_price >= 1.5 * AVG(p.unit_price) OVER (PARTITION BY p.category) THEN 'premium'
                WHEN p.unit_price <= 0.5 * AVG(p.unit_price) OVER (PARTITION BY p.category) THEN 'budget'
                ELSE 'core' END AS band
         FROM products p ORDER BY p.category, p.unit_price DESC, p.id"""),
]

SETS = [
    ("customers_buying_in_both_years",
     """SELECT customer_id FROM orders WHERE YEAR(order_ts) = 2024 AND status <> 'cancelled'
         INTERSECT
         SELECT customer_id FROM orders WHERE YEAR(order_ts) = 2025 AND status <> 'cancelled'
         ORDER BY 1"""),
    ("customers_lost_after_2024",
     """SELECT c.id, TRIM(c.last_name) AS last_name
         FROM customers c
         WHERE c.id IN (SELECT customer_id FROM orders WHERE YEAR(order_ts) = 2024
                        EXCEPT
                        SELECT customer_id FROM orders WHERE YEAR(order_ts) = 2025)
         ORDER BY c.id"""),
    ("revenue_rollup_with_labels",
     """SELECT CASE WHEN GROUPING(p.category) = 1 THEN 'All categories' ELSE p.category END AS category,
           CASE WHEN GROUPING(o.channel) = 1 THEN 'All channels' ELSE o.channel END AS channel,
           COUNT(DISTINCT o.id) AS orders, SUM(oi.quantity * oi.unit_price) AS revenue
         FROM orders o JOIN order_items oi ON oi.order_id = o.id JOIN products p ON p.id = oi.product_id
         WHERE o.status <> 'cancelled'
         GROUP BY ROLLUP(p.category, o.channel)
         ORDER BY GROUPING(p.category), p.category, GROUPING(o.channel), o.channel"""),
    ("management_chain_two_levels",
     """SELECT e.id, TRIM(e.first_name) || ' ' || TRIM(e.last_name) AS employee, e.title,
           COALESCE(TRIM(m.first_name) || ' ' || TRIM(m.last_name), '(none)') AS manager,
           COALESCE(TRIM(g.first_name) || ' ' || TRIM(g.last_name), '(none)') AS managers_manager,
           CASE WHEN m.id IS NULL THEN 1 WHEN g.id IS NULL THEN 2 ELSE 3 END AS level
         FROM employees e LEFT JOIN employees m ON m.id = e.manager_id LEFT JOIN employees g ON g.id = m.manager_id
         ORDER BY level, e.id"""),
    ("products_never_ordered_or_only_cancelled",
     """SELECT p.id, p.name, 'never ordered' AS reason FROM products p
         WHERE NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id)
         UNION ALL
         SELECT p.id, p.name, 'only cancelled orders' FROM products p
         WHERE EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id)
           AND NOT EXISTS (SELECT 1 FROM order_items oi JOIN orders o ON o.id = oi.order_id
                           WHERE oi.product_id = p.id AND o.status <> 'cancelled')
         ORDER BY 1"""),
]

CASES = [("window_reports", WINDOWS), ("set_reports", SETS)]
