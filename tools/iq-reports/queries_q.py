"""Realistic report queries over the retail schema, part Q: marketing and planning reports, as analysts write them --
an RFM segmentation by NTILE, UTM parameters parsed out of landing URLs, a fiscal calendar that starts in July, the
top three products of each category by ROW_NUMBER, each region's distinct segments and channels listed, an hour by
weekday heat map of web traffic, month-two retention of signup cohorts, the median days to ship by channel, a
reorder plan against supplier lead times, and a sales-rep leaderboard with its gap to the leader. Quotients a report
shows are cast to a fixed NUMERIC, as such reports do."""

MARKETING = [
    ("rfm_segments",
     """WITH base AS (
           SELECT o.customer_id, MAX(CAST(o.order_ts AS DATE)) AS last_order, COUNT(*) AS orders,
                  SUM(oi.quantity * oi.unit_price) AS spent
           FROM orders o JOIN order_items oi ON oi.order_id = o.id
           WHERE o.status <> 'cancelled'
           GROUP BY o.customer_id),
         scored AS (
           SELECT customer_id, last_order, orders, spent,
                  NTILE(3) OVER (ORDER BY last_order, customer_id) AS r,
                  NTILE(3) OVER (ORDER BY orders, customer_id) AS f,
                  NTILE(3) OVER (ORDER BY spent, customer_id) AS m
           FROM base)
         SELECT customer_id, r, f, m, CAST(r AS VARCHAR(1)) || CAST(f AS VARCHAR(1)) || CAST(m AS VARCHAR(1)) AS rfm,
           CASE WHEN r = 3 AND f = 3 AND m = 3 THEN 'champion' WHEN r = 3 AND f >= 2 THEN 'loyal'
                WHEN r = 1 AND m = 3 THEN 'at risk' WHEN r = 1 THEN 'lapsed' ELSE 'regular' END AS segment
         FROM scored ORDER BY customer_id"""),
    ("utm_sources_of_first_events",
     """WITH landing AS (
           SELECT w.id, w.customer_id, w.device, LOWER(w.url) AS url
           FROM web_events w WHERE w.referrer IS NOT NULL OR LOCATE(LOWER(w.url), 'utm_source=') > 0),
         parsed AS (
           SELECT id, device,
                  CASE WHEN LOCATE(url, 'utm_source=') = 0 THEN NULL
                       ELSE SUBSTRING(url, LOCATE(url, 'utm_source=') + 11,
                            CASE WHEN LOCATE(url, '&', LOCATE(url, 'utm_source=')) = 0 THEN 40
                                 ELSE LOCATE(url, '&', LOCATE(url, 'utm_source=')) - LOCATE(url, 'utm_source=') - 11 END) END AS source,
                  CASE WHEN LOCATE(url, 'utm_medium=') = 0 THEN NULL
                       ELSE SUBSTRING(url, LOCATE(url, 'utm_medium=') + 11,
                            CASE WHEN LOCATE(url, '&', LOCATE(url, 'utm_medium=')) = 0 THEN 40
                                 ELSE LOCATE(url, '&', LOCATE(url, 'utm_medium=')) - LOCATE(url, 'utm_medium=') - 11 END) END AS medium
           FROM landing)
         SELECT COALESCE(source, '(none)') AS utm_source, COALESCE(medium, '(none)') AS utm_medium, COUNT(*) AS landings,
           SUM(CASE WHEN device = 'mobile' THEN 1 ELSE 0 END) AS on_mobile
         FROM parsed GROUP BY COALESCE(source, '(none)'), COALESCE(medium, '(none)') ORDER BY landings DESC, 1, 2"""),
    ("traffic_heatmap_hour_by_weekday",
     """SELECT DATEPART(HOUR, w.event_ts) / 6 * 6 AS from_hour,
           SUM(CASE WHEN DOW(w.event_ts) = 1 THEN 1 ELSE 0 END) AS sun,
           SUM(CASE WHEN DOW(w.event_ts) = 2 THEN 1 ELSE 0 END) AS mon,
           SUM(CASE WHEN DOW(w.event_ts) = 3 THEN 1 ELSE 0 END) AS tue,
           SUM(CASE WHEN DOW(w.event_ts) = 4 THEN 1 ELSE 0 END) AS wed,
           SUM(CASE WHEN DOW(w.event_ts) = 5 THEN 1 ELSE 0 END) AS thu,
           SUM(CASE WHEN DOW(w.event_ts) = 6 THEN 1 ELSE 0 END) AS fri,
           SUM(CASE WHEN DOW(w.event_ts) = 7 THEN 1 ELSE 0 END) AS sat,
           COUNT(*) AS events
         FROM web_events w GROUP BY DATEPART(HOUR, w.event_ts) / 6 * 6 ORDER BY 1"""),
    ("signup_cohort_month_two_retention",
     """WITH cohorts AS (
           SELECT c.id, DATEFORMAT(c.signup_date, 'yyyy-mm') AS cohort, c.signup_date FROM customers c),
         active AS (
           SELECT DISTINCT o.customer_id, DATEDIFF(MONTH, ch.signup_date, o.order_ts) AS months_after
           FROM orders o JOIN cohorts ch ON ch.id = o.customer_id WHERE o.status <> 'cancelled')
         SELECT ch.cohort, COUNT(*) AS customers,
           COUNT(DISTINCT CASE WHEN a.months_after BETWEEN 0 AND 1 THEN a.customer_id END) AS active_first_two_months,
           COUNT(DISTINCT CASE WHEN a.months_after BETWEEN 2 AND 12 THEN a.customer_id END) AS active_months_3_to_13
         FROM cohorts ch LEFT JOIN active a ON a.customer_id = ch.id
         GROUP BY ch.cohort ORDER BY ch.cohort"""),
    ("region_directory_of_segments_and_channels",
     """SELECT r.name AS region, COUNT(DISTINCT c.id) AS customers,
           LIST(DISTINCT c.segment, ', ' ORDER BY c.segment) AS segments,
           LIST(DISTINCT o.channel, ', ' ORDER BY o.channel) AS channels,
           LIST(DISTINCT c.city, ' / ' ORDER BY c.city DESC) AS cities
         FROM regions r JOIN customers c ON c.region_id = r.id LEFT JOIN orders o ON o.customer_id = c.id
         GROUP BY r.name ORDER BY r.name"""),
]

PLANNING = [
    ("fiscal_year_starting_july",
     """SELECT YEAR(DATEADD(MONTH, 6, o.order_ts)) AS fiscal_year,
           'Q' || CAST(QUARTER(DATEADD(MONTH, 6, o.order_ts)) AS VARCHAR(1)) AS fiscal_quarter,
           COUNT(DISTINCT o.id) AS orders, SUM(oi.quantity * oi.unit_price) AS revenue,
           MIN(CAST(o.order_ts AS DATE)) AS first_day, MAX(CAST(o.order_ts AS DATE)) AS last_day
         FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE o.status <> 'cancelled'
         GROUP BY YEAR(DATEADD(MONTH, 6, o.order_ts)), QUARTER(DATEADD(MONTH, 6, o.order_ts))
         ORDER BY 1, 2"""),
    ("category_top_three_by_revenue",
     """WITH ranked AS (
           SELECT p.category, p.id, p.name, SUM(oi.quantity) AS units, SUM(oi.quantity * oi.unit_price) AS revenue,
                  ROW_NUMBER() OVER (PARTITION BY p.category ORDER BY SUM(oi.quantity * oi.unit_price) DESC, p.id) AS rn
           FROM products p JOIN order_items oi ON oi.product_id = p.id JOIN orders o ON o.id = oi.order_id
           WHERE o.status <> 'cancelled'
           GROUP BY p.category, p.id, p.name)
         SELECT category, rn, id, name, units, revenue,
           CAST(100 * revenue / SUM(revenue) OVER (PARTITION BY category) AS NUMERIC(5,1)) AS share_of_top_three
         FROM ranked WHERE rn <= 3 ORDER BY category, rn"""),
    ("median_days_to_ship_by_channel",
     """SELECT o.channel, COUNT(*) AS shipped,
           MEDIAN(DATEDIFF(DAY, CAST(o.order_ts AS DATE), o.ship_date)) AS median_days,
           MIN(DATEDIFF(DAY, CAST(o.order_ts AS DATE), o.ship_date)) AS fastest,
           MAX(DATEDIFF(DAY, CAST(o.order_ts AS DATE), o.ship_date)) AS slowest,
           SUM(CASE WHEN DATEDIFF(DAY, CAST(o.order_ts AS DATE), o.ship_date) > 5 THEN 1 ELSE 0 END) AS over_five_days
         FROM orders o WHERE o.ship_date IS NOT NULL GROUP BY o.channel ORDER BY o.channel"""),
    ("reorder_plan_against_lead_times",
     """WITH latest AS (
           SELECT i.product_id, i.on_hand, i.reorder_point,
                  ROW_NUMBER() OVER (PARTITION BY i.product_id ORDER BY i.snapshot_date DESC, i.id DESC) AS rn
           FROM inventory i),
         best_supplier AS (
           SELECT sp.product_id, sp.supplier_id, sp.lead_days, sp.unit_cost, COALESCE(sp.min_order, 1) AS min_order,
                  ROW_NUMBER() OVER (PARTITION BY sp.product_id ORDER BY sp.lead_days, sp.unit_cost, sp.id) AS pick
           FROM supplier_products sp)
         SELECT p.id, p.name, l.on_hand, l.reorder_point, b.supplier_id, b.lead_days,
           GREATER(l.reorder_point * 2 - l.on_hand, b.min_order) AS order_qty,
           CAST(GREATER(l.reorder_point * 2 - l.on_hand, b.min_order) * b.unit_cost AS NUMERIC(10,2)) AS order_cost,
           DATEADD(DAY, b.lead_days, CAST('2025-12-31' AS DATE)) AS expected_arrival
         FROM products p JOIN latest l ON l.product_id = p.id AND l.rn = 1
           JOIN best_supplier b ON b.product_id = p.id AND b.pick = 1
         WHERE l.on_hand <= l.reorder_point
         ORDER BY b.lead_days DESC, p.id"""),
    ("sales_rep_leaderboard",
     """WITH reps AS (
           SELECT e.id, TRIM(e.first_name) || ' ' || TRIM(e.last_name) AS rep, r.name AS region,
                  COUNT(DISTINCT o.id) AS orders, COALESCE(SUM(oi.quantity * oi.unit_price), 0) AS revenue
           FROM employees e LEFT JOIN regions r ON r.id = e.region_id
             LEFT JOIN orders o ON o.sales_rep_id = e.id AND o.status <> 'cancelled'
             LEFT JOIN order_items oi ON oi.order_id = o.id
           GROUP BY e.id, e.first_name, e.last_name, r.name)
         SELECT RANK() OVER (ORDER BY revenue DESC) AS place, rep, COALESCE(region, '(none)') AS region, orders, revenue,
           MAX(revenue) OVER () - revenue AS behind_leader,
           CAST(CASE WHEN orders = 0 THEN NULL ELSE revenue / orders END AS NUMERIC(10,2)) AS average_order
         FROM reps ORDER BY revenue DESC, id"""),
    ("orders_with_open_tickets_and_late_shipping",
     """SELECT o.id, o.customer_id, o.status, CAST(o.order_ts AS DATE) AS ordered,
           DATEDIFF(DAY, CAST(o.order_ts AS DATE), COALESCE(o.ship_date, CAST('2025-12-31' AS DATE))) AS days_waiting,
           (SELECT COUNT(*) FROM support_tickets t WHERE t.order_id = o.id AND t.closed_ts IS NULL) AS open_tickets,
           (SELECT MAX(t.priority) FROM support_tickets t WHERE t.order_id = o.id) AS top_priority
         FROM orders o
         WHERE EXISTS (SELECT 1 FROM support_tickets t WHERE t.order_id = o.id)
           AND o.status IN ('pending', 'shipped')
         ORDER BY days_waiting DESC, o.id"""),
]

CASES = [("marketing_reports", MARKETING), ("planning_reports", PLANNING)]
