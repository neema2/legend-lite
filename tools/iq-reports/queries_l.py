"""Realistic report queries over the retail schema, part L: whole reports in one statement, as BI users and analysts
write them -- a monthly management pack from five CTEs, a customer health dashboard, a profitability waterfall, a
sales league table, a fulfilment SLA report with correlated subqueries, a reorder plan picking each product's
supplier, last-touch web attribution, a cohort revenue triangle, a unified activity timeline and a price-change
audit. Quotients a report shows are cast to a fixed NUMERIC, as such reports do."""

PACKS = [
    ("monthly_management_pack",
     """WITH lines AS (SELECT o.id AS order_id, DATEFORMAT(o.order_ts, 'yyyy-mm') AS ym,
                             SUM(oi.quantity * oi.unit_price - COALESCE(oi.line_discount, 0)) AS net
                      FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE o.status <> 'cancelled'
                      GROUP BY o.id, DATEFORMAT(o.order_ts, 'yyyy-mm')),
             sales AS (SELECT ym, COUNT(*) AS orders, SUM(net) AS revenue FROM lines GROUP BY ym),
             cash AS (SELECT DATEFORMAT(paid_ts, 'yyyy-mm') AS ym, SUM(amount) AS collected FROM payments GROUP BY DATEFORMAT(paid_ts, 'yyyy-mm')),
             firsts AS (SELECT customer_id, MIN(order_ts) AS first_ts FROM orders GROUP BY customer_id),
             newbies AS (SELECT DATEFORMAT(first_ts, 'yyyy-mm') AS ym, COUNT(*) AS new_customers FROM firsts GROUP BY DATEFORMAT(first_ts, 'yyyy-mm')),
             support AS (SELECT DATEFORMAT(opened_ts, 'yyyy-mm') AS ym, COUNT(*) AS tickets,
                                SUM(CASE WHEN priority = 'urgent' THEN 1 ELSE 0 END) AS urgent
                         FROM support_tickets GROUP BY DATEFORMAT(opened_ts, 'yyyy-mm'))
         SELECT s.ym, s.orders, s.revenue, COALESCE(c.collected, 0) AS collected, COALESCE(n.new_customers, 0) AS new_customers,
           COALESCE(t.tickets, 0) AS tickets, COALESCE(t.urgent, 0) AS urgent,
           s.revenue - LAG(s.revenue) OVER (ORDER BY s.ym) AS revenue_change,
           SUM(s.revenue) OVER (PARTITION BY LEFT(s.ym, 4) ORDER BY s.ym ROWS UNBOUNDED PRECEDING) AS revenue_ytd,
           CAST(ROUND(100.0 * COALESCE(t.tickets, 0) / s.orders, 1) AS NUMERIC(6,1)) AS tickets_per_100_orders
         FROM sales s LEFT JOIN cash c ON c.ym = s.ym LEFT JOIN newbies n ON n.ym = s.ym LEFT JOIN support t ON t.ym = s.ym
         ORDER BY s.ym"""),
    ("customer_health_dashboard",
     """WITH spend AS (SELECT o.customer_id, COUNT(DISTINCT o.id) AS orders, SUM(oi.quantity * oi.unit_price) AS gross, MAX(o.order_ts) AS last_ts
                      FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE o.status <> 'cancelled' GROUP BY o.customer_id),
             paid AS (SELECT o.customer_id, SUM(p.amount) AS paid FROM payments p JOIN orders o ON o.id = p.order_id GROUP BY o.customer_id),
             care AS (SELECT customer_id, COUNT(*) AS tickets, SUM(CASE WHEN closed_ts IS NULL THEN 1 ELSE 0 END) AS open_tickets
                      FROM support_tickets GROUP BY customer_id)
         SELECT c.id, TRIM(c.first_name) || ' ' || c.last_name AS customer, r.name AS region, COALESCE(s.orders, 0) AS orders,
           COALESCE(s.gross, 0) AS gross, COALESCE(p.paid, 0) AS paid, COALESCE(s.gross, 0) - COALESCE(p.paid, 0) AS outstanding,
           DATEDIFF(DAY, s.last_ts, CAST('2026-01-01' AS TIMESTAMP)) AS days_since_last,
           COALESCE(k.tickets, 0) AS tickets, COALESCE(k.open_tickets, 0) AS open_tickets,
           RANK() OVER (PARTITION BY c.region_id ORDER BY COALESCE(s.gross, 0) DESC) AS rank_in_region,
           CASE WHEN s.orders IS NULL THEN 'never bought'
                WHEN DATEDIFF(DAY, s.last_ts, CAST('2026-01-01' AS TIMESTAMP)) > 365 THEN 'lapsed'
                WHEN COALESCE(k.open_tickets, 0) > 0 THEN 'needs care'
                WHEN COALESCE(s.gross, 0) - COALESCE(p.paid, 0) > 0 THEN 'owes' ELSE 'healthy' END AS status,
           (SELECT COUNT(*) FROM web_events w WHERE w.customer_id = c.id) AS web_events
         FROM customers c JOIN regions r ON r.id = c.region_id LEFT JOIN spend s ON s.customer_id = c.id
           LEFT JOIN paid p ON p.customer_id = c.id LEFT JOIN care k ON k.customer_id = c.id
         ORDER BY c.id"""),
    ("product_profitability_waterfall",
     """SELECT p.id, p.name, p.category, SUM(oi.quantity) AS units, SUM(oi.quantity * oi.unit_price) AS gross,
           SUM(COALESCE(oi.line_discount, 0)) AS discounts, SUM(oi.quantity * p.unit_cost) AS cogs,
           SUM(oi.quantity * oi.unit_price) - SUM(COALESCE(oi.line_discount, 0)) - SUM(oi.quantity * p.unit_cost) AS margin,
           CAST(ROUND(100 * (SUM(oi.quantity * oi.unit_price) - SUM(COALESCE(oi.line_discount, 0)) - SUM(oi.quantity * p.unit_cost))
                      / NULLIF(SUM(oi.quantity * oi.unit_price), 0), 2) AS NUMERIC(7,2)) AS margin_pct,
           RANK() OVER (PARTITION BY p.category ORDER BY SUM(oi.quantity * oi.unit_price) - SUM(COALESCE(oi.line_discount, 0))
                                                        - SUM(oi.quantity * p.unit_cost) DESC) AS rank_in_category,
           CAST(ROUND(100 * SUM(SUM(oi.quantity * oi.unit_price)) OVER (ORDER BY SUM(oi.quantity * oi.unit_price) DESC, p.id ROWS UNBOUNDED PRECEDING)
                      / SUM(SUM(oi.quantity * oi.unit_price)) OVER (), 2) AS NUMERIC(7,2)) AS cumulative_gross_pct
         FROM products p JOIN order_items oi ON oi.product_id = p.id JOIN orders o ON o.id = oi.order_id
         WHERE o.status <> 'cancelled' GROUP BY p.id, p.name, p.category ORDER BY margin DESC, p.id"""),
    ("sales_rep_league_table",
     """SELECT e.id, e.first_name || ' ' || e.last_name AS rep, m.last_name AS manager, r.name AS region,
           COUNT(DISTINCT o.id) AS orders, COUNT(DISTINCT o.customer_id) AS customers, SUM(oi.quantity * oi.unit_price) AS revenue,
           MAX(SUM(oi.quantity * oi.unit_price)) OVER () - SUM(oi.quantity * oi.unit_price) AS gap_to_leader,
           NTILE(4) OVER (ORDER BY SUM(oi.quantity * oi.unit_price) DESC, e.id) AS quartile,
           CAST(ROUND(PERCENT_RANK() OVER (ORDER BY SUM(oi.quantity * oi.unit_price)), 3) AS NUMERIC(4,3)) AS pct_rank,
           DENSE_RANK() OVER (PARTITION BY e.region_id ORDER BY COUNT(DISTINCT o.id) DESC) AS rank_by_orders_in_region
         FROM employees e LEFT JOIN employees m ON m.id = e.manager_id LEFT JOIN regions r ON r.id = e.region_id
           JOIN orders o ON o.sales_rep_id = e.id JOIN order_items oi ON oi.order_id = o.id
         WHERE o.status <> 'cancelled' GROUP BY e.id, e.first_name, e.last_name, m.last_name, r.name, e.region_id
         ORDER BY revenue DESC, e.id"""),
    ("order_fulfilment_sla",
     """SELECT o.id, o.channel, CAST(o.order_ts AS DATE) AS ordered, o.ship_date,
           DATEDIFF(DAY, CAST(o.order_ts AS DATE), o.ship_date) AS days_to_ship,
           DATEDIFF(DAY, CAST(o.order_ts AS DATE), o.ship_date) - 2 * DATEDIFF(WEEK, CAST(o.order_ts AS DATE), o.ship_date) AS approx_business_days,
           (SELECT MIN(p.paid_ts) FROM payments p WHERE p.order_id = o.id) AS first_paid,
           (SELECT COALESCE(SUM(p.amount), 0) FROM payments p WHERE p.order_id = o.id) AS paid,
           CASE WHEN o.ship_date IS NULL AND o.status = 'shipped' THEN 'data issue' WHEN o.ship_date IS NULL THEN 'unshipped'
                WHEN DATEDIFF(DAY, CAST(o.order_ts AS DATE), o.ship_date) > 3 THEN 'late' ELSE 'on time' END AS sla,
           SUM(CASE WHEN DATEDIFF(DAY, CAST(o.order_ts AS DATE), o.ship_date) > 3 THEN 1 ELSE 0 END)
             OVER (PARTITION BY DATEFORMAT(o.order_ts, 'yyyy-mm')) AS late_in_month
         FROM orders o WHERE o.order_ts >= '2025-01-01' ORDER BY o.id"""),
]

OPERATIONS = [
    ("reorder_plan_with_suppliers",
     """WITH latest AS (SELECT product_id, on_hand, reorder_point, snapshot_date,
                              ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY snapshot_date DESC, id) AS rn FROM inventory),
             demand AS (SELECT oi.product_id, SUM(oi.quantity) AS units_90d FROM order_items oi JOIN orders o ON o.id = oi.order_id
                        WHERE o.order_ts >= '2025-10-03' AND o.order_ts < '2026-01-01' AND o.status <> 'cancelled' GROUP BY oi.product_id),
             best AS (SELECT sp.product_id, sp.supplier_id, sp.lead_days, sp.unit_cost, sp.min_order,
                             ROW_NUMBER() OVER (PARTITION BY sp.product_id ORDER BY sp.unit_cost, sp.lead_days, sp.id) AS rn
                      FROM supplier_products sp)
         SELECT p.id, p.sku, l.snapshot_date, l.on_hand, l.reorder_point, COALESCE(d.units_90d, 0) AS units_90d,
           CAST(ROUND(COALESCE(d.units_90d, 0) / 90.0, 3) AS NUMERIC(8,3)) AS daily_rate,
           CAST(l.on_hand * 90.0 / NULLIF(d.units_90d, 0) AS NUMERIC(9,1)) AS days_of_cover,
           TRIM(s.name) AS supplier, b.lead_days, b.unit_cost,
           CASE WHEN l.on_hand > l.reorder_point THEN 0
                WHEN COALESCE(b.min_order, 1) > 2 * l.reorder_point - l.on_hand THEN COALESCE(b.min_order, 1)
                ELSE 2 * l.reorder_point - l.on_hand END AS order_qty
         FROM products p JOIN latest l ON l.product_id = p.id AND l.rn = 1 LEFT JOIN demand d ON d.product_id = p.id
           LEFT JOIN best b ON b.product_id = p.id AND b.rn = 1 LEFT JOIN suppliers s ON s.id = b.supplier_id
         ORDER BY p.id"""),
    ("last_touch_attribution",
     """WITH touches AS (SELECT o.id AS order_id, o.customer_id, o.order_ts,
                               (SELECT MAX(w.event_ts) FROM web_events w WHERE w.customer_id = o.customer_id AND w.event_ts <= o.order_ts
                                  AND w.event_ts > DATEADD(DAY, -7, o.order_ts)) AS last_touch
                        FROM orders o WHERE o.status <> 'cancelled'),
             attributed AS (SELECT t.order_id, t.order_ts, t.last_touch,
                                   CASE WHEN t.last_touch IS NULL THEN '(direct)'
                                        WHEN LOCATE(w.url, 'utm_source=') > 0
                                          THEN SUBSTR(w.url || '&', LOCATE(w.url, 'utm_source=') + 11,
                                                      LOCATE(w.url || '&', '&', LOCATE(w.url, 'utm_source=')) - LOCATE(w.url, 'utm_source=') - 11)
                                        WHEN w.referrer IS NOT NULL THEN 'referral' ELSE '(none)' END AS source
                            FROM touches t LEFT JOIN web_events w ON w.customer_id = t.customer_id AND w.event_ts = t.last_touch)
         SELECT a.source, COUNT(*) AS orders, SUM(x.gross) AS revenue,
           CAST(ROUND(AVG(DATEDIFF(MINUTE, a.last_touch, a.order_ts) / 60.0), 1) AS NUMERIC(8,1)) AS avg_hours_to_convert
         FROM attributed a JOIN (SELECT order_id, SUM(quantity * unit_price) AS gross FROM order_items GROUP BY order_id) x ON x.order_id = a.order_id
         GROUP BY a.source ORDER BY orders DESC, a.source"""),
    ("cohort_revenue_triangle",
     """WITH firsts AS (SELECT customer_id, MIN(order_ts) AS first_ts FROM orders WHERE status <> 'cancelled' GROUP BY customer_id),
             rev AS (SELECT o.customer_id, DATEDIFF(MONTH, f.first_ts, o.order_ts) AS age_m, SUM(oi.quantity * oi.unit_price) AS gross
                     FROM orders o JOIN firsts f ON f.customer_id = o.customer_id JOIN order_items oi ON oi.order_id = o.id
                     WHERE o.status <> 'cancelled' GROUP BY o.customer_id, DATEDIFF(MONTH, f.first_ts, o.order_ts))
         SELECT YEAR(f.first_ts) || '-Q' || QUARTER(f.first_ts) AS cohort, COUNT(DISTINCT f.customer_id) AS customers,
           SUM(CASE WHEN r.age_m = 0 THEN r.gross ELSE 0 END) AS m0, SUM(CASE WHEN r.age_m = 1 THEN r.gross ELSE 0 END) AS m1,
           SUM(CASE WHEN r.age_m = 2 THEN r.gross ELSE 0 END) AS m2, SUM(CASE WHEN r.age_m BETWEEN 3 AND 5 THEN r.gross ELSE 0 END) AS m3_to_5,
           SUM(CASE WHEN r.age_m >= 6 THEN r.gross ELSE 0 END) AS m6_plus, SUM(r.gross) AS total
         FROM firsts f JOIN rev r ON r.customer_id = f.customer_id
         GROUP BY YEAR(f.first_ts) || '-Q' || QUARTER(f.first_ts) ORDER BY cohort"""),
    ("unified_activity_timeline",
     """SELECT customer_id, ts, kind, ref_id, detail,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY ts, kind, ref_id) AS seq,
           DATEDIFF(HOUR, LAG(ts) OVER (PARTITION BY customer_id ORDER BY ts, kind, ref_id), ts) AS hours_since_previous
         FROM (SELECT o.customer_id, o.order_ts AS ts, 'order' AS kind, o.id AS ref_id, o.channel || ' ' || o.status AS detail FROM orders o
               UNION ALL SELECT o.customer_id, p.paid_ts, 'payment', p.id, p.method || ' ' || CAST(p.amount AS VARCHAR(12))
                 FROM payments p JOIN orders o ON o.id = p.order_id
               UNION ALL SELECT t.customer_id, t.opened_ts, 'ticket', t.id, t.priority || ': ' || LEFT(t.subject, 30) FROM support_tickets t
               UNION ALL SELECT w.customer_id, w.event_ts, 'web', w.id, w.device FROM web_events w WHERE w.customer_id IS NOT NULL) x
         WHERE customer_id IN (3, 7, 12) ORDER BY customer_id, ts, kind, ref_id"""),
    ("price_change_audit",
     """WITH sold AS (SELECT oi.product_id, o.order_ts, oi.id AS line_id, oi.unit_price,
                            LAG(oi.unit_price) OVER (PARTITION BY oi.product_id ORDER BY o.order_ts, oi.id) AS prev_price
                     FROM order_items oi JOIN orders o ON o.id = oi.order_id)
         SELECT p.id, p.name, p.unit_price AS list_price, COUNT(*) AS lines,
           SUM(CASE WHEN s.prev_price IS NOT NULL AND s.unit_price <> s.prev_price THEN 1 ELSE 0 END) AS price_changes,
           MIN(s.unit_price) AS lowest, MAX(s.unit_price) AS highest,
           CAST(ROUND(100 * (p.unit_price - MIN(s.unit_price)) / p.unit_price, 1) AS NUMERIC(5,1)) AS deepest_cut_pct,
           MAX(CASE WHEN s.unit_price < p.unit_price THEN s.order_ts END) AS last_below_list
         FROM products p JOIN sold s ON s.product_id = p.id GROUP BY p.id, p.name, p.unit_price ORDER BY price_changes DESC, p.id"""),
]

CASES = [("whole_reports", PACKS), ("whole_operations", OPERATIONS)]
