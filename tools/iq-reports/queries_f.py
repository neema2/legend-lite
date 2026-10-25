"""Realistic report queries over the retail schema, part F: the classic constructions analysts combine -- EXISTS inside a
CASE, a HAVING against a subquery, INTERSECT and EXCEPT cohorts, date truncation by DATEADD of a DATEDIFF, a median by
row numbers, ABC classes by cumulative share, basket pairs with support and confidence, a twelve-month pivot,
year-over-year self-joins, and chains of derived tables with a window at each level."""

LINE = "oi.quantity * oi.unit_price - oi.line_discount"
SHIPPED_LINES = """FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE o.status = 'shipped'"""

COMBINATIONS = [
    ("customer_flags_by_exists",
     """SELECT c.id, c.last_name,
           CASE WHEN r.returns > 0 THEN 1 ELSE 0 END AS has_return,
           CASE WHEN t.open_tickets > 0 THEN 1 ELSE 0 END AS open_ticket,
           CASE WHEN ph.customer_id IS NOT NULL THEN 'phone buyer' ELSE '' END AS phone
         FROM customers c
           LEFT JOIN (SELECT customer_id, COUNT(*) AS returns FROM orders WHERE status = 'returned' GROUP BY customer_id) r
             ON r.customer_id = c.id
           LEFT JOIN (SELECT customer_id, COUNT(*) AS open_tickets FROM support_tickets WHERE closed_ts IS NULL
                      GROUP BY customer_id) t ON t.customer_id = c.id
           LEFT JOIN (SELECT DISTINCT customer_id FROM orders WHERE channel = 'phone') ph ON ph.customer_id = c.id
         ORDER BY c.id"""),
    ("categories_above_average_revenue",
     f"""SELECT p.category, SUM({LINE}) AS revenue
         FROM order_items oi JOIN products p ON p.id = oi.product_id JOIN orders o ON o.id = oi.order_id
         WHERE o.status = 'shipped'
         GROUP BY p.category
         HAVING SUM({LINE}) > (SELECT SUM(oi2.quantity * oi2.unit_price - oi2.line_discount) / COUNT(DISTINCT p2.category)
                               FROM order_items oi2 JOIN products p2 ON p2.id = oi2.product_id
                               JOIN orders o2 ON o2.id = oi2.order_id WHERE o2.status = 'shipped')
         ORDER BY revenue DESC, p.category"""),
    ("bought_in_2024_and_2025",
     """SELECT c.id FROM customers c
         WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND YEAR(o.order_ts) = 2024 AND o.status = 'shipped')
           AND EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND YEAR(o.order_ts) = 2025 AND o.status = 'shipped')
         ORDER BY c.id"""),
    ("bought_in_2024_not_2025",
     """SELECT DISTINCT o.customer_id FROM orders o
         WHERE YEAR(o.order_ts) = 2024 AND o.status <> 'cancelled'
           AND NOT EXISTS (SELECT 1 FROM orders o2 WHERE o2.customer_id = o.customer_id AND YEAR(o2.order_ts) = 2025
                             AND o2.status <> 'cancelled')
         ORDER BY o.customer_id"""),
    ("quarter_starts_by_datediff",
     f"""SELECT DATEADD(QUARTER, DATEDIFF(QUARTER, '2000-01-01', o.order_ts), '2000-01-01') AS quarter_start,
           COUNT(DISTINCT o.id) AS orders, SUM({LINE}) AS revenue
         {SHIPPED_LINES}
         GROUP BY DATEADD(QUARTER, DATEDIFF(QUARTER, '2000-01-01', o.order_ts), '2000-01-01') ORDER BY quarter_start"""),
    ("month_starts_and_week_numbers",
     """SELECT o.id, DATEADD(MONTH, DATEDIFF(MONTH, '2000-01-01', o.order_ts), '2000-01-01') AS month_start,
           DATEDIFF(WEEK, '2025-01-05', o.order_ts) + 1 AS fiscal_week,
           DATEDIFF(DAY, DATEADD(MONTH, DATEDIFF(MONTH, '2000-01-01', o.order_ts), '2000-01-01'), o.order_ts) + 1 AS day_of_month
         FROM orders o WHERE o.order_ts >= '2025-01-05' AND o.id <= 60 ORDER BY o.id"""),
    ("median_order_value_by_row_numbers",
     f"""WITH totals AS (SELECT o.channel, o.id, SUM({LINE}) AS total {SHIPPED_LINES} GROUP BY o.channel, o.id),
         ranked AS (SELECT channel, id, total, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY total, id) AS rn,
                           COUNT(*) OVER (PARTITION BY channel) AS n FROM totals),
         middles AS (SELECT channel, total, rn, n, (n + 1) / 2 AS low_middle, (n + 2) / 2 AS high_middle FROM ranked)
         SELECT channel, AVG(total) AS median_total, MAX(n) AS orders
         FROM middles WHERE rn = low_middle OR rn = high_middle GROUP BY channel ORDER BY channel"""),
    ("abc_classes_by_cumulative_share",
     f"""WITH revenue AS (SELECT oi.product_id, SUM({LINE}) AS revenue {SHIPPED_LINES} GROUP BY oi.product_id),
         running AS (SELECT product_id, revenue,
                            SUM(revenue) OVER (ORDER BY revenue DESC, product_id ROWS UNBOUNDED PRECEDING) AS cumulative,
                            SUM(revenue) OVER () AS grand FROM revenue)
         SELECT r.product_id, p.name, r.revenue, ROUND(100.0 * r.cumulative / r.grand, 1) AS cumulative_pct,
           CASE WHEN r.cumulative <= 0.7 * r.grand THEN 'A' WHEN r.cumulative <= 0.9 * r.grand THEN 'B' ELSE 'C' END AS abc_class
         FROM running r JOIN products p ON p.id = r.product_id ORDER BY r.revenue DESC, r.product_id"""),
    ("basket_pairs_support_and_confidence",
     """WITH baskets AS (SELECT DISTINCT oi.order_id, oi.product_id FROM order_items oi JOIN orders o ON o.id = oi.order_id
                         WHERE o.status <> 'cancelled'),
         pairs AS (SELECT a.product_id AS p1, b.product_id AS p2, COUNT(*) AS together
                   FROM baskets a JOIN baskets b ON b.order_id = a.order_id AND a.product_id < b.product_id
                   GROUP BY a.product_id, b.product_id),
         singles AS (SELECT product_id, COUNT(*) AS orders FROM baskets GROUP BY product_id)
         SELECT x.p1, x.p2, x.together, s.orders AS p1_orders, ROUND(1.0 * x.together / s.orders, 3) AS confidence
         FROM pairs x JOIN singles s ON s.product_id = x.p1 WHERE x.together >= 2 ORDER BY x.together DESC, x.p1, x.p2"""),
    ("twelve_month_pivot_2025",
     f"""SELECT p.category,
           SUM(CASE WHEN MONTH(o.order_ts) = 1 THEN {LINE} ELSE 0 END) AS jan, SUM(CASE WHEN MONTH(o.order_ts) = 2 THEN {LINE} ELSE 0 END) AS feb,
           SUM(CASE WHEN MONTH(o.order_ts) = 3 THEN {LINE} ELSE 0 END) AS mar, SUM(CASE WHEN MONTH(o.order_ts) = 4 THEN {LINE} ELSE 0 END) AS apr,
           SUM(CASE WHEN MONTH(o.order_ts) = 5 THEN {LINE} ELSE 0 END) AS may, SUM(CASE WHEN MONTH(o.order_ts) = 6 THEN {LINE} ELSE 0 END) AS jun,
           SUM(CASE WHEN MONTH(o.order_ts) = 7 THEN {LINE} ELSE 0 END) AS jul, SUM(CASE WHEN MONTH(o.order_ts) = 8 THEN {LINE} ELSE 0 END) AS aug,
           SUM(CASE WHEN MONTH(o.order_ts) = 9 THEN {LINE} ELSE 0 END) AS sep, SUM(CASE WHEN MONTH(o.order_ts) = 10 THEN {LINE} ELSE 0 END) AS oct,
           SUM(CASE WHEN MONTH(o.order_ts) = 11 THEN {LINE} ELSE 0 END) AS nov, SUM(CASE WHEN MONTH(o.order_ts) = 12 THEN {LINE} ELSE 0 END) AS dec_,
           SUM({LINE}) AS total
         FROM orders o JOIN order_items oi ON oi.order_id = o.id JOIN products p ON p.id = oi.product_id
         WHERE o.status = 'shipped' AND YEAR(o.order_ts) = 2025 GROUP BY p.category ORDER BY p.category"""),
    ("year_over_year_by_self_join",
     f"""WITH yearly AS (SELECT o.customer_id, YEAR(o.order_ts) AS yr, YEAR(o.order_ts) - 1 AS prev_yr, SUM({LINE}) AS revenue
                        {SHIPPED_LINES} GROUP BY o.customer_id, YEAR(o.order_ts))
         SELECT cur.customer_id, cur.yr, cur.revenue, prev.revenue AS prev_revenue, cur.revenue - COALESCE(prev.revenue, 0) AS change,
           CASE WHEN prev.revenue IS NULL THEN 'new' WHEN cur.revenue >= prev.revenue THEN 'growing' ELSE 'shrinking' END AS trend
         FROM yearly cur LEFT JOIN yearly prev ON prev.customer_id = cur.customer_id AND prev.yr = cur.prev_yr
         WHERE cur.yr >= 2024 ORDER BY cur.customer_id, cur.yr"""),
    ("lapsed_customers_not_exists",
     """SELECT c.id, c.last_name, MAX(o.order_ts) AS last_order
         FROM customers c JOIN orders o ON o.customer_id = c.id
         WHERE NOT EXISTS (SELECT 1 FROM orders o2 WHERE o2.customer_id = c.id AND o2.order_ts >= '2025-07-01')
         GROUP BY c.id, c.last_name ORDER BY last_order, c.id"""),
    ("derived_chain_with_windows",
     f"""SELECT region, customer_id, revenue, region_rank, share_of_region, CASE WHEN region_rank = 1 THEN 'leader' ELSE '' END AS badge
         FROM (SELECT region, customer_id, revenue, RANK() OVER (PARTITION BY region ORDER BY revenue DESC) AS region_rank,
                      ROUND(100.0 * revenue / SUM(revenue) OVER (PARTITION BY region), 1) AS share_of_region
               FROM (SELECT r.name AS region, c.id AS customer_id, SUM({LINE}) AS revenue
                     FROM regions r JOIN customers c ON c.region_id = r.id JOIN orders o ON o.customer_id = c.id
                       JOIN order_items oi ON oi.order_id = o.id
                     WHERE o.status = 'shipped' GROUP BY r.name, c.id) by_customer) ranked
         WHERE region_rank <= 2 ORDER BY region, region_rank, customer_id"""),
    ("top_two_with_ties",
     f"""SELECT category, product_id, units, rnk FROM (
           SELECT p.category, p.id AS product_id, SUM(oi.quantity) AS units,
                  RANK() OVER (PARTITION BY p.category ORDER BY SUM(oi.quantity) DESC) AS rnk
           FROM order_items oi JOIN products p ON p.id = oi.product_id GROUP BY p.category, p.id) x
         WHERE rnk <= 2 ORDER BY category, rnk, product_id"""),
    ("orders_with_max_line_by_correlation",
     """SELECT l.order_id, l.id, l.product_id, l.line_value
         FROM (SELECT id, order_id, product_id, quantity * unit_price AS line_value FROM order_items) l
         WHERE l.line_value = (SELECT MAX(x.quantity * x.unit_price) FROM order_items x WHERE x.order_id = l.order_id)
           AND l.order_id <= 40
         ORDER BY l.order_id, l.id"""),
    ("union_summary_wrapped_and_totalled",
     """SELECT source, SUM(events) AS events, MIN(first_seen) AS first_seen FROM (
           SELECT 'order' AS source, COUNT(*) AS events, MIN(order_ts) AS first_seen FROM orders
           UNION ALL SELECT 'payment', COUNT(*), MIN(paid_ts) FROM payments
           UNION ALL SELECT 'ticket', COUNT(*), MIN(opened_ts) FROM support_tickets
           UNION ALL SELECT 'web', COUNT(*), MIN(event_ts) FROM web_events) u
         GROUP BY source ORDER BY source"""),
    ("customer_tenure_segments",
     """SELECT CASE WHEN DATEDIFF(MONTH, c.signup_date, '2026-01-01') < 12 THEN 'under 1 year'
                     WHEN DATEDIFF(MONTH, c.signup_date, '2026-01-01') < 24 THEN '1-2 years' ELSE '2+ years' END AS tenure,
           COUNT(DISTINCT c.id) AS customers, COUNT(o.id) AS orders,
           ROUND(1.0 * COUNT(o.id) / COUNT(DISTINCT c.id), 2) AS orders_per_customer
         FROM customers c LEFT JOIN orders o ON o.customer_id = c.id AND o.status <> 'cancelled'
         GROUP BY CASE WHEN DATEDIFF(MONTH, c.signup_date, '2026-01-01') < 12 THEN 'under 1 year'
                       WHEN DATEDIFF(MONTH, c.signup_date, '2026-01-01') < 24 THEN '1-2 years' ELSE '2+ years' END
         ORDER BY tenure"""),
    ("rep_quota_attainment",
     f"""WITH quota AS (SELECT id, first_name || ' ' || last_name AS rep, salary * 3 AS annual_quota FROM employees),
         sold AS (SELECT o.sales_rep_id, SUM({LINE}) AS revenue {SHIPPED_LINES} AND YEAR(o.order_ts) = 2025 GROUP BY o.sales_rep_id)
         SELECT q.id, q.rep, q.annual_quota, COALESCE(s.revenue, 0) AS revenue,
           ROUND(100.0 * COALESCE(s.revenue, 0) / q.annual_quota, 1) AS attainment_pct,
           NTILE(3) OVER (ORDER BY COALESCE(s.revenue, 0) DESC, q.id) AS tier,
           DENSE_RANK() OVER (ORDER BY COALESCE(s.revenue, 0) DESC) AS dense_rank
         FROM quota q LEFT JOIN sold s ON s.sales_rep_id = q.id ORDER BY revenue DESC, q.id"""),
    ("first_order_channel_mix",
     """SELECT f.channel, COUNT(*) AS first_orders, SUM(CASE WHEN n.orders > 1 THEN 1 ELSE 0 END) AS came_back
         FROM orders f JOIN (SELECT customer_id, COUNT(*) AS orders FROM orders GROUP BY customer_id) n
           ON n.customer_id = f.customer_id
         WHERE f.order_ts = (SELECT MIN(o.order_ts) FROM orders o WHERE o.customer_id = f.customer_id)
         GROUP BY f.channel ORDER BY f.channel"""),
    ("ticket_rate_per_hundred_orders",
     """SELECT r.country, COUNT(DISTINCT o.id) AS orders, COUNT(DISTINCT t.id) AS tickets,
           CASE WHEN COUNT(DISTINCT o.id) = 0 THEN NULL ELSE ROUND(100.0 * COUNT(DISTINCT t.id) / COUNT(DISTINCT o.id), 1) END AS tickets_per_100
         FROM regions r JOIN customers c ON c.region_id = r.id LEFT JOIN orders o ON o.customer_id = c.id
           LEFT JOIN support_tickets t ON t.order_id = o.id
         GROUP BY r.country ORDER BY r.country"""),
    ("inventory_versus_sales_by_month",
     """SELECT i.snapshot_date, SUM(i.on_hand) AS units_on_hand,
           (SELECT SUM(oi.quantity) FROM order_items oi JOIN orders o ON o.id = oi.order_id
             WHERE o.status <> 'cancelled' AND YEAR(o.order_ts) = YEAR(i.snapshot_date) AND MONTH(o.order_ts) = MONTH(i.snapshot_date)) AS units_sold
         FROM inventory i GROUP BY i.snapshot_date ORDER BY i.snapshot_date"""),
    ("orders_between_two_dates_inclusive",
     """SELECT CAST(order_ts AS DATE) AS day, COUNT(*) AS orders, LIST(CAST(id AS VARCHAR(5)), ',' ORDER BY id) AS ids
         FROM orders WHERE CAST(order_ts AS DATE) BETWEEN '2025-11-01' AND '2025-11-30'
         GROUP BY CAST(order_ts AS DATE) ORDER BY day"""),
]

CASES = [("combinations", COMBINATIONS)]
