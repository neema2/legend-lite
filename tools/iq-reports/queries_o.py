"""Realistic report queries over the retail schema, part O: depth rather than breadth, as senior analysts write them --
stacked CTEs that join aggregates of aggregates, windows over grouped results over windows, subqueries three levels
deep, long CASE, COALESCE and NULLIF expressions, filters that mix IN, NOT EXISTS, BETWEEN and OR, ranks that move
month to month, Pareto and ABC classes, a status-by-channel cross-tab with its totals, business days between two
dates, repeat purchases after a support ticket, and products one region buys that another never does. Quotients a
report shows are cast to a fixed NUMERIC, as such reports do."""

DEPTH = [
    ("customers_above_their_regions_average",
     """WITH totals AS (SELECT o.customer_id, SUM(oi.quantity * oi.unit_price) AS spent
                       FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE o.status <> 'cancelled'
                       GROUP BY o.customer_id)
         SELECT c.id, c.last_name, r.name AS region, t.spent,
           CAST((SELECT AVG(t2.spent) FROM totals t2 JOIN customers c2 ON c2.id = t2.customer_id
                 WHERE c2.region_id = c.region_id) AS NUMERIC(12,2)) AS region_average
         FROM customers c JOIN regions r ON r.id = c.region_id JOIN totals t ON t.customer_id = c.id
         WHERE t.spent > (SELECT AVG(t3.spent) FROM totals t3 JOIN customers c3 ON c3.id = t3.customer_id
                          WHERE c3.region_id = c.region_id)
         ORDER BY r.name, t.spent DESC, c.id"""),
    ("monthly_rank_movement",
     """WITH monthly AS (SELECT p.category, YEAR(o.order_ts) * 100 + MONTH(o.order_ts) AS ym, SUM(oi.quantity * oi.unit_price) AS revenue
                        FROM orders o JOIN order_items oi ON oi.order_id = o.id JOIN products p ON p.id = oi.product_id
                        WHERE o.status <> 'cancelled' AND YEAR(o.order_ts) = 2025
                        GROUP BY p.category, YEAR(o.order_ts) * 100 + MONTH(o.order_ts)),
             ranked AS (SELECT category, ym, revenue, RANK() OVER (PARTITION BY ym ORDER BY revenue DESC, category) AS rnk FROM monthly)
         SELECT category, ym, revenue, rnk, LAG(rnk) OVER (PARTITION BY category ORDER BY ym) AS prior_rank,
           CASE WHEN LAG(rnk) OVER (PARTITION BY category ORDER BY ym) IS NULL THEN 'new'
                WHEN rnk < LAG(rnk) OVER (PARTITION BY category ORDER BY ym) THEN 'up'
                WHEN rnk > LAG(rnk) OVER (PARTITION BY category ORDER BY ym) THEN 'down' ELSE 'same' END AS movement
         FROM ranked ORDER BY category, ym"""),
    ("pareto_abc_classes",
     """WITH rev AS (SELECT p.id, p.name, SUM(oi.quantity * oi.unit_price) AS revenue
                    FROM products p JOIN order_items oi ON oi.product_id = p.id JOIN orders o ON o.id = oi.order_id
                    WHERE o.status <> 'cancelled' GROUP BY p.id, p.name),
             cum AS (SELECT id, name, revenue,
                            SUM(revenue) OVER (ORDER BY revenue DESC, id ROWS UNBOUNDED PRECEDING) AS running,
                            SUM(revenue) OVER () AS grand
                     FROM rev)
         SELECT id, name, revenue, CAST(100 * running / grand AS NUMERIC(6,2)) AS cumulative_pct,
           CASE WHEN 100 * (running - revenue) / grand < 80 THEN 'A' WHEN 100 * (running - revenue) / grand < 95 THEN 'B' ELSE 'C' END AS abc_class
         FROM cum ORDER BY revenue DESC, id"""),
    ("order_priority_score",
     """SELECT o.id, o.customer_id, o.channel, o.status,
           CAST(ROUND(COALESCE(t.total, 0) / 100.0 * CASE c.segment WHEN 'enterprise' THEN 1.5 WHEN 'smb' THEN 1.2 ELSE 1.0 END
                 + CASE WHEN o.ship_date IS NULL AND o.status = 'pending' THEN 25 ELSE 0 END
                 + COALESCE(k.open_tickets, 0) * 10
                 - COALESCE(DATEDIFF(DAY, o.order_ts, o.ship_date), 0), 1) AS NUMERIC(10,1)) AS score,
           COALESCE(NULLIF(TRIM(c.phone), ''), c.email, 'no contact') AS contact
         FROM orders o JOIN customers c ON c.id = o.customer_id
           LEFT JOIN (SELECT order_id, SUM(quantity * unit_price) AS total FROM order_items GROUP BY order_id) t ON t.order_id = o.id
           LEFT JOIN (SELECT customer_id, COUNT(*) AS open_tickets FROM support_tickets WHERE closed_ts IS NULL GROUP BY customer_id) k
             ON k.customer_id = o.customer_id
         WHERE o.status IN ('pending', 'shipped') AND o.order_ts >= '2025-07-01'
         ORDER BY score DESC, o.id"""),
    ("orders_matching_a_complex_filter",
     """SELECT o.id, o.customer_id, o.order_ts, o.status, o.channel
         FROM orders o
         WHERE (o.status IN ('shipped', 'returned') OR (o.status = 'pending' AND o.order_ts < '2025-06-01'))
           AND o.channel <> 'phone'
           AND o.order_ts BETWEEN '2024-01-01' AND '2025-12-31'
           AND NOT EXISTS (SELECT 1 FROM support_tickets t WHERE t.order_id = o.id AND t.priority IN ('urgent', 'high'))
           AND o.customer_id IN (SELECT c.id FROM customers c WHERE c.segment <> 'retail' OR c.credit_limit >= 50000)
         ORDER BY o.order_ts, o.id"""),
    ("status_by_channel_crosstab_with_totals",
     """SELECT status, web, store, phone, web + store + phone AS total
         FROM (SELECT status, 1 AS part,
                      SUM(CASE WHEN channel = 'web' THEN 1 ELSE 0 END) AS web,
                      SUM(CASE WHEN channel = 'store' THEN 1 ELSE 0 END) AS store,
                      SUM(CASE WHEN channel = 'phone' THEN 1 ELSE 0 END) AS phone
               FROM orders GROUP BY status
               UNION ALL
               SELECT 'all', 2, SUM(CASE WHEN channel = 'web' THEN 1 ELSE 0 END), SUM(CASE WHEN channel = 'store' THEN 1 ELSE 0 END),
                      SUM(CASE WHEN channel = 'phone' THEN 1 ELSE 0 END)
               FROM orders) x
         ORDER BY part, status"""),
    ("business_days_to_ship_estimate",
     """SELECT o.id, CAST(o.order_ts AS DATE) AS ordered, o.ship_date,
           DATEDIFF(DAY, o.order_ts, o.ship_date) AS calendar_days,
           DATEDIFF(DAY, o.order_ts, o.ship_date) - 2 * (DATEDIFF(DAY, o.order_ts, o.ship_date) / 7)
             - CASE WHEN MOD(DATEDIFF(DAY, o.order_ts, o.ship_date), 7) + DOW(o.order_ts) > 7 THEN 2
                    WHEN MOD(DATEDIFF(DAY, o.order_ts, o.ship_date), 7) + DOW(o.order_ts) = 7 THEN 1 ELSE 0 END AS business_days_estimate,
           DATENAME(WEEKDAY, o.order_ts) AS ordered_on, DATENAME(WEEKDAY, o.ship_date) AS shipped_on
         FROM orders o WHERE o.ship_date IS NOT NULL AND o.order_ts >= '2025-09-01'
         ORDER BY o.id"""),
    ("repeat_purchase_after_a_ticket",
     """SELECT t.id AS ticket, t.customer_id, t.opened_ts, t.priority, n.next_order,
           CASE WHEN n.next_order IS NULL THEN 'not yet' WHEN n.next_order <= DATEADD(DAY, 30, t.opened_ts) THEN 'retained 30d'
                ELSE 'came back later' END AS outcome
         FROM support_tickets t
           LEFT JOIN (SELECT t2.id, MIN(o.order_ts) AS next_order FROM support_tickets t2
                      JOIN orders o ON o.customer_id = t2.customer_id AND o.order_ts > t2.opened_ts GROUP BY t2.id) n ON n.id = t.id
         ORDER BY t.customer_id, t.opened_ts, t.id"""),
]

COMBINATIONS = [
    ("products_one_region_buys_and_another_never",
     """SELECT p.id, p.name, p.category,
           (SELECT COUNT(DISTINCT o.id) FROM orders o JOIN order_items oi ON oi.order_id = o.id JOIN customers c ON c.id = o.customer_id
            WHERE oi.product_id = p.id AND c.region_id = 1) AS north_orders
         FROM products p
         WHERE EXISTS (SELECT 1 FROM order_items oi JOIN orders o ON o.id = oi.order_id JOIN customers c ON c.id = o.customer_id
                       WHERE oi.product_id = p.id AND c.region_id = 1)
           AND NOT EXISTS (SELECT 1 FROM order_items oi JOIN orders o ON o.id = oi.order_id JOIN customers c ON c.id = o.customer_id
                           WHERE oi.product_id = p.id AND c.region_id = 5)
         ORDER BY p.id"""),
    ("segment_kpis_with_distinct_cases",
     """SELECT c.segment, COUNT(DISTINCT c.id) AS customers,
           COUNT(DISTINCT CASE WHEN o.status = 'shipped' THEN o.customer_id END) AS buyers,
           COUNT(DISTINCT CASE WHEN o.status = 'returned' THEN o.customer_id END) AS returners,
           COUNT(DISTINCT CASE WHEN o.channel = 'web' THEN o.id END) AS web_orders,
           CAST(100.0 * COUNT(DISTINCT CASE WHEN o.status = 'shipped' THEN o.customer_id END) / COUNT(DISTINCT c.id) AS NUMERIC(5,1)) AS buyer_rate,
           SUM(CASE WHEN o.status = 'returned' THEN 1 ELSE 0 END) AS returned_orders
         FROM customers c LEFT JOIN orders o ON o.customer_id = c.id
         GROUP BY c.segment ORDER BY c.segment"""),
    ("year_over_year_with_arrows",
     """WITH yearly AS (SELECT p.category, YEAR(o.order_ts) AS yr, SUM(oi.quantity * oi.unit_price) AS revenue, COUNT(DISTINCT o.id) AS orders
                       FROM orders o JOIN order_items oi ON oi.order_id = o.id JOIN products p ON p.id = oi.product_id
                       WHERE o.status <> 'cancelled' GROUP BY p.category, YEAR(o.order_ts))
         SELECT category, yr, revenue, orders, LAG(revenue) OVER (PARTITION BY category ORDER BY yr) AS prior,
           CAST(100 * (revenue - LAG(revenue) OVER (PARTITION BY category ORDER BY yr))
                / NULLIF(LAG(revenue) OVER (PARTITION BY category ORDER BY yr), 0) AS NUMERIC(8,1)) AS growth_pct,
           CASE WHEN LAG(revenue) OVER (PARTITION BY category ORDER BY yr) IS NULL THEN '-'
                WHEN revenue > LAG(revenue) OVER (PARTITION BY category ORDER BY yr) THEN '^'
                WHEN revenue < LAG(revenue) OVER (PARTITION BY category ORDER BY yr) THEN 'v' ELSE '=' END AS arrow,
           CAST(revenue / orders AS NUMERIC(10,2)) AS per_order
         FROM yearly ORDER BY category, yr"""),
    ("share_thresholds_by_region",
     """WITH spend AS (SELECT c.region_id, c.id AS customer_id, SUM(oi.quantity * oi.unit_price) AS spent
                     FROM customers c JOIN orders o ON o.customer_id = c.id JOIN order_items oi ON oi.order_id = o.id
                     WHERE o.status <> 'cancelled' GROUP BY c.region_id, c.id)
         SELECT r.name AS region, s.customer_id, s.spent,
           CAST(100 * s.spent / SUM(s.spent) OVER (PARTITION BY s.region_id) AS NUMERIC(6,2)) AS share_pct,
           CASE WHEN 100 * s.spent / SUM(s.spent) OVER (PARTITION BY s.region_id) >= 40 THEN 'dominant'
                WHEN 100 * s.spent / SUM(s.spent) OVER (PARTITION BY s.region_id) >= 20 THEN 'major' ELSE 'minor' END AS weight,
           DENSE_RANK() OVER (PARTITION BY s.region_id ORDER BY s.spent DESC) AS rank_in_region,
           NTILE(3) OVER (ORDER BY s.spent DESC, s.customer_id) AS overall_tercile
         FROM spend s JOIN regions r ON r.id = s.region_id ORDER BY r.name, rank_in_region, s.customer_id"""),
    ("median_versus_mean_by_category",
     """SELECT p.category, COUNT(*) AS lines,
           CAST(AVG(oi.quantity * oi.unit_price) AS NUMERIC(10,2)) AS mean_line,
           MEDIAN(oi.quantity * oi.unit_price) AS median_line,
           CASE WHEN AVG(oi.quantity * oi.unit_price) > 1.5 * MEDIAN(oi.quantity * oi.unit_price) THEN 'skewed by large lines'
                ELSE 'balanced' END AS shape,
           MAX(oi.quantity * oi.unit_price) AS largest_line, MIN(oi.quantity * oi.unit_price) AS smallest_line
         FROM order_items oi JOIN products p ON p.id = oi.product_id
         GROUP BY p.category ORDER BY p.category"""),
    ("ticket_bursts_within_a_week",
     """WITH t AS (SELECT id, customer_id, opened_ts,
                         CASE WHEN DATEDIFF(DAY, LAG(opened_ts) OVER (PARTITION BY customer_id ORDER BY opened_ts, id), opened_ts) <= 7
                              THEN 0 ELSE 1 END AS starts_burst
                  FROM support_tickets),
             b AS (SELECT id, customer_id, opened_ts,
                          SUM(starts_burst) OVER (PARTITION BY customer_id ORDER BY opened_ts, id ROWS UNBOUNDED PRECEDING) AS burst
                   FROM t)
         SELECT customer_id, burst, COUNT(*) AS tickets, MIN(opened_ts) AS first_ticket, MAX(opened_ts) AS last_ticket
         FROM b GROUP BY customer_id, burst HAVING COUNT(*) >= 2 ORDER BY customer_id, burst"""),
    ("supplier_cheapest_and_speed",
     """SELECT s.id, s.name, s.country, COUNT(sp.id) AS products_offered,
           SUM(CASE WHEN sp.unit_cost = b.best THEN 1 ELSE 0 END) AS cheapest_on,
           CAST(AVG(sp.lead_days) AS NUMERIC(5,1)) AS avg_lead_days,
           MAX(sp.lead_days) AS slowest,
           CASE WHEN AVG(sp.lead_days) <= 10 THEN 'fast' WHEN AVG(sp.lead_days) <= 20 THEN 'standard' ELSE 'slow' END AS speed
         FROM suppliers s LEFT JOIN supplier_products sp ON sp.supplier_id = s.id
           LEFT JOIN (SELECT product_id, MIN(unit_cost) AS best FROM supplier_products GROUP BY product_id) b ON b.product_id = sp.product_id
         GROUP BY s.id, s.name, s.country ORDER BY cheapest_on DESC, s.id"""),
    ("inventory_cover_in_days",
     """WITH latest AS (SELECT product_id, on_hand, reorder_point, snapshot_date,
                              ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY snapshot_date DESC, id DESC) AS rn
                       FROM inventory),
             demand AS (SELECT oi.product_id, SUM(oi.quantity) AS units_90d
                        FROM order_items oi JOIN orders o ON o.id = oi.order_id
                        WHERE o.status <> 'cancelled' AND o.order_ts >= DATEADD(DAY, -90, CAST('2026-01-01' AS TIMESTAMP))
                        GROUP BY oi.product_id)
         SELECT p.id, p.name, l.on_hand, COALESCE(d.units_90d, 0) AS units_90d,
           CASE WHEN COALESCE(d.units_90d, 0) = 0 THEN NULL ELSE CAST(l.on_hand * 90.0 / d.units_90d AS NUMERIC(8,1)) END AS days_of_cover,
           CASE WHEN l.on_hand <= l.reorder_point THEN 'order now'
                WHEN COALESCE(d.units_90d, 0) > 0 AND l.on_hand * 90 / d.units_90d < 30 THEN 'order soon' ELSE 'covered' END AS action
         FROM products p JOIN latest l ON l.product_id = p.id AND l.rn = 1 LEFT JOIN demand d ON d.product_id = p.id
         ORDER BY p.id"""),
    ("customer_lifetime_summary",
     """SELECT c.id, TRIM(c.first_name) || ' ' || TRIM(c.last_name) AS customer, c.segment,
           DATEDIFF(MONTH, c.signup_date, CAST('2026-01-01' AS DATE)) AS months_since_signup,
           COALESCE(x.orders, 0) AS orders, COALESCE(x.spent, 0) AS spent,
           CAST(COALESCE(x.spent, 0) / NULLIF(DATEDIFF(MONTH, c.signup_date, CAST('2026-01-01' AS DATE)), 0) AS NUMERIC(10,2)) AS spend_per_month,
           COALESCE(y.paid, 0) AS paid, COALESCE(x.spent, 0) - COALESCE(y.paid, 0) AS balance,
           CASE WHEN x.orders IS NULL THEN 'prospect' WHEN x.orders = 1 THEN 'one-time' WHEN x.orders <= 4 THEN 'repeat' ELSE 'loyal' END AS tier
         FROM customers c
           LEFT JOIN (SELECT o.customer_id, COUNT(DISTINCT o.id) AS orders, SUM(oi.quantity * oi.unit_price) AS spent
                      FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE o.status <> 'cancelled' GROUP BY o.customer_id) x
             ON x.customer_id = c.id
           LEFT JOIN (SELECT o.customer_id, SUM(p.amount) AS paid FROM payments p JOIN orders o ON o.id = p.order_id GROUP BY o.customer_id) y
             ON y.customer_id = c.id
         ORDER BY c.id"""),
    ("discount_band_effectiveness",
     """SELECT CASE WHEN o.discount_pct = 0 THEN 'none' WHEN o.discount_pct <= 0.10 THEN 'up to 10%'
                     WHEN o.discount_pct <= 0.20 THEN '11-20%' ELSE 'over 20%' END AS discount_band,
           COUNT(DISTINCT o.id) AS orders, CAST(AVG(t.items) AS NUMERIC(6,2)) AS avg_items,
           CAST(AVG(t.gross) AS NUMERIC(10,2)) AS avg_gross,
           CAST(AVG(t.gross * (1 - o.discount_pct)) AS NUMERIC(10,2)) AS avg_net,
           SUM(CASE WHEN o.status = 'returned' THEN 1 ELSE 0 END) AS returns
         FROM orders o JOIN (SELECT order_id, SUM(quantity) AS items, SUM(quantity * unit_price) AS gross FROM order_items GROUP BY order_id) t
           ON t.order_id = o.id
         WHERE o.status <> 'cancelled'
         GROUP BY CASE WHEN o.discount_pct = 0 THEN 'none' WHEN o.discount_pct <= 0.10 THEN 'up to 10%'
                       WHEN o.discount_pct <= 0.20 THEN '11-20%' ELSE 'over 20%' END
         ORDER BY MIN(o.discount_pct)"""),
    ("web_funnel_by_device",
     """SELECT w.device,
           COUNT(DISTINCT w.customer_id) AS visitors,
           COUNT(DISTINCT CASE WHEN w.url LIKE '%/product%' THEN w.customer_id END) AS viewed_product,
           COUNT(DISTINCT CASE WHEN w.url LIKE '%/cart%' THEN w.customer_id END) AS added_to_cart,
           COUNT(DISTINCT CASE WHEN w.url LIKE '%/checkout%' THEN w.customer_id END) AS reached_checkout,
           CAST(100.0 * COUNT(DISTINCT CASE WHEN w.url LIKE '%/checkout%' THEN w.customer_id END)
                / NULLIF(COUNT(DISTINCT w.customer_id), 0) AS NUMERIC(5,1)) AS checkout_rate
         FROM web_events w WHERE w.customer_id IS NOT NULL
         GROUP BY w.device ORDER BY w.device"""),
]

CASES = [("depth", DEPTH), ("combinations", COMBINATIONS)]
