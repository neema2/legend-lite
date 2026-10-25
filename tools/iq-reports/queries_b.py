"""Realistic report queries over the retail schema, part B: operations, payments, people, and the long reports."""

LINE = "oi.quantity * oi.unit_price - oi.line_discount"

OPERATIONS = [
    ("shipping_lead_times",
     """SELECT channel, COUNT(*) AS shipped, MIN(DATEDIFF(DAY, order_ts, ship_date)) AS fastest,
         MAX(DATEDIFF(DAY, order_ts, ship_date)) AS slowest,
         ROUND(AVG(CAST(DATEDIFF(DAY, order_ts, ship_date) AS NUMERIC(6,2))), 2) AS avg_days,
         SUM(CASE WHEN DATEDIFF(DAY, order_ts, ship_date) > 5 THEN 1 ELSE 0 END) AS late
         FROM orders WHERE ship_date IS NOT NULL GROUP BY channel ORDER BY channel"""),
    ("pending_orders_aging",
     """SELECT o.id, o.order_ts, c.last_name, DATEDIFF(DAY, o.order_ts, '2026-01-01') AS age_days,
         CASE WHEN DATEDIFF(DAY, o.order_ts, '2026-01-01') > 365 THEN 'stale' WHEN DATEDIFF(DAY, o.order_ts, '2026-01-01') > 90 THEN 'aging' ELSE 'fresh' END AS bucket
         FROM orders o JOIN customers c ON c.id = o.customer_id WHERE o.status = 'pending' ORDER BY o.order_ts, o.id"""),
    ("order_numbers_and_week",
     """SELECT id, 'ORD-' || RIGHT('00000' || CAST(id AS VARCHAR(10)), 5) AS order_no,
         DATEFORMAT(order_ts, 'YYYY-MM-DD') AS order_day, DATEPART(WEEK, order_ts) AS iso_week,
         CONVERT(VARCHAR(8), ship_date, 112) AS ship_yyyymmdd
         FROM orders WHERE id <= 25 ORDER BY id"""),
    ("first_of_month_and_month_end",
     """SELECT id, CAST(order_ts AS DATE) AS order_day,
         DATEADD(DAY, 1 - DAY(order_ts), CAST(order_ts AS DATE)) AS month_start,
         DATEADD(DAY, -1, DATEADD(MONTH, 1, DATEADD(DAY, 1 - DAY(order_ts), CAST(order_ts AS DATE)))) AS month_end
         FROM orders WHERE id BETWEEN 10 AND 30 ORDER BY id"""),
    ("orders_in_the_last_90_days_of_2025",
     """SELECT o.id, o.order_ts, o.status, c.city FROM orders o JOIN customers c ON c.id = o.customer_id
         WHERE o.order_ts >= DATEADD(DAY, -90, '2025-12-31') AND o.order_ts < '2026-01-01'
           AND (o.status = 'shipped' OR (o.status = 'pending' AND o.discount_pct > 0))
         ORDER BY o.order_ts, o.id"""),
    ("order_totals_with_fees_and_tax",
     f"""SELECT o.id, SUM({LINE}) AS subtotal, o.discount_pct, o.shipping_fee,
         ROUND(SUM({LINE}) * (1 - o.discount_pct) + o.shipping_fee, 2) AS total,
         ROUND((SUM({LINE}) * (1 - o.discount_pct) + o.shipping_fee) * 0.0825, 2) AS sales_tax
         FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE o.status <> 'cancelled'
         GROUP BY o.id, o.discount_pct, o.shipping_fee HAVING SUM({LINE}) > 1000 ORDER BY total DESC, o.id"""),
    ("big_orders_with_exists_and_in",
     """SELECT o.id, o.customer_id, o.channel FROM orders o
         WHERE EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id AND oi.quantity >= 5)
           AND o.customer_id IN (SELECT c.id FROM customers c WHERE c.segment IN ('smb', 'enterprise'))
           AND o.status NOT IN ('cancelled')
         ORDER BY o.id"""),
    ("returned_orders_detail",
     """SELECT o.id, o.order_ts, p.name, oi.quantity, oi.unit_price, c.email
         FROM orders o JOIN order_items oi ON oi.order_id = o.id JOIN products p ON p.id = oi.product_id
         JOIN customers c ON c.id = o.customer_id WHERE o.status = 'returned' ORDER BY o.id, oi.id"""),
    ("line_count_distribution",
     """SELECT lines, COUNT(*) AS orders FROM (SELECT o.id, COUNT(oi.id) AS lines FROM orders o
           LEFT JOIN order_items oi ON oi.order_id = o.id GROUP BY o.id) per GROUP BY lines ORDER BY lines"""),
]

PAYMENTS = [
    ("unpaid_and_partially_paid_orders",
     f"""WITH due AS (SELECT o.id, ROUND(SUM({LINE}) * (1 - o.discount_pct), 2) AS amount_due FROM orders o
           JOIN order_items oi ON oi.order_id = o.id WHERE o.status <> 'cancelled' GROUP BY o.id, o.discount_pct),
         paid AS (SELECT order_id, SUM(amount) AS amount_paid, COUNT(*) AS installments FROM payments GROUP BY order_id)
         SELECT d.id, d.amount_due, COALESCE(p.amount_paid, 0) AS amount_paid, COALESCE(p.installments, 0) AS installments,
         d.amount_due - COALESCE(p.amount_paid, 0) AS balance
         FROM due d LEFT JOIN paid p ON p.order_id = d.id WHERE COALESCE(p.amount_paid, 0) < d.amount_due - 0.01
         ORDER BY balance DESC, d.id"""),
    ("payment_methods_by_month",
     """SELECT DATEFORMAT(paid_ts, 'YYYY-MM') AS ym, method, COUNT(*) AS payments, SUM(amount) AS collected
         FROM payments WHERE paid_ts >= '2025-01-01' GROUP BY DATEFORMAT(paid_ts, 'YYYY-MM'), method ORDER BY ym, method"""),
    ("days_to_payment",
     """SELECT o.channel, COUNT(*) AS paid_orders,
         ROUND(AVG(CAST(DATEDIFF(HOUR, o.order_ts, p.first_paid) AS DOUBLE)) / 24, 2) AS avg_days_to_pay
         FROM orders o JOIN (SELECT order_id, MIN(paid_ts) AS first_paid FROM payments GROUP BY order_id) p ON p.order_id = o.id
         GROUP BY o.channel ORDER BY o.channel"""),
    ("installment_numbers",
     """SELECT order_id, id AS payment_id, paid_ts, amount,
         ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_ts, id) AS installment,
         SUM(amount) OVER (PARTITION BY order_id ORDER BY paid_ts, id ROWS UNBOUNDED PRECEDING) AS paid_so_far
         FROM payments WHERE order_id IN (SELECT order_id FROM payments GROUP BY order_id HAVING COUNT(*) > 1)
         ORDER BY order_id, installment"""),
    ("collections_by_method_share",
     """SELECT method, SUM(amount) AS collected, ROUND(100.0 * SUM(amount) / (SELECT SUM(amount) FROM payments), 2) AS share_pct
         FROM payments GROUP BY method ORDER BY collected DESC, method"""),
]

PEOPLE = [
    ("employee_manager_names",
     """SELECT e.id, e.first_name || ' ' || e.last_name AS employee, e.title,
         COALESCE(m.first_name || ' ' || m.last_name, '(none)') AS manager
         FROM employees e LEFT JOIN employees m ON m.id = e.manager_id ORDER BY e.id"""),
    ("tenure_and_salary_rank",
     """SELECT id, last_name, title, hire_date, DATEDIFF(YEAR, hire_date, '2026-01-01') AS years,
         salary, RANK() OVER (PARTITION BY title ORDER BY salary DESC) AS rank_in_title,
         ROUND(100.0 * salary / SUM(salary) OVER (), 2) AS payroll_share
         FROM employees ORDER BY title, rank_in_title, id"""),
    ("rep_performance",
     f"""SELECT e.id, e.last_name, COUNT(DISTINCT o.id) AS orders, COALESCE(SUM({LINE}), 0) AS revenue,
         CASE WHEN COUNT(DISTINCT o.id) = 0 THEN NULL ELSE ROUND(SUM({LINE}) / COUNT(DISTINCT o.id), 2) END AS per_order
         FROM employees e LEFT JOIN orders o ON o.sales_rep_id = e.id AND o.status IN ('shipped', 'returned')
         LEFT JOIN order_items oi ON oi.order_id = o.id GROUP BY e.id, e.last_name ORDER BY revenue DESC, e.id"""),
    ("team_size_per_manager",
     """SELECT m.id, m.last_name, COUNT(e.id) AS direct_reports, SUM(e.salary) AS team_payroll
         FROM employees m JOIN employees e ON e.manager_id = m.id GROUP BY m.id, m.last_name ORDER BY m.id"""),
    ("salary_vs_title_average",
     """SELECT e.id, e.title, e.salary, ROUND(AVG(e.salary) OVER (PARTITION BY e.title), 2) AS title_avg,
         e.salary - AVG(e.salary) OVER (PARTITION BY e.title) AS diff_from_avg
         FROM employees e ORDER BY e.title, e.id"""),
    ("reps_region_coverage",
     """SELECT r.name AS region, COUNT(DISTINCT e.id) AS reps, LIST(e.last_name, ', ' ORDER BY e.last_name) AS names
         FROM regions r LEFT JOIN employees e ON e.region_id = r.id GROUP BY r.name ORDER BY r.name"""),
]

LONG = [
    ("regional_scorecard",
     f"""WITH order_totals AS (
           SELECT o.id, o.customer_id, o.order_ts, o.status, SUM({LINE}) AS gross, o.discount_pct
           FROM orders o JOIN order_items oi ON oi.order_id = o.id GROUP BY o.id, o.customer_id, o.order_ts, o.status, o.discount_pct),
         by_region AS (
           SELECT r.id AS region_id, r.name AS region, r.country,
             COUNT(DISTINCT ot.customer_id) AS buyers, COUNT(*) AS orders,
             SUM(CASE WHEN ot.status IN ('shipped', 'returned') THEN ot.gross * (1 - ot.discount_pct) ELSE 0 END) AS net_revenue,
             SUM(CASE WHEN ot.status = 'returned' THEN 1 ELSE 0 END) AS returns
           FROM order_totals ot JOIN customers c ON c.id = ot.customer_id JOIN regions r ON r.id = c.region_id
           GROUP BY r.id, r.name, r.country)
         SELECT region, country, buyers, orders, ROUND(net_revenue, 2) AS net_revenue,
           ROUND(net_revenue / NULLIF(orders, 0), 2) AS revenue_per_order,
           ROUND(100.0 * returns / NULLIF(orders, 0), 1) AS return_rate,
           RANK() OVER (ORDER BY net_revenue DESC) AS revenue_rank,
           RANK() OVER (PARTITION BY country ORDER BY net_revenue DESC) AS rank_in_country
         FROM by_region ORDER BY revenue_rank, region"""),
    ("customer_360",
     f"""SELECT c.id, TRIM(c.first_name) || ' ' || UPPER(c.last_name) AS name, r.name AS region, c.segment,
         DATEDIFF(DAY, c.signup_date, '2026-01-01') AS tenure_days,
         COALESCE(stats.orders, 0) AS orders, COALESCE(stats.spend, 0) AS spend,
         COALESCE(pay.paid, 0) AS paid, COALESCE(stats.spend, 0) - COALESCE(pay.paid, 0) AS gap,
         CASE WHEN stats.orders IS NULL THEN 'prospect' WHEN stats.spend >= 5000 THEN 'vip'
              WHEN stats.orders >= 3 THEN 'loyal' ELSE 'occasional' END AS tier
         FROM customers c JOIN regions r ON r.id = c.region_id
         LEFT JOIN (SELECT o.customer_id, COUNT(DISTINCT o.id) AS orders, SUM({LINE}) AS spend
                    FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE o.status <> 'cancelled' GROUP BY o.customer_id) stats
           ON stats.customer_id = c.id
         LEFT JOIN (SELECT o.customer_id, SUM(p.amount) AS paid FROM payments p JOIN orders o ON o.id = p.order_id GROUP BY o.customer_id) pay
           ON pay.customer_id = c.id
         ORDER BY c.id"""),
    ("product_performance_report",
     f"""WITH sales AS (
           SELECT oi.product_id, YEAR(o.order_ts) AS yr, SUM(oi.quantity) AS units, SUM({LINE}) AS revenue,
             SUM(oi.quantity * p.unit_cost) AS cost
           FROM order_items oi JOIN orders o ON o.id = oi.order_id JOIN products p ON p.id = oi.product_id
           WHERE o.status IN ('shipped', 'returned') GROUP BY oi.product_id, YEAR(o.order_ts))
         SELECT p.category, p.name, s.yr, s.units, s.revenue, s.revenue - s.cost AS profit,
           ROUND(100.0 * (s.revenue - s.cost) / NULLIF(s.revenue, 0), 1) AS margin_pct,
           s.revenue - LAG(s.revenue) OVER (PARTITION BY s.product_id ORDER BY s.yr) AS yoy_change
         FROM sales s JOIN products p ON p.id = s.product_id
         WHERE p.discontinued = 0 ORDER BY p.category, p.name, s.yr"""),
    ("monthly_kpis_with_targets",
     f"""WITH monthly AS (
           SELECT DATEFORMAT(o.order_ts, 'YYYY-MM') AS ym, COUNT(DISTINCT o.id) AS orders,
             COUNT(DISTINCT o.customer_id) AS active_customers, SUM({LINE}) AS revenue
           FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE o.status <> 'cancelled'
           GROUP BY DATEFORMAT(o.order_ts, 'YYYY-MM'))
         SELECT ym, orders, active_customers, revenue,
           CASE WHEN revenue >= 10000 THEN 'above target' WHEN revenue >= 5000 THEN 'near target' ELSE 'below target' END AS status,
           SUM(revenue) OVER (ORDER BY ym ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS trailing_12m,
           MAX(revenue) OVER (ORDER BY ym ROWS UNBOUNDED PRECEDING) AS best_so_far
         FROM monthly ORDER BY ym"""),
    ("basket_analysis_by_size",
     f"""SELECT CASE WHEN items <= 1 THEN 'single' WHEN items <= 3 THEN 'small' ELSE 'large' END AS basket,
           COUNT(*) AS orders, ROUND(AVG(order_value), 2) AS avg_value, SUM(order_value) AS total_value,
           ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_of_orders
         FROM (SELECT o.id, SUM(oi.quantity) AS items, SUM({LINE}) AS order_value FROM orders o
               JOIN order_items oi ON oi.order_id = o.id WHERE o.status <> 'cancelled' GROUP BY o.id) baskets
         GROUP BY CASE WHEN items <= 1 THEN 'single' WHEN items <= 3 THEN 'small' ELSE 'large' END
         ORDER BY basket"""),
    ("churn_risk_list",
     """SELECT c.id, c.first_name, c.last_name, c.email, last_o.last_order,
         DATEDIFF(DAY, last_o.last_order, '2026-01-01') AS days_quiet
         FROM customers c JOIN (SELECT customer_id, MAX(order_ts) AS last_order FROM orders
                                WHERE status <> 'cancelled' GROUP BY customer_id) last_o ON last_o.customer_id = c.id
         WHERE c.is_active = 1 AND DATEDIFF(DAY, last_o.last_order, '2026-01-01') > 180
           AND (c.email LIKE '%@gmail.com' OR c.email LIKE '%@yahoo.com' OR c.segment = 'enterprise')
         ORDER BY days_quiet DESC, c.id"""),
    ("union_of_activity_feed",
     """SELECT 'order' AS kind, o.id AS ref, o.order_ts AS at_ts, o.customer_id AS who FROM orders o WHERE o.order_ts >= '2025-11-01'
         UNION ALL SELECT 'payment', p.id, p.paid_ts, o.customer_id FROM payments p JOIN orders o ON o.id = p.order_id WHERE p.paid_ts >= '2025-11-01'
         UNION ALL SELECT 'signup', c.id, CAST(c.signup_date AS TIMESTAMP), c.id FROM customers c WHERE c.signup_date >= '2024-05-01'
         ORDER BY 3, 1, 2"""),
]

CASES = [("operations", OPERATIONS), ("payments", PAYMENTS), ("people", PEOPLE), ("long", LONG)]
