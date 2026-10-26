"""Realistic report queries over the retail schema, part U: HR and operations reports, as people analysts and
planners write them -- salary position by PERCENT_RANK and CUME_DIST against the title's average, employees paid more
than their manager, direct and indirect reports by correlated subqueries, month starts and ends by date arithmetic
with orders per day, orders above every earlier order of the customer by a framed MAX, products priced above ALL or
ANY of another category, compound monthly growth by POWER, the median and 90th percentile of payment lag, stock-out
streaks as islands, first and last stock levels by framed FIRST_VALUE and LAST_VALUE, and the gaps between a
customer's orders."""

PEOPLE = [
    ("salary_position_within_title",
     """SELECT e.id, TRIM(e.first_name) || ' ' || TRIM(e.last_name) AS employee, e.title, e.salary,
           CAST(AVG(e.salary) OVER (PARTITION BY e.title) AS NUMERIC(10,2)) AS title_average,
           e.salary - AVG(e.salary) OVER (PARTITION BY e.title) AS above_title_average,
           CAST(PERCENT_RANK() OVER (ORDER BY e.salary) AS NUMERIC(5,3)) AS salary_percent_rank,
           CAST(CUME_DIST() OVER (ORDER BY e.salary) AS NUMERIC(5,3)) AS salary_cume_dist,
           DATEDIFF(MONTH, e.hire_date, CAST('2025-12-31' AS DATE)) / 12 AS years_of_service
         FROM employees e ORDER BY e.salary DESC, e.id"""),
    ("paid_more_than_their_manager",
     """SELECT e.id, TRIM(e.last_name) AS employee, e.salary, m.id AS boss_id, TRIM(m.last_name) AS manager,
           m.salary AS manager_salary, e.salary - m.salary AS difference,
           CAST(100.0 * (e.salary - m.salary) / m.salary AS NUMERIC(6,2)) AS difference_pct
         FROM employees e JOIN employees m ON m.id = e.manager_id
         WHERE e.salary > 0.6 * m.salary
         ORDER BY difference DESC, e.id"""),
    ("direct_and_indirect_reports",
     """SELECT m.id, TRIM(m.first_name) || ' ' || TRIM(m.last_name) AS manager, m.title,
           (SELECT COUNT(*) FROM employees d WHERE d.manager_id = m.id) AS direct_reports,
           (SELECT COUNT(*) FROM employees d JOIN employees i ON i.manager_id = d.id WHERE d.manager_id = m.id)
             AS second_level_reports,
           (SELECT COALESCE(SUM(d.salary), 0) FROM employees d WHERE d.manager_id = m.id) AS direct_payroll
         FROM employees m
         WHERE EXISTS (SELECT 1 FROM employees d WHERE d.manager_id = m.id)
         ORDER BY direct_reports DESC, m.id"""),
]

OPERATIONS = [
    ("orders_per_day_by_month",
     """WITH months AS (
           SELECT DATEADD(DAY, 1 - DAY(o.order_ts), CAST(o.order_ts AS DATE)) AS month_start, o.id
           FROM orders o WHERE o.status <> 'cancelled' AND YEAR(o.order_ts) = 2025)
         SELECT CAST(month_start AS DATE) AS first_day,
           CAST(DATEADD(DAY, -1, DATEADD(MONTH, 1, month_start)) AS DATE) AS last_day,
           DATEDIFF(DAY, month_start, DATEADD(MONTH, 1, month_start)) AS days_in_month,
           COUNT(*) AS orders,
           CAST(COUNT(*) * 1.0 / DATEDIFF(DAY, month_start, DATEADD(MONTH, 1, month_start)) AS NUMERIC(6,3)) AS orders_per_day
         FROM months GROUP BY month_start ORDER BY first_day"""),
    ("orders_above_every_earlier_order",
     """WITH totals AS (
           SELECT o.id, o.customer_id, o.order_ts, SUM(oi.quantity * oi.unit_price) AS total
           FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE o.status <> 'cancelled'
           GROUP BY o.id, o.customer_id, o.order_ts),
         framed AS (
           SELECT id, customer_id, order_ts, total,
                  MAX(total) OVER (PARTITION BY customer_id ORDER BY order_ts, id
                                   ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS previous_best,
                  COUNT(*) OVER (PARTITION BY customer_id ORDER BY order_ts, id
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS earlier_orders
           FROM totals)
         SELECT customer_id, id, CAST(order_ts AS DATE) AS ordered, total, previous_best, earlier_orders
         FROM framed WHERE earlier_orders > 0 AND total > previous_best
         ORDER BY customer_id, order_ts, id"""),
    ("products_priced_against_another_category",
     """SELECT p.id, p.name, p.category, p.unit_price,
           CASE WHEN p.unit_price > ALL (SELECT q.unit_price FROM products q WHERE q.category = 'Outdoor') THEN 'above all outdoor'
                WHEN p.unit_price > ANY (SELECT q.unit_price FROM products q WHERE q.category = 'Outdoor') THEN 'above some outdoor'
                ELSE 'at or below every outdoor' END AS against_outdoor
         FROM products p WHERE p.category <> 'Outdoor'
         ORDER BY p.unit_price DESC, p.id"""),
    ("compound_monthly_growth_by_category",
     """WITH monthly AS (
           SELECT p.category, MONTH(o.order_ts) AS month_no, SUM(oi.quantity * oi.unit_price) AS revenue
           FROM orders o JOIN order_items oi ON oi.order_id = o.id JOIN products p ON p.id = oi.product_id
           WHERE o.status <> 'cancelled' AND YEAR(o.order_ts) = 2024
           GROUP BY p.category, MONTH(o.order_ts)),
         ends AS (
           SELECT category, MIN(month_no) AS first_month, MAX(month_no) AS last_month FROM monthly GROUP BY category)
         SELECT e.category, e.first_month, e.last_month, f.revenue AS first_revenue, l.revenue AS last_revenue,
           CAST(100 * (POWER(CAST(l.revenue AS DOUBLE) / CAST(f.revenue AS DOUBLE), 1.0 / (e.last_month - e.first_month)) - 1)
                AS NUMERIC(8,2)) AS compound_growth_pct
         FROM ends e JOIN monthly f ON f.category = e.category AND f.month_no = e.first_month
           JOIN monthly l ON l.category = e.category AND l.month_no = e.last_month
         WHERE e.last_month > e.first_month
         ORDER BY e.category"""),
    ("payment_lag_percentiles_by_method",
     """SELECT p.method, COUNT(*) AS payments,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DATEDIFF(HOUR, o.order_ts, p.paid_ts)) AS median_hours,
           PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY DATEDIFF(HOUR, o.order_ts, p.paid_ts)) AS p90_hours,
           MAX(DATEDIFF(DAY, o.order_ts, p.paid_ts)) AS longest_days,
           SUM(CASE WHEN DATEDIFF(HOUR, o.order_ts, p.paid_ts) > 48 THEN 1 ELSE 0 END) AS after_two_days
         FROM payments p JOIN orders o ON o.id = p.order_id
         GROUP BY p.method ORDER BY p.method"""),
    ("stock_out_streaks",
     """WITH flagged AS (
           SELECT i.product_id, i.snapshot_date, i.on_hand, i.reorder_point,
                  ROW_NUMBER() OVER (PARTITION BY i.product_id ORDER BY i.snapshot_date, i.id)
                  - ROW_NUMBER() OVER (PARTITION BY i.product_id, CASE WHEN i.on_hand < i.reorder_point THEN 1 ELSE 0 END
                                       ORDER BY i.snapshot_date, i.id) AS streak_key
           FROM inventory i),
         streaks AS (
           SELECT product_id, streak_key, MIN(snapshot_date) AS from_month, MAX(snapshot_date) AS to_month,
                  COUNT(*) AS months, MIN(on_hand) AS lowest
           FROM flagged WHERE on_hand < reorder_point GROUP BY product_id, streak_key)
         SELECT s.product_id, p.name, s.from_month, s.to_month, s.months, s.lowest
         FROM streaks s JOIN products p ON p.id = s.product_id
         ORDER BY s.months DESC, s.product_id, s.from_month"""),
    ("stock_first_and_last_levels",
     """SELECT DISTINCT i.product_id,
           FIRST_VALUE(i.on_hand) OVER (PARTITION BY i.product_id ORDER BY i.snapshot_date, i.id
                                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS january_on_hand,
           LAST_VALUE(i.on_hand) OVER (PARTITION BY i.product_id ORDER BY i.snapshot_date, i.id
                                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS december_on_hand,
           MIN(i.on_hand) OVER (PARTITION BY i.product_id) AS lowest_on_hand,
           SUM(CASE WHEN i.on_hand < i.reorder_point THEN 1 ELSE 0 END) OVER (PARTITION BY i.product_id) AS months_below_reorder
         FROM inventory i ORDER BY i.product_id"""),
    ("gaps_between_customer_orders",
     """WITH spaced AS (
           SELECT o.customer_id, o.id,
                  DATEDIFF(DAY, LAG(o.order_ts) OVER (PARTITION BY o.customer_id ORDER BY o.order_ts, o.id), o.order_ts)
                    AS days_since_previous
           FROM orders o WHERE o.status <> 'cancelled')
         SELECT customer_id, COUNT(*) AS orders, COUNT(days_since_previous) AS gaps,
           CAST(AVG(days_since_previous * 1.0) AS NUMERIC(8,1)) AS average_gap_days,
           MAX(days_since_previous) AS longest_gap_days,
           CASE WHEN MAX(days_since_previous) > 365 THEN 'lapsed once' WHEN MAX(days_since_previous) > 180 THEN 'slow'
                ELSE 'regular' END AS cadence
         FROM spaced GROUP BY customer_id HAVING COUNT(*) >= 3
         ORDER BY longest_gap_days DESC, customer_id"""),
]

CASES = [("people_reports", PEOPLE), ("operations_reports", OPERATIONS)]
