"""Realistic report queries over the retail schema, part S: finance reports, as controllers write them -- a monthly
target against actuals with the variance in percent, a seven-order moving average by AVG over a ROWS frame,
month-over-month change by LAG and NULLIF, inventory turnover (cost of goods sold over average stock), revenue
retention of first-order cohorts, the discount's correlation with quantity, average order value against each
region's average, and each customer's share of their region. Quotients by an AVG -- IQ types an AVG of a NUMERIC(p,s)
NUMERIC(p+22,s+21) -- are cast to a fixed NUMERIC, as such reports do."""

FINANCE = [
    ("monthly_target_against_actuals",
     """WITH actuals AS (
           SELECT YEAR(o.order_ts) * 100 + MONTH(o.order_ts) AS ym, SUM(oi.quantity * oi.unit_price) AS revenue
           FROM orders o JOIN order_items oi ON oi.order_id = o.id
           WHERE o.status <> 'cancelled' AND YEAR(o.order_ts) = 2025
           GROUP BY YEAR(o.order_ts) * 100 + MONTH(o.order_ts)),
         targets AS (
           SELECT ym, CAST(40000 + (ym - 202500) * 1500 AS NUMERIC(12,2)) AS target FROM actuals)
         SELECT a.ym, t.target, a.revenue, a.revenue - t.target AS variance,
           CAST(100 * (a.revenue - t.target) / NULLIF(t.target, 0) AS NUMERIC(7,2)) AS variance_pct,
           CASE WHEN a.revenue >= t.target THEN 'met' WHEN a.revenue >= 0.9 * t.target THEN 'close' ELSE 'missed' END AS status
         FROM actuals a JOIN targets t ON t.ym = a.ym
         ORDER BY a.ym"""),
    ("seven_order_moving_average",
     """WITH totals AS (
           SELECT o.id, CAST(o.order_ts AS DATE) AS ordered, SUM(oi.quantity * oi.unit_price) AS total
           FROM orders o JOIN order_items oi ON oi.order_id = o.id
           WHERE o.status <> 'cancelled' AND o.order_ts >= '2025-06-01'
           GROUP BY o.id, CAST(o.order_ts AS DATE))
         SELECT id, ordered, total,
           CAST(AVG(total) OVER (ORDER BY ordered, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS NUMERIC(12,2)) AS moving_average,
           CAST(total / AVG(total) OVER (ORDER BY ordered, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS NUMERIC(6,3)) AS against_average
         FROM totals ORDER BY ordered, id"""),
    ("month_over_month_change",
     """WITH monthly AS (
           SELECT DATEFORMAT(o.order_ts, 'yyyy-mm') AS month, COUNT(DISTINCT o.id) AS orders,
                  SUM(oi.quantity * oi.unit_price) AS revenue
           FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE o.status <> 'cancelled'
           GROUP BY DATEFORMAT(o.order_ts, 'yyyy-mm'))
         SELECT month, orders, revenue,
           LAG(revenue) OVER (ORDER BY month) AS prior_revenue,
           CAST(100 * (revenue - LAG(revenue) OVER (ORDER BY month)) / NULLIF(LAG(revenue) OVER (ORDER BY month), 0)
                AS NUMERIC(8,1)) AS change_pct
         FROM monthly WHERE month >= '2025-01' ORDER BY month"""),
    ("inventory_turnover_by_category",
     """WITH cogs AS (
           SELECT p.category, SUM(oi.quantity * p.unit_cost) AS cost_of_goods
           FROM order_items oi JOIN orders o ON o.id = oi.order_id JOIN products p ON p.id = oi.product_id
           WHERE o.status IN ('shipped', 'returned') AND YEAR(o.order_ts) = 2025
           GROUP BY p.category),
         stock AS (
           SELECT p.category, AVG(i.on_hand * p.unit_cost) AS average_stock_value
           FROM inventory i JOIN products p ON p.id = i.product_id
           GROUP BY p.category)
         SELECT c.category, c.cost_of_goods, CAST(s.average_stock_value AS NUMERIC(12,2)) AS average_stock_value,
           CAST(c.cost_of_goods / NULLIF(s.average_stock_value, 0) AS NUMERIC(8,2)) AS turnover,
           CAST(365 / NULLIF(c.cost_of_goods / NULLIF(s.average_stock_value, 0), 0) AS NUMERIC(8,1)) AS days_of_stock
         FROM cogs c JOIN stock s ON s.category = c.category ORDER BY c.category"""),
    ("first_order_cohort_revenue_retention",
     """WITH firsts AS (
           SELECT customer_id, MIN(CAST(order_ts AS DATE)) AS first_day FROM orders WHERE status <> 'cancelled'
           GROUP BY customer_id),
         revenue AS (
           SELECT f.customer_id, YEAR(f.first_day) AS cohort, YEAR(o.order_ts) - YEAR(f.first_day) AS years_after,
                  SUM(oi.quantity * oi.unit_price) AS revenue
           FROM firsts f JOIN orders o ON o.customer_id = f.customer_id AND o.status <> 'cancelled'
             JOIN order_items oi ON oi.order_id = o.id
           GROUP BY f.customer_id, YEAR(f.first_day), YEAR(o.order_ts) - YEAR(f.first_day))
         SELECT cohort, years_after, COUNT(DISTINCT customer_id) AS customers, SUM(revenue) AS cohort_revenue,
           CAST(100 * SUM(revenue) / SUM(SUM(CASE WHEN years_after = 0 THEN revenue ELSE 0 END)) OVER (PARTITION BY cohort)
                AS NUMERIC(8,1)) AS pct_of_first_year
         FROM revenue GROUP BY cohort, years_after ORDER BY cohort, years_after"""),
    ("discount_effect_on_quantity",
     """SELECT CASE WHEN o.discount_pct = 0 THEN 'none' WHEN o.discount_pct < 0.10 THEN 'under 10%' ELSE '10% or more' END AS discount_band,
           COUNT(*) AS lines, CAST(AVG(oi.quantity * 1.0) AS NUMERIC(6,2)) AS average_quantity,
           CAST(AVG(oi.unit_price) AS NUMERIC(10,2)) AS average_price,
           CAST(CORR(o.discount_pct, oi.quantity) AS NUMERIC(6,3)) AS discount_quantity_corr
         FROM orders o JOIN order_items oi ON oi.order_id = o.id
         WHERE o.status <> 'cancelled' AND o.discount_pct IS NOT NULL
         GROUP BY CASE WHEN o.discount_pct = 0 THEN 'none' WHEN o.discount_pct < 0.10 THEN 'under 10%' ELSE '10% or more' END
         ORDER BY 1"""),
    ("order_value_against_region_average",
     """WITH ordered AS (
           SELECT o.id, c.region_id, SUM(oi.quantity * oi.unit_price) AS total
           FROM orders o JOIN customers c ON c.id = o.customer_id JOIN order_items oi ON oi.order_id = o.id
           WHERE o.status <> 'cancelled' GROUP BY o.id, c.region_id)
         SELECT r.name AS region, COUNT(*) AS orders,
           CAST(AVG(d.total) AS NUMERIC(10,2)) AS average_order,
           CAST(MAX(d.total) / AVG(d.total) AS NUMERIC(6,2)) AS largest_against_average,
           SUM(CASE WHEN d.total > (SELECT AVG(d2.total) FROM ordered d2 WHERE d2.region_id = d.region_id) THEN 1 ELSE 0 END)
             AS above_region_average
         FROM ordered d JOIN regions r ON r.id = d.region_id
         GROUP BY r.name, d.region_id ORDER BY r.name"""),
    ("customer_share_of_region",
     """WITH spend AS (
           SELECT c.region_id, c.id, TRIM(c.last_name) AS customer, SUM(oi.quantity * oi.unit_price) AS spent
           FROM customers c JOIN orders o ON o.customer_id = c.id JOIN order_items oi ON oi.order_id = o.id
           WHERE o.status <> 'cancelled' GROUP BY c.region_id, c.id, c.last_name)
         SELECT region_id, customer, spent,
           CAST(100 * spent / SUM(spent) OVER (PARTITION BY region_id) AS NUMERIC(6,2)) AS share_of_region,
           RANK() OVER (PARTITION BY region_id ORDER BY spent DESC) AS rank_in_region
         FROM spend ORDER BY region_id, spent DESC, id"""),
]

CASES = [("finance_reports", FINANCE)]
