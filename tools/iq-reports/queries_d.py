"""Realistic report queries over the retail schema, part D: finance (aging, balances, commissions, tax), cohorts
(retention, gaps and islands, recency), rankings (top-N, rank changes, quantiles, percentiles) and mixed reports
(grouping sets, ANY and ALL, CTE chains, string and date formatting, nested calculations)."""

LINE = "oi.quantity * oi.unit_price - oi.line_discount"
SHIPPED_TOTALS = f"""(SELECT o.id, o.channel, SUM({LINE}) AS total FROM orders o JOIN order_items oi ON oi.order_id = o.id
    WHERE o.status = 'shipped' GROUP BY o.id, o.channel)"""

FINANCE = [
    ("invoice_aging_buckets",
     f"""WITH totals AS (
           SELECT o.id, o.customer_id, CAST(o.order_ts AS DATE) AS order_date, o.discount_pct, SUM({LINE}) AS gross
           FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE o.status <> 'cancelled'
           GROUP BY o.id, o.customer_id, CAST(o.order_ts AS DATE), o.discount_pct),
         paid AS (SELECT order_id, SUM(amount) AS paid FROM payments GROUP BY order_id),
         open_items AS (
           SELECT t.id, t.customer_id, ROUND(t.gross * (1 - t.discount_pct), 2) - COALESCE(p.paid, 0) AS open_amount,
                  DATEDIFF(DAY, t.order_date, '2026-01-31') AS age_days
           FROM totals t LEFT JOIN paid p ON p.order_id = t.id)
         SELECT c.id, TRIM(c.first_name) || ' ' || c.last_name AS customer,
           SUM(CASE WHEN x.age_days <= 30 THEN x.open_amount ELSE 0 END) AS current_0_30,
           SUM(CASE WHEN x.age_days BETWEEN 31 AND 60 THEN x.open_amount ELSE 0 END) AS days_31_60,
           SUM(CASE WHEN x.age_days BETWEEN 61 AND 90 THEN x.open_amount ELSE 0 END) AS days_61_90,
           SUM(CASE WHEN x.age_days > 90 THEN x.open_amount ELSE 0 END) AS over_90,
           SUM(x.open_amount) AS total_open
         FROM open_items x JOIN customers c ON c.id = x.customer_id WHERE x.open_amount > 0.01
         GROUP BY c.id, c.first_name, c.last_name ORDER BY total_open DESC, c.id"""),
    ("running_customer_balance",
     f"""WITH ledger AS (
           SELECT o.customer_id, o.order_ts AS entry_ts, 'invoice' AS kind, o.id AS ref, SUM({LINE}) AS amount
           FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE o.status = 'shipped'
           GROUP BY o.customer_id, o.order_ts, o.id
           UNION ALL
           SELECT o.customer_id, p.paid_ts, 'payment', p.id, -p.amount
           FROM payments p JOIN orders o ON o.id = p.order_id WHERE o.status = 'shipped')
         SELECT customer_id, entry_ts, kind, ref, amount,
           SUM(amount) OVER (PARTITION BY customer_id ORDER BY entry_ts, kind, ref
                             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS balance
         FROM ledger WHERE customer_id <= 4 ORDER BY customer_id, entry_ts, kind, ref"""),
    ("sales_commission_tiers",
     f"""WITH rep_sales AS (
           SELECT o.sales_rep_id, YEAR(o.order_ts) AS yr, SUM({LINE}) AS revenue
           FROM orders o JOIN order_items oi ON oi.order_id = o.id
           WHERE o.status IN ('shipped', 'returned') AND o.sales_rep_id IS NOT NULL
           GROUP BY o.sales_rep_id, YEAR(o.order_ts))
         SELECT e.id, e.first_name || ' ' || e.last_name AS rep, r.yr, r.revenue,
           ROUND(CASE WHEN r.revenue <= 50000 THEN r.revenue * 0.05
                      WHEN r.revenue <= 150000 THEN 2500 + (r.revenue - 50000) * 0.08
                      ELSE 10500 + (r.revenue - 150000) * 0.12 END, 2) AS commission
         FROM rep_sales r JOIN employees e ON e.id = r.sales_rep_id ORDER BY r.yr, commission DESC, e.id"""),
    ("payment_status_by_month",
     """SELECT DATEFORMAT(o.order_ts, 'yyyy-mm') AS month,
           SUM(CASE WHEN COALESCE(p.paid, 0) = 0 THEN 1 ELSE 0 END) AS unpaid,
           SUM(CASE WHEN p.payments > 1 THEN 1 ELSE 0 END) AS installments,
           SUM(CASE WHEN p.payments = 1 THEN 1 ELSE 0 END) AS single_payment,
           COUNT(*) AS orders
         FROM orders o LEFT JOIN (SELECT order_id, SUM(amount) AS paid, COUNT(*) AS payments FROM payments GROUP BY order_id) p
           ON p.order_id = o.id
         WHERE o.status <> 'cancelled' AND o.order_ts >= '2025-01-01'
         GROUP BY DATEFORMAT(o.order_ts, 'yyyy-mm') ORDER BY month"""),
    ("gross_to_net_by_channel",
     """WITH lines AS (SELECT order_id, SUM(quantity * unit_price) AS gross, SUM(line_discount) AS line_disc
                       FROM order_items GROUP BY order_id)
         SELECT o.channel, COUNT(*) AS orders, SUM(l.gross) AS gross, SUM(l.line_disc) AS line_discounts,
           SUM(ROUND((l.gross - l.line_disc) * o.discount_pct, 2)) AS order_discounts, SUM(o.shipping_fee) AS shipping,
           SUM(l.gross - l.line_disc - ROUND((l.gross - l.line_disc) * o.discount_pct, 2) + o.shipping_fee) AS net
         FROM orders o JOIN lines l ON l.order_id = o.id WHERE o.status = 'shipped'
         GROUP BY o.channel ORDER BY net DESC, o.channel"""),
    ("quarterly_sales_tax",
     f"""SELECT r.country, QUARTER(o.order_ts) AS qtr,
           'Q' || CAST(QUARTER(o.order_ts) AS VARCHAR(1)) || ' ' || CAST(YEAR(o.order_ts) AS VARCHAR(4)) AS period,
           SUM({LINE}) AS taxable,
           ROUND(SUM({LINE}) * CASE r.country WHEN 'Germany' THEN 0.19 WHEN 'Canada' THEN 0.13 ELSE 0.0725 END, 2) AS tax
         FROM orders o JOIN order_items oi ON oi.order_id = o.id JOIN customers c ON c.id = o.customer_id
           JOIN regions r ON r.id = c.region_id
         WHERE o.status = 'shipped' AND YEAR(o.order_ts) = 2025
         GROUP BY r.country, YEAR(o.order_ts), QUARTER(o.order_ts) ORDER BY r.country, qtr"""),
    ("credit_limit_exposure",
     """SELECT c.id, c.last_name, c.credit_limit, x.open_value, ROUND(100.0 * x.open_value / c.credit_limit, 1) AS pct_of_limit
         FROM customers c JOIN (SELECT o.customer_id, SUM(oi.quantity * oi.unit_price) AS open_value
                                FROM orders o JOIN order_items oi ON oi.order_id = o.id
                                WHERE o.status IN ('pending', 'shipped') AND o.order_ts >= '2025-07-01'
                                GROUP BY o.customer_id) x ON x.customer_id = c.id
         WHERE x.open_value > 0.25 * c.credit_limit ORDER BY pct_of_limit DESC, c.id"""),
]

COHORTS = [
    ("cohort_retention_by_first_year",
     """WITH firsts AS (SELECT customer_id, MIN(order_ts) AS first_ts FROM orders WHERE status <> 'cancelled' GROUP BY customer_id),
         activity AS (SELECT o.customer_id, YEAR(f.first_ts) AS cohort_year, DATEDIFF(MONTH, f.first_ts, o.order_ts) AS months_after
                      FROM orders o JOIN firsts f ON f.customer_id = o.customer_id WHERE o.status <> 'cancelled')
         SELECT cohort_year, COUNT(DISTINCT customer_id) AS customers,
           COUNT(DISTINCT CASE WHEN months_after BETWEEN 1 AND 6 THEN customer_id END) AS active_m1_6,
           COUNT(DISTINCT CASE WHEN months_after BETWEEN 7 AND 12 THEN customer_id END) AS active_m7_12,
           COUNT(DISTINCT CASE WHEN months_after > 12 THEN customer_id END) AS active_after_m12
         FROM activity GROUP BY cohort_year ORDER BY cohort_year"""),
    ("long_gaps_between_orders",
     """SELECT customer_id, id, order_ts, prev_ts, DATEDIFF(DAY, prev_ts, order_ts) AS gap_days
         FROM (SELECT customer_id, id, order_ts, LAG(order_ts) OVER (PARTITION BY customer_id ORDER BY order_ts, id) AS prev_ts
               FROM orders) x
         WHERE DATEDIFF(DAY, prev_ts, order_ts) > 180 ORDER BY customer_id, order_ts, id"""),
    ("islands_of_consecutive_months",
     """WITH months AS (SELECT DISTINCT customer_id, YEAR(order_ts) * 12 + MONTH(order_ts) - 1 AS month_no
                        FROM orders WHERE status <> 'cancelled'),
         numbered AS (SELECT customer_id, month_no,
                             month_no - ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY month_no) AS island FROM months)
         SELECT customer_id, island, MIN(month_no) AS first_month_no, MAX(month_no) AS last_month_no, COUNT(*) AS months_in_a_row
         FROM numbered GROUP BY customer_id, island HAVING COUNT(*) >= 2 ORDER BY customer_id, island"""),
    ("first_basket_categories",
     """WITH first_orders AS (SELECT id, customer_id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_ts, id) AS rn
                              FROM orders WHERE status <> 'cancelled')
         SELECT p.category, COUNT(DISTINCT f.customer_id) AS customers, SUM(oi.quantity) AS units
         FROM first_orders f JOIN order_items oi ON oi.order_id = f.id JOIN products p ON p.id = oi.product_id
         WHERE f.rn = 1 GROUP BY p.category ORDER BY customers DESC, p.category"""),
    ("recency_buckets",
     """SELECT CASE WHEN l.last_ts IS NULL THEN 'never'
                     WHEN DATEDIFF(MONTH, l.last_ts, '2026-01-01') <= 3 THEN '0-3 months'
                     WHEN DATEDIFF(MONTH, l.last_ts, '2026-01-01') <= 12 THEN '4-12 months' ELSE 'over a year' END AS recency,
           COUNT(*) AS customers, AVG(c.credit_limit) AS avg_limit
         FROM customers c LEFT JOIN (SELECT customer_id, MAX(order_ts) AS last_ts FROM orders GROUP BY customer_id) l
           ON l.customer_id = c.id
         GROUP BY CASE WHEN l.last_ts IS NULL THEN 'never'
                       WHEN DATEDIFF(MONTH, l.last_ts, '2026-01-01') <= 3 THEN '0-3 months'
                       WHEN DATEDIFF(MONTH, l.last_ts, '2026-01-01') <= 12 THEN '4-12 months' ELSE 'over a year' END
         ORDER BY recency"""),
    ("weekly_orders_rolling_four_weeks",
     """WITH weekly AS (SELECT DATEADD(DAY, 1 - DOW(order_ts), CAST(order_ts AS DATE)) AS week_start,
                               COUNT(DISTINCT customer_id) AS active, COUNT(*) AS orders
                        FROM orders WHERE order_ts >= '2025-07-01'
                        GROUP BY DATEADD(DAY, 1 - DOW(order_ts), CAST(order_ts AS DATE)))
         SELECT week_start, active, orders,
           SUM(orders) OVER (ORDER BY week_start ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS orders_4wk,
           MAX(active) OVER (ORDER BY week_start ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS peak_active_4wk
         FROM weekly ORDER BY week_start"""),
    ("new_vs_repeat_revenue_by_quarter",
     f"""WITH firsts AS (SELECT customer_id, MIN(order_ts) AS first_ts FROM orders WHERE status = 'shipped' GROUP BY customer_id)
         SELECT YEAR(o.order_ts) AS yr, QUARTER(o.order_ts) AS qtr,
           SUM(CASE WHEN o.order_ts = f.first_ts THEN {LINE} ELSE 0 END) AS first_order_revenue,
           SUM(CASE WHEN o.order_ts > f.first_ts THEN {LINE} ELSE 0 END) AS repeat_revenue,
           COUNT(DISTINCT CASE WHEN o.order_ts > f.first_ts THEN o.customer_id END) AS repeat_customers
         FROM orders o JOIN firsts f ON f.customer_id = o.customer_id JOIN order_items oi ON oi.order_id = o.id
         WHERE o.status = 'shipped' GROUP BY YEAR(o.order_ts), QUARTER(o.order_ts) ORDER BY yr, qtr"""),
]

RANKINGS = [
    ("top_three_customers_per_region",
     f"""SELECT region_id, region, customer_id, customer, revenue, rn FROM (
           SELECT r.id AS region_id, r.name AS region, c.id AS customer_id, TRIM(c.first_name) || ' ' || c.last_name AS customer,
                  SUM({LINE}) AS revenue,
                  ROW_NUMBER() OVER (PARTITION BY r.id ORDER BY SUM({LINE}) DESC, c.id) AS rn
           FROM regions r JOIN customers c ON c.region_id = r.id JOIN orders o ON o.customer_id = c.id
             JOIN order_items oi ON oi.order_id = o.id
           WHERE o.status = 'shipped' GROUP BY r.id, r.name, c.id, c.first_name, c.last_name) ranked
         WHERE rn <= 3 ORDER BY region_id, rn, customer_id"""),
    ("product_rank_change_2024_to_2025",
     f"""WITH yearly AS (SELECT oi.product_id, YEAR(o.order_ts) AS yr, SUM({LINE}) AS revenue
                        FROM order_items oi JOIN orders o ON o.id = oi.order_id
                        WHERE o.status = 'shipped' AND YEAR(o.order_ts) IN (2024, 2025) GROUP BY oi.product_id, YEAR(o.order_ts)),
         ranked AS (SELECT product_id, yr, revenue, RANK() OVER (PARTITION BY yr ORDER BY revenue DESC) AS rnk FROM yearly)
         SELECT p.id, p.name, a.rnk AS rank_2024, b.rnk AS rank_2025, a.rnk - b.rnk AS places_gained,
           CASE WHEN a.rnk IS NULL THEN 'new' WHEN b.rnk IS NULL THEN 'dropped' WHEN b.rnk < a.rnk THEN 'up'
                WHEN b.rnk > a.rnk THEN 'down' ELSE 'same' END AS movement
         FROM products p LEFT JOIN ranked a ON a.product_id = p.id AND a.yr = 2024
           LEFT JOIN ranked b ON b.product_id = p.id AND b.yr = 2025
         WHERE a.rnk IS NOT NULL OR b.rnk IS NOT NULL ORDER BY p.id"""),
    ("order_value_quartiles",
     f"""WITH q AS (SELECT id, total, NTILE(4) OVER (ORDER BY total, id) AS quartile FROM {SHIPPED_TOTALS} t)
         SELECT quartile, COUNT(*) AS orders, MIN(total) AS low, MAX(total) AS high, SUM(total) AS revenue
         FROM q GROUP BY quartile ORDER BY quartile"""),
    ("order_value_percentiles_by_channel",
     f"""SELECT channel, COUNT(*) AS orders, MEDIAN(total) AS median_total,
           PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY total) AS p90,
           PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY total) AS p25
         FROM {SHIPPED_TOTALS} t GROUP BY channel ORDER BY channel"""),
    ("salary_percentiles",
     """SELECT e.id, e.last_name, r.name AS region, e.salary, RANK() OVER (ORDER BY e.salary DESC) AS overall_rank,
           PERCENT_RANK() OVER (ORDER BY e.salary) AS pct_rank, CUME_DIST() OVER (ORDER BY e.salary) AS cume
         FROM employees e LEFT JOIN regions r ON r.id = e.region_id ORDER BY e.salary DESC, e.id"""),
    ("first_and_latest_product_per_customer",
     """SELECT DISTINCT o.customer_id,
           FIRST_VALUE(p.name) OVER (PARTITION BY o.customer_id ORDER BY o.order_ts, oi.id
                                     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_product,
           LAST_VALUE(p.name) OVER (PARTITION BY o.customer_id ORDER BY o.order_ts, oi.id
                                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_product,
           COUNT(*) OVER (PARTITION BY o.customer_id) AS lines
         FROM orders o JOIN order_items oi ON oi.order_id = o.id JOIN products p ON p.id = oi.product_id
         WHERE o.status <> 'cancelled' ORDER BY o.customer_id"""),
    ("second_highest_salary_per_title",
     """SELECT title, id, last_name, salary FROM (
           SELECT title, id, last_name, salary, DENSE_RANK() OVER (PARTITION BY title ORDER BY salary DESC) AS dr FROM employees) x
         WHERE dr = 2 ORDER BY title, id"""),
    ("top_product_per_month_2025",
     """WITH monthly AS (SELECT MONTH(o.order_ts) AS mo, oi.product_id, SUM(oi.quantity) AS units
                         FROM orders o JOIN order_items oi ON oi.order_id = o.id
                         WHERE o.status = 'shipped' AND YEAR(o.order_ts) = 2025 GROUP BY MONTH(o.order_ts), oi.product_id),
         ranked AS (SELECT mo, product_id, units, ROW_NUMBER() OVER (PARTITION BY mo ORDER BY units DESC, product_id) AS rn
                    FROM monthly)
         SELECT r.mo, p.name, r.units FROM ranked r JOIN products p ON p.id = r.product_id WHERE r.rn = 1 ORDER BY r.mo"""),
]

MIXED = [
    ("rollup_year_and_quarter",
     f"""SELECT YEAR(o.order_ts) AS yr, QUARTER(o.order_ts) AS qtr, COUNT(*) AS lines, SUM({LINE}) AS revenue,
           GROUPING(YEAR(o.order_ts)) AS g_year, GROUPING(QUARTER(o.order_ts)) AS g_qtr,
           CASE WHEN GROUPING(YEAR(o.order_ts)) = 1 THEN 'grand total' WHEN GROUPING(QUARTER(o.order_ts)) = 1
                THEN 'year total' ELSE 'quarter' END AS level
         FROM orders o JOIN order_items oi ON oi.order_id = o.id
         WHERE o.status = 'shipped' GROUP BY ROLLUP (YEAR(o.order_ts), QUARTER(o.order_ts))
         ORDER BY g_year, yr, g_qtr, qtr"""),
    ("cube_segment_by_year",
     """SELECT COALESCE(c.segment, 'all segments') AS segment,
           COALESCE(CAST(YEAR(o.order_ts) AS VARCHAR(4)), 'all years') AS yr, COUNT(*) AS orders, SUM(o.shipping_fee) AS shipping
         FROM orders o JOIN customers c ON c.id = o.customer_id WHERE o.status <> 'cancelled'
         GROUP BY CUBE (c.segment, YEAR(o.order_ts)) ORDER BY 1, 2"""),
    ("priced_above_every_home_product",
     """SELECT id, name, category, unit_price FROM products
         WHERE unit_price > ALL (SELECT unit_price FROM products WHERE category = 'Home') AND category <> 'Home'
         ORDER BY unit_price DESC, id"""),
    ("customers_in_manager_regions",
     """SELECT c.id, c.last_name, c.region_id FROM customers c
         WHERE c.region_id = ANY (SELECT region_id FROM employees WHERE title = 'Sales Manager') AND c.is_active = 1
         ORDER BY c.id"""),
    ("customer_scorecard_cte_chain",
     f"""WITH order_totals AS (SELECT o.id, o.customer_id, o.order_ts, SUM({LINE}) AS total
                              FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE o.status = 'shipped'
                              GROUP BY o.id, o.customer_id, o.order_ts),
         customer_stats AS (SELECT customer_id, COUNT(*) AS orders, SUM(total) AS revenue, MAX(order_ts) AS last_ts
                            FROM order_totals GROUP BY customer_id),
         payment_stats AS (SELECT o.customer_id, COUNT(DISTINCT p.method) AS methods
                           FROM payments p JOIN orders o ON o.id = p.order_id GROUP BY o.customer_id),
         scored AS (SELECT s.customer_id, s.orders, s.revenue, COALESCE(ps.methods, 0) AS methods,
                      CASE WHEN s.revenue >= 20000 THEN 3 WHEN s.revenue >= 5000 THEN 2 ELSE 1 END
                      + CASE WHEN s.orders >= 5 THEN 2 WHEN s.orders >= 3 THEN 1 ELSE 0 END
                      + CASE WHEN DATEDIFF(DAY, s.last_ts, '2026-01-01') <= 120 THEN 2 ELSE 0 END AS score
                    FROM customer_stats s LEFT JOIN payment_stats ps ON ps.customer_id = s.customer_id)
         SELECT c.id, TRIM(c.first_name) || ' ' || c.last_name AS customer, c.segment, sc.orders, sc.revenue, sc.methods, sc.score,
           CASE WHEN sc.score >= 6 THEN 'platinum' WHEN sc.score >= 4 THEN 'gold' WHEN sc.score >= 2 THEN 'silver'
                ELSE 'bronze' END AS tier
         FROM scored sc JOIN customers c ON c.id = sc.customer_id ORDER BY sc.score DESC, sc.revenue DESC, c.id"""),
    ("invoice_numbers",
     """SELECT o.id, 'INV-' || CAST(YEAR(o.order_ts) AS VARCHAR(4)) || '-' || RIGHT('00000' || CAST(o.id AS VARCHAR(10)), 5) AS invoice_no,
           UPPER(LEFT(o.channel, 1)) || SUBSTRING(o.channel, 2) AS channel_label,
           RIGHT(SPACE(10) || CAST(o.shipping_fee AS VARCHAR(10)), 10) AS fee_aligned
         FROM orders o WHERE o.id <= 15 ORDER BY o.id"""),
    ("email_tld_and_user_lengths",
     """SELECT CASE WHEN LOWER(email) LIKE '%.com' THEN 'com' WHEN LOWER(email) LIKE '%.org' THEN 'org' ELSE 'other' END AS tld,
           COUNT(*) AS customers, MIN(LOCATE(email, '@') - 1) AS shortest_user, MAX(LOCATE(email, '@') - 1) AS longest_user,
           SUM(CASE WHEN email <> LOWER(email) THEN 1 ELSE 0 END) AS not_lowercase
         FROM customers WHERE email IS NOT NULL
         GROUP BY CASE WHEN LOWER(email) LIKE '%.com' THEN 'com' WHEN LOWER(email) LIKE '%.org' THEN 'org' ELSE 'other' END
         ORDER BY customers DESC, tld"""),
    ("same_initial_rep_matches",
     """SELECT c.id AS customer_id, TRIM(c.first_name) || ' ' || c.last_name AS customer, e.id AS employee_id,
           e.first_name || ' ' || e.last_name AS employee
         FROM customers c JOIN employees e
           ON UPPER(LEFT(TRIM(c.first_name), 1)) = UPPER(LEFT(e.first_name, 1)) AND c.region_id = e.region_id
         ORDER BY c.id, e.id"""),
    ("fiscal_calendar_from_february",
     """SELECT YEAR(DATEADD(MONTH, 11, o.order_ts)) AS fiscal_year, QUARTER(DATEADD(MONTH, -1, o.order_ts)) AS fiscal_quarter,
           MIN(CAST(o.order_ts AS DATE)) AS first_order, MAX(CAST(o.order_ts AS DATE)) AS last_order, COUNT(*) AS orders
         FROM orders o WHERE o.status <> 'cancelled'
         GROUP BY YEAR(DATEADD(MONTH, 11, o.order_ts)), QUARTER(DATEADD(MONTH, -1, o.order_ts))
         ORDER BY fiscal_year, fiscal_quarter"""),
    ("business_days_to_ship",
     """SELECT o.id, CAST(o.order_ts AS DATE) AS order_date, o.ship_date, DATEDIFF(DAY, o.order_ts, o.ship_date) AS calendar_days,
           DATEDIFF(DAY, o.order_ts, o.ship_date) - 2 * DATEDIFF(WEEK, o.order_ts, o.ship_date) AS business_days,
           DATENAME(WEEKDAY, o.ship_date) AS ship_day
         FROM orders o WHERE o.ship_date IS NOT NULL AND o.id <= 40 ORDER BY o.id"""),
    ("next_birthdays",
     """SELECT id, birth_date,
           CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, birth_date, '2026-01-15'), birth_date) < '2026-01-15'
                THEN DATEADD(YEAR, DATEDIFF(YEAR, birth_date, '2026-01-15') + 1, birth_date)
                ELSE DATEADD(YEAR, DATEDIFF(YEAR, birth_date, '2026-01-15'), birth_date) END AS next_birthday
         FROM customers WHERE birth_date IS NOT NULL ORDER BY id"""),
    ("category_growth_rate",
     """WITH y AS (SELECT p.category,
                         SUM(CASE WHEN YEAR(o.order_ts) = 2023 THEN oi.quantity * oi.unit_price ELSE 0 END) AS rev_2023,
                         SUM(CASE WHEN YEAR(o.order_ts) = 2025 THEN oi.quantity * oi.unit_price ELSE 0 END) AS rev_2025
                  FROM order_items oi JOIN orders o ON o.id = oi.order_id JOIN products p ON p.id = oi.product_id
                  WHERE o.status = 'shipped' GROUP BY p.category)
         SELECT category, rev_2023, rev_2025, ROUND(SQRT(CAST(rev_2025 AS DOUBLE) / rev_2023) - 1, 4) AS annual_growth
         FROM y WHERE rev_2023 > 0 ORDER BY category"""),
    ("shipping_cost_estimate",
     """SELECT o.id, r.country, SUM(COALESCE(p.weight_kg, 1.0) * oi.quantity) AS kg,
           CASE WHEN r.country = 'USA' THEN 5.00 + 1.25 * CEILING(SUM(COALESCE(p.weight_kg, 1.0) * oi.quantity))
                WHEN r.country = 'Canada' THEN 12.00 + 2.10 * CEILING(SUM(COALESCE(p.weight_kg, 1.0) * oi.quantity))
                ELSE 18.00 + 3.40 * CEILING(SUM(COALESCE(p.weight_kg, 1.0) * oi.quantity)) END AS est_shipping,
           o.shipping_fee AS charged
         FROM orders o JOIN customers c ON c.id = o.customer_id JOIN regions r ON r.id = c.region_id
           JOIN order_items oi ON oi.order_id = o.id JOIN products p ON p.id = oi.product_id
         WHERE o.status = 'shipped' AND o.id <= 40 GROUP BY o.id, r.country, o.shipping_fee ORDER BY o.id"""),
    ("units_in_packs_of_four",
     """SELECT p.id, p.name, SUM(oi.quantity) AS units, FLOOR(SUM(oi.quantity) / 4) AS packs_of_4, MOD(SUM(oi.quantity), 4) AS singles
         FROM products p JOIN order_items oi ON oi.product_id = p.id GROUP BY p.id, p.name ORDER BY p.id"""),
    ("order_facts_by_correlated_subqueries",
     """SELECT o.id, o.status, (SELECT MAX(p.paid_ts) FROM payments p WHERE p.order_id = o.id) AS last_paid,
           (SELECT COUNT(*) FROM order_items oi WHERE oi.order_id = o.id AND oi.line_discount > 0) AS discounted_lines,
           (SELECT SUM(oi.quantity) FROM order_items oi WHERE oi.order_id = o.id) AS units
         FROM orders o WHERE o.customer_id IN (3, 7, 11) ORDER BY o.id"""),
    ("priced_above_category_average",
     """SELECT p.id, p.name, p.category, p.unit_price FROM products p
         WHERE p.unit_price > (SELECT AVG(p2.unit_price) FROM products p2 WHERE p2.category = p.category)
         ORDER BY p.category, p.unit_price DESC, p.id"""),
    ("orders_with_every_line_discounted",
     """SELECT o.id, o.customer_id FROM orders o
         WHERE EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id)
           AND NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id AND oi.line_discount = 0)
         ORDER BY o.id"""),
    ("line_statistics_by_category",
     """SELECT p.category, COUNT(*) AS lines, ROUND(AVG(oi.quantity * oi.unit_price), 2) AS avg_line,
           ROUND(STDDEV_SAMP(oi.quantity * oi.unit_price), 2) AS sd_line, ROUND(CORR(oi.quantity, oi.unit_price), 4) AS qty_price_corr
         FROM order_items oi JOIN products p ON p.id = oi.product_id GROUP BY p.category ORDER BY p.category"""),
    ("month_over_month_by_category",
     f"""WITH m AS (SELECT p.category, YEAR(o.order_ts) * 100 + MONTH(o.order_ts) AS ym, SUM({LINE}) AS revenue
                   FROM orders o JOIN order_items oi ON oi.order_id = o.id JOIN products p ON p.id = oi.product_id
                   WHERE o.status = 'shipped' AND o.order_ts >= '2025-01-01'
                   GROUP BY p.category, YEAR(o.order_ts) * 100 + MONTH(o.order_ts))
         SELECT category, ym, revenue, LAG(revenue) OVER (PARTITION BY category ORDER BY ym) AS prev_revenue,
           revenue - LAG(revenue) OVER (PARTITION BY category ORDER BY ym) AS change
         FROM m ORDER BY category, ym"""),
    ("hired_after_their_manager",
     """SELECT e.id, e.first_name || ' ' || e.last_name AS employee, e.hire_date, m.first_name || ' ' || m.last_name AS manager,
           m.hire_date AS manager_hired, DATEDIFF(MONTH, m.hire_date, e.hire_date) AS months_after_manager,
           e.salary - m.salary AS salary_gap
         FROM employees e JOIN employees m ON m.id = e.manager_id ORDER BY months_after_manager, e.id"""),
    ("payroll_by_region_with_share",
     """SELECT COALESCE(r.name, 'unassigned') AS region, COUNT(*) AS headcount, SUM(e.salary) AS payroll,
           ROUND(100.0 * SUM(e.salary) / SUM(SUM(e.salary)) OVER (), 2) AS payroll_share, AVG(e.salary) AS avg_salary
         FROM employees e LEFT JOIN regions r ON r.id = e.region_id GROUP BY r.name ORDER BY payroll DESC, region"""),
    ("orders_matching_campaign_rules",
     """SELECT o.id, o.channel, o.status, o.discount_pct, o.order_ts FROM orders o JOIN customers c ON c.id = o.customer_id
         WHERE ((o.channel = 'web' AND o.discount_pct >= 0.10) OR (o.channel IN ('store', 'phone') AND o.shipping_fee = 0))
           AND o.status NOT IN ('cancelled', 'returned') AND (c.segment = 'enterprise' OR c.credit_limit > 50000)
           AND MONTH(o.order_ts) NOT IN (7, 8) AND (o.ship_date IS NULL OR DATEDIFF(DAY, o.order_ts, o.ship_date) <= 5)
         ORDER BY o.order_ts, o.id"""),
    ("multichannel_customers",
     """SELECT c.id, c.last_name, COUNT(*) AS orders, COUNT(DISTINCT o.channel) AS channels, SUM(o.shipping_fee) AS shipping_paid,
           MAX(o.order_ts) AS last_order
         FROM customers c JOIN orders o ON o.customer_id = c.id WHERE o.status <> 'cancelled'
         GROUP BY c.id, c.last_name
         HAVING COUNT(DISTINCT o.channel) >= 2 AND COUNT(*) >= 3 AND MAX(o.order_ts) >= '2025-01-01' AND MIN(o.shipping_fee) = 0
         ORDER BY orders DESC, c.id"""),
    ("catalog_coverage",
     """SELECT o.customer_id, COUNT(DISTINCT oi.product_id) AS products_bought,
           ROUND(COUNT(DISTINCT oi.product_id) * 100.0 / (SELECT COUNT(*) FROM products WHERE discontinued = 0), 1) AS coverage_pct
         FROM orders o JOIN order_items oi ON oi.order_id = o.id GROUP BY o.customer_id
         HAVING COUNT(DISTINCT oi.product_id) >= 4 ORDER BY coverage_pct DESC, o.customer_id"""),
    ("product_name_words",
     """SELECT id, name, CASE WHEN LOCATE(name, ' ') > 0 THEN LEFT(name, LOCATE(name, ' ') - 1) ELSE name END AS first_word,
           CASE WHEN LOCATE(name, ' ') > 0 THEN TRIM(SUBSTRING(name, LOCATE(name, ' ') + 1)) ELSE '' END AS rest,
           LENGTH(name) - LENGTH(REPLACE(name, ' ', '')) + 1 AS word_count
         FROM products ORDER BY id"""),
    ("year_to_date_same_period",
     f"""SELECT YEAR(o.order_ts) AS yr, COUNT(DISTINCT o.id) AS orders_to_date, SUM({LINE}) AS revenue_to_date
         FROM orders o JOIN order_items oi ON oi.order_id = o.id
         WHERE o.status = 'shipped' AND DATEPART(DAYOFYEAR, o.order_ts) <= DATEPART(DAYOFYEAR, '2025-06-30')
         GROUP BY YEAR(o.order_ts) ORDER BY yr"""),
]

CASES = [("finance", FINANCE), ("cohorts", COHORTS), ("rankings", RANKINGS), ("mixed", MIXED)]
