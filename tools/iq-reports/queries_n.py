"""Realistic report queries over the retail schema, part N: finance, people, data quality and calendars, as finance
analysts, HR analysts and data engineers write them -- receivables aging, a customer ledger's running balance over
invoices and payments, payment methods by year from a list of constants, headcount and payroll by month over a calendar
of sa_rowgenerator, salary bands with LIST, managers' spans with a skip-level subquery, commission tiers with LAG, lead
times by weekday, fiscal quarters of a year that starts in April, weeks that start on Monday, age bands with the
birthday counted; then city cohorts with FIRST_VALUE, a null-rate profile, integrity checks, days without orders,
SKU parsing with LOCATE, a 7-day moving average over a calendar, a quarterly pivot, reorder gaps per customer, each
product's cheapest supplier, a weekly ticket backlog and a formatted region directory. Quotients a report shows are
cast to a fixed NUMERIC, as such reports do."""

FINANCE_AND_PEOPLE = [
    ("receivables_aging",
     """WITH billed AS (SELECT o.id, o.customer_id, o.order_ts, SUM(oi.quantity * oi.unit_price) + o.shipping_fee AS billed
                       FROM orders o JOIN order_items oi ON oi.order_id = o.id
                       WHERE o.status IN ('shipped', 'returned', 'pending')
                       GROUP BY o.id, o.customer_id, o.order_ts, o.shipping_fee),
             paid AS (SELECT order_id, SUM(amount) AS paid FROM payments GROUP BY order_id)
         SELECT age_bucket, COUNT(*) AS invoices, SUM(outstanding) AS total_outstanding, MIN(order_ts) AS oldest,
           MAX(outstanding) AS largest
         FROM (SELECT b.id, b.order_ts, b.billed - COALESCE(p.paid, 0) AS outstanding,
                      CASE WHEN DATEDIFF(DAY, b.order_ts, '2026-01-01') <= 30 THEN 1
                           WHEN DATEDIFF(DAY, b.order_ts, '2026-01-01') <= 60 THEN 2
                           WHEN DATEDIFF(DAY, b.order_ts, '2026-01-01') <= 90 THEN 3 ELSE 4 END AS bucket_no,
                      CASE WHEN DATEDIFF(DAY, b.order_ts, '2026-01-01') <= 30 THEN '0-30'
                           WHEN DATEDIFF(DAY, b.order_ts, '2026-01-01') <= 60 THEN '31-60'
                           WHEN DATEDIFF(DAY, b.order_ts, '2026-01-01') <= 90 THEN '61-90' ELSE '90+' END AS age_bucket
               FROM billed b LEFT JOIN paid p ON p.order_id = b.id) x
         WHERE outstanding > 0 GROUP BY bucket_no, age_bucket ORDER BY bucket_no"""),
    ("customer_ledger_running_balance",
     """WITH entries AS (SELECT o.customer_id, CAST(o.order_ts AS DATE) AS entry_date, 'invoice' AS kind, o.id AS ref,
                               SUM(oi.quantity * oi.unit_price) AS debit, CAST(0 AS NUMERIC(12,2)) AS credit
                        FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE o.status <> 'cancelled'
                        GROUP BY o.customer_id, CAST(o.order_ts AS DATE), o.id
                        UNION ALL
                        SELECT o.customer_id, CAST(p.paid_ts AS DATE), 'payment', p.id, 0, p.amount
                        FROM payments p JOIN orders o ON o.id = p.order_id)
         SELECT customer_id, entry_date, kind, ref, debit, credit,
           SUM(debit - credit) OVER (PARTITION BY customer_id ORDER BY entry_date, kind, ref ROWS UNBOUNDED PRECEDING) AS balance
         FROM entries WHERE customer_id IN (2, 5, 9) ORDER BY customer_id, entry_date, kind, ref"""),
    ("payment_methods_by_year",
     """SELECT m.method, YEAR(p.paid_ts) AS yr, COUNT(p.id) AS payments, COALESCE(SUM(p.amount), 0) AS amount
         FROM (SELECT 'card' AS method FROM dummy UNION ALL SELECT 'paypal' FROM dummy
               UNION ALL SELECT 'invoice' FROM dummy UNION ALL SELECT 'wire' FROM dummy) m
           LEFT JOIN payments p ON p.method = m.method
         GROUP BY m.method, YEAR(p.paid_ts) ORDER BY m.method, yr"""),
    ("headcount_and_payroll_by_month",
     """SELECT g.row_num AS month_no, DATEADD(MONTH, g.row_num - 1, CAST('2025-01-01' AS DATE)) AS month_start,
           COUNT(e.id) AS headcount, CAST(COALESCE(SUM(e.salary), 0) / 12 AS NUMERIC(12,2)) AS monthly_payroll,
           SUM(CASE WHEN e.hire_date >= DATEADD(MONTH, g.row_num - 1, CAST('2025-01-01' AS DATE)) THEN 1 ELSE 0 END) AS hired_this_month
         FROM sa_rowgenerator(1, 12) g
           LEFT JOIN employees e ON e.hire_date < DATEADD(MONTH, g.row_num, CAST('2025-01-01' AS DATE))
         GROUP BY g.row_num ORDER BY g.row_num"""),
    ("salary_bands_by_title",
     """SELECT e.title, COUNT(*) AS staff, MIN(e.salary) AS low, MAX(e.salary) AS high, CAST(AVG(e.salary) AS NUMERIC(10,2)) AS mean,
           MAX(e.salary) - MIN(e.salary) AS spread, LIST(e.last_name, ', ' ORDER BY e.salary DESC, e.id) AS by_salary,
           CAST(AVG(DATEDIFF(MONTH, e.hire_date, CAST('2026-01-01' AS DATE))) / 12.0 AS NUMERIC(5,1)) AS avg_tenure_years
         FROM employees e GROUP BY e.title ORDER BY high DESC, e.title"""),
    ("managers_span_of_control",
     """SELECT m.id, m.first_name || ' ' || m.last_name AS manager, m.title, COUNT(e.id) AS direct_reports,
           (SELECT COUNT(*) FROM employees x WHERE x.manager_id IN (SELECT y.id FROM employees y WHERE y.manager_id = m.id)) AS skip_level_reports,
           COALESCE(SUM(e.salary), 0) AS team_salary,
           CASE WHEN COUNT(e.id) = 0 THEN 'individual contributor' WHEN COUNT(e.id) >= 2 THEN 'manager of many' ELSE 'manager' END AS kind
         FROM employees m LEFT JOIN employees e ON e.manager_id = m.id
         GROUP BY m.id, m.first_name, m.last_name, m.title ORDER BY m.id"""),
    ("sales_rep_commission_tiers",
     """WITH rev AS (SELECT o.sales_rep_id, YEAR(o.order_ts) AS yr, SUM(oi.quantity * oi.unit_price * (1 - o.discount_pct)) AS revenue
                    FROM orders o JOIN order_items oi ON oi.order_id = o.id
                    WHERE o.status = 'shipped' AND o.sales_rep_id IS NOT NULL
                    GROUP BY o.sales_rep_id, YEAR(o.order_ts))
         SELECT r.sales_rep_id, e.last_name, r.yr, CAST(r.revenue AS NUMERIC(12,2)) AS revenue,
           CASE WHEN r.revenue >= 20000 THEN 'gold' WHEN r.revenue >= 10000 THEN 'silver' ELSE 'bronze' END AS tier,
           CAST(r.revenue * CASE WHEN r.revenue >= 20000 THEN 0.08 WHEN r.revenue >= 10000 THEN 0.05 ELSE 0.03 END AS NUMERIC(10,2)) AS commission,
           CAST(r.revenue - LAG(r.revenue) OVER (PARTITION BY r.sales_rep_id ORDER BY r.yr) AS NUMERIC(12,2)) AS change_vs_prior_year
         FROM rev r JOIN employees e ON e.id = r.sales_rep_id ORDER BY r.sales_rep_id, r.yr"""),
    ("fulfilment_lead_time_by_weekday",
     """SELECT o.channel, DOW(o.order_ts) AS dow, DATENAME(WEEKDAY, o.order_ts) AS weekday, COUNT(*) AS orders,
           SUM(CASE WHEN o.ship_date IS NULL THEN 1 ELSE 0 END) AS unshipped,
           CAST(AVG(DATEDIFF(DAY, o.order_ts, o.ship_date)) AS NUMERIC(6,2)) AS avg_days,
           MAX(DATEDIFF(DAY, o.order_ts, o.ship_date)) AS worst_days
         FROM orders o WHERE o.status <> 'cancelled'
         GROUP BY o.channel, DOW(o.order_ts), DATENAME(WEEKDAY, o.order_ts) ORDER BY o.channel, dow"""),
    ("fiscal_quarters_from_april",
     """SELECT 'FY' || CAST(f.fy AS VARCHAR(4)) || '-Q' || CAST(f.fq AS VARCHAR(1)) AS fiscal_quarter,
           COUNT(DISTINCT f.id) AS orders, SUM(oi.quantity * oi.unit_price) AS revenue, MIN(f.d) AS first_day, MAX(f.d) AS last_day
         FROM (SELECT id, CAST(order_ts AS DATE) AS d, YEAR(DATEADD(MONTH, 9, order_ts)) AS fy, QUARTER(DATEADD(MONTH, -3, order_ts)) AS fq
               FROM orders WHERE status <> 'cancelled') f
           JOIN order_items oi ON oi.order_id = f.id
         GROUP BY f.fy, f.fq ORDER BY f.fy, f.fq"""),
    ("weeks_starting_monday",
     """SELECT w.week_start, DATEFORMAT(w.week_start, 'Mmm dd') AS label, COUNT(DISTINCT w.id) AS orders, SUM(oi.quantity) AS units
         FROM (SELECT o.id, DATEADD(DAY, -MOD(DOW(o.order_ts) + 5, 7), CAST(o.order_ts AS DATE)) AS week_start
               FROM orders o WHERE o.order_ts >= '2025-09-01') w
           JOIN order_items oi ON oi.order_id = w.id
         GROUP BY w.week_start ORDER BY w.week_start"""),
    ("customer_age_bands_with_birthdays",
     """SELECT CASE WHEN age < 30 THEN 'under 30' WHEN age < 45 THEN '30-44' WHEN age < 60 THEN '45-59' ELSE '60+' END AS band,
           COUNT(*) AS customers, CAST(AVG(credit_limit) AS NUMERIC(10,2)) AS avg_credit, MIN(age) AS youngest, MAX(age) AS oldest
         FROM (SELECT id, credit_limit,
                      DATEDIFF(YEAR, birth_date, CAST('2026-01-01' AS DATE))
                        - CASE WHEN MONTH(birth_date) * 100 + DAY(birth_date) > 101 THEN 1 ELSE 0 END AS age
               FROM customers WHERE birth_date IS NOT NULL) c
         GROUP BY CASE WHEN age < 30 THEN 'under 30' WHEN age < 45 THEN '30-44' WHEN age < 60 THEN '45-59' ELSE '60+' END
         ORDER BY MIN(age)"""),
]

QUALITY_AND_CALENDAR = [
    ("customers_by_city_cohort",
     """SELECT c.city, c.id, c.last_name, COUNT(*) OVER (PARTITION BY c.city) AS same_city,
           ROW_NUMBER() OVER (PARTITION BY c.city ORDER BY c.signup_date, c.id) AS signup_order,
           FIRST_VALUE(c.last_name) OVER (PARTITION BY c.city ORDER BY c.signup_date, c.id
                                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_in_city
         FROM customers c ORDER BY c.city, signup_order"""),
    ("null_rate_profile",
     """SELECT 'orders' AS tbl, COUNT(*) AS rows_total, SUM(CASE WHEN ship_date IS NULL THEN 1 ELSE 0 END) AS nulls_a,
           SUM(CASE WHEN sales_rep_id IS NULL THEN 1 ELSE 0 END) AS nulls_b,
           CAST(100.0 * SUM(CASE WHEN ship_date IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS NUMERIC(5,1)) AS pct_a
         FROM orders
         UNION ALL
         SELECT 'customers', COUNT(*), SUM(CASE WHEN phone IS NULL THEN 1 ELSE 0 END), SUM(CASE WHEN birth_date IS NULL THEN 1 ELSE 0 END),
           CAST(100.0 * SUM(CASE WHEN phone IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS NUMERIC(5,1))
         FROM customers
         UNION ALL
         SELECT 'web_events', COUNT(*), SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END), SUM(CASE WHEN referrer IS NULL THEN 1 ELSE 0 END),
           CAST(100.0 * SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS NUMERIC(5,1))
         FROM web_events
         ORDER BY 1"""),
    ("integrity_checks",
     """SELECT 'order without items' AS check_name, COUNT(*) AS violations FROM orders o
         WHERE NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id)
         UNION ALL
         SELECT 'payments above the bill', COUNT(*)
         FROM (SELECT order_id, SUM(amount) AS paid FROM payments GROUP BY order_id) x
           JOIN (SELECT order_id, SUM(quantity * unit_price) AS billed FROM order_items GROUP BY order_id) y ON y.order_id = x.order_id
         WHERE x.paid > y.billed
         UNION ALL
         SELECT 'ticket on another customer''s order', COUNT(*) FROM support_tickets t JOIN orders o ON o.id = t.order_id
         WHERE o.customer_id <> t.customer_id
         UNION ALL
         SELECT 'shipped before ordered', COUNT(*) FROM orders WHERE ship_date < CAST(order_ts AS DATE)
         UNION ALL
         SELECT 'stock below zero', COUNT(*) FROM inventory WHERE on_hand < 0
         ORDER BY 1"""),
    ("days_without_orders_in_june",
     """SELECT DATEADD(DAY, g.row_num - 1, CAST('2025-06-01' AS DATE)) AS day,
           DATENAME(WEEKDAY, DATEADD(DAY, g.row_num - 1, CAST('2025-06-01' AS DATE))) AS weekday, COUNT(o.id) AS orders
         FROM sa_rowgenerator(1, 30) g
           LEFT JOIN orders o ON CAST(o.order_ts AS DATE) = DATEADD(DAY, g.row_num - 1, CAST('2025-06-01' AS DATE))
         GROUP BY g.row_num ORDER BY g.row_num"""),
    ("sku_parsing",
     """SELECT p.sku, LEFT(p.sku, LOCATE(p.sku, '-') - 1) AS dept,
           SUBSTR(p.sku, LOCATE(p.sku, '-') + 1, LOCATE(p.sku, '-', LOCATE(p.sku, '-') + 1) - LOCATE(p.sku, '-') - 1) AS line,
           CAST(SUBSTR(p.sku, LOCATE(p.sku, '-', LOCATE(p.sku, '-') + 1) + 1) AS INTEGER) AS seq,
           CASE WHEN UPPER(LEFT(p.subcategory, 3)) = SUBSTR(p.sku, 5, 3) THEN 'consistent' ELSE 'check' END AS sku_matches,
           p.name, LENGTH(p.name) AS name_length, REPLACE(LOWER(p.name), ' ', '-') AS slug
         FROM products p ORDER BY dept, line, seq"""),
    ("receipts_seven_day_moving_average",
     """WITH days AS (SELECT DATEADD(DAY, n.id - 1, CAST('2025-09-01' AS DATE)) AS d FROM order_items n WHERE n.id <= 61),
             daily AS (SELECT d.d, COALESCE(SUM(p.amount), 0) AS receipts
                       FROM days d LEFT JOIN payments p ON CAST(p.paid_ts AS DATE) = d.d GROUP BY d.d)
         SELECT d, receipts, CAST(AVG(receipts) OVER (ORDER BY d ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS NUMERIC(10,2)) AS avg_7d,
           SUM(receipts) OVER (ORDER BY d ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sum_7d
         FROM daily ORDER BY d"""),
    ("quarterly_category_pivot_2025",
     """SELECT p.category,
           SUM(CASE WHEN QUARTER(o.order_ts) = 1 THEN oi.quantity * oi.unit_price ELSE 0 END) AS q1,
           SUM(CASE WHEN QUARTER(o.order_ts) = 2 THEN oi.quantity * oi.unit_price ELSE 0 END) AS q2,
           SUM(CASE WHEN QUARTER(o.order_ts) = 3 THEN oi.quantity * oi.unit_price ELSE 0 END) AS q3,
           SUM(CASE WHEN QUARTER(o.order_ts) = 4 THEN oi.quantity * oi.unit_price ELSE 0 END) AS q4,
           SUM(oi.quantity * oi.unit_price) AS total,
           CASE WHEN SUM(CASE WHEN QUARTER(o.order_ts) = 4 THEN oi.quantity * oi.unit_price ELSE 0 END)
                     > SUM(CASE WHEN QUARTER(o.order_ts) = 1 THEN oi.quantity * oi.unit_price ELSE 0 END) THEN 'up' ELSE 'flat or down' END AS q4_vs_q1
         FROM orders o JOIN order_items oi ON oi.order_id = o.id JOIN products p ON p.id = oi.product_id
         WHERE YEAR(o.order_ts) = 2025 AND o.status <> 'cancelled'
         GROUP BY p.category ORDER BY p.category"""),
    ("reorder_rhythm_per_customer",
     """WITH seq AS (SELECT customer_id, id, order_ts, LAG(order_ts) OVER (PARTITION BY customer_id ORDER BY order_ts, id) AS prev_ts
                    FROM orders WHERE status <> 'cancelled')
         SELECT customer_id, COUNT(*) AS orders, MIN(order_ts) AS first_order, MAX(order_ts) AS last_order,
           CAST(AVG(DATEDIFF(DAY, prev_ts, order_ts)) AS NUMERIC(8,1)) AS avg_days_between, MAX(DATEDIFF(DAY, prev_ts, order_ts)) AS longest_gap,
           DATEADD(DAY, CAST(AVG(DATEDIFF(DAY, prev_ts, order_ts)) AS INTEGER), MAX(order_ts)) AS next_expected
         FROM seq GROUP BY customer_id HAVING COUNT(*) >= 3 ORDER BY customer_id"""),
    ("cheapest_supplier_offer",
     """SELECT p.id, p.name, s.name AS supplier, x.unit_cost, x.lead_days, x.offers, p.unit_price - x.unit_cost AS margin_per_unit
         FROM (SELECT sp.product_id, sp.supplier_id, sp.unit_cost, sp.lead_days,
                      COUNT(*) OVER (PARTITION BY sp.product_id) AS offers,
                      ROW_NUMBER() OVER (PARTITION BY sp.product_id ORDER BY sp.unit_cost, sp.lead_days, sp.id) AS rn
               FROM supplier_products sp) x
           JOIN products p ON p.id = x.product_id JOIN suppliers s ON s.id = x.supplier_id
         WHERE x.rn = 1 ORDER BY p.id"""),
    ("weekly_ticket_backlog",
     """SELECT DATEADD(WEEK, g.row_num, CAST('2025-01-05' AS DATE)) AS week_end, COUNT(t.id) AS open_tickets,
           SUM(CASE WHEN t.priority IN ('urgent', 'high') THEN 1 ELSE 0 END) AS open_high
         FROM sa_rowgenerator(1, 12) g
           LEFT JOIN support_tickets t ON t.opened_ts < DATEADD(WEEK, g.row_num, CAST('2025-01-05' AS DATE))
             AND (t.closed_ts IS NULL OR t.closed_ts >= DATEADD(WEEK, g.row_num, CAST('2025-01-05' AS DATE)))
         GROUP BY g.row_num ORDER BY g.row_num"""),
    ("region_directory_formatted",
     """SELECT r.id, r.name AS region, COUNT(c.id) AS customers,
           RIGHT(SPACE(12) || CAST(COALESCE(SUM(c.credit_limit), 0) AS VARCHAR(20)), 12) AS credit_right_aligned,
           UPPER(LEFT(r.country, 3)) || '-' || RIGHT('00' || CAST(r.id AS VARCHAR(2)), 2) AS region_code,
           REPEAT('#', COUNT(c.id)) AS bar, LIST(DISTINCT c.segment, '/' ORDER BY c.segment) AS segments
         FROM regions r LEFT JOIN customers c ON c.region_id = r.id GROUP BY r.id, r.name, r.country ORDER BY r.id"""),
]

CASES = [("finance_and_people", FINANCE_AND_PEOPLE), ("quality_and_calendar", QUALITY_AND_CALENDAR)]
