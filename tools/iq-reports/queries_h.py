"""Realistic report queries over the retail schema, part H: the heavy scalar expressions reports are full of -- names,
usernames, initials and phone numbers rebuilt by hand, e-mail checks, the next SKU of a series; ages to the day, fiscal
and week labels, a month's first Monday and last weekday, weekend dates moved to Monday, timestamps cut to the hour; and
calculations -- compound growth, margin and markup, capped percentages, prices rounded to a nickel, z-scores."""

STRINGS = [
    ("usernames_and_initials",
     """SELECT id, LOWER(LEFT(TRIM(first_name), 1) || REPLACE(last_name, ' ', '')) || RIGHT('0' || CAST(id AS VARCHAR(3)), 2) AS username,
           UPPER(LEFT(TRIM(first_name), 1)) || '.' || UPPER(LEFT(last_name, 1)) || '.' AS initials,
           LENGTH(TRIM(first_name)) + LENGTH(last_name) + 1 AS full_name_length
         FROM customers ORDER BY id"""),
    ("phones_reformatted",
     """SELECT id, phone, digits,
           CASE WHEN LENGTH(digits) = 10 THEN '(' || LEFT(digits, 3) || ') ' || SUBSTRING(digits, 4, 3) || '-' || RIGHT(digits, 4)
                ELSE 'invalid' END AS formatted,
           CASE WHEN LENGTH(digits) = 10 THEN '+1-' || LEFT(digits, 3) || '-' || SUBSTRING(digits, 4, 3) || '-' || RIGHT(digits, 4) END AS e164ish
         FROM (SELECT id, phone, REPLACE(REPLACE(REPLACE(REPLACE(phone, '(', ''), ')', ''), '-', ''), ' ', '') AS digits
               FROM customers WHERE phone IS NOT NULL) p
         ORDER BY id"""),
    ("email_checks",
     """SELECT id, email,
           CASE WHEN email IS NULL THEN 'missing' WHEN email NOT LIKE '%_@_%._%' THEN 'malformed'
                WHEN LOCATE(email, '@') <> LOCATE(email, '@', LOCATE(email, '@') + 1) - 0 AND LOCATE(email, '@', LOCATE(email, '@') + 1) > 0
                THEN 'two @' ELSE 'ok' END AS check_result,
           LENGTH(email) - LENGTH(REPLACE(email, '.', '')) AS dots,
           CASE WHEN email IS NOT NULL THEN LENGTH(email) - LOCATE(email, '@') END AS domain_length
         FROM customers ORDER BY id"""),
    ("next_sku_in_series",
     """SELECT LEFT(sku, 8) AS series, MAX(sku) AS last_sku,
           LEFT(MAX(sku), 8) || RIGHT('000' || CAST(CAST(RIGHT(MAX(sku), 3) AS INTEGER) + 1 AS VARCHAR(4)), 3) AS next_sku,
           COUNT(*) AS products
         FROM products GROUP BY LEFT(sku, 8) ORDER BY series"""),
    ("display_names_with_titles",
     """SELECT e.id, CASE WHEN e.title LIKE 'VP%' THEN 'VP ' WHEN e.title LIKE '%Manager%' THEN 'Mgr ' ELSE '' END
                 || e.first_name || ' ' || UPPER(LEFT(e.last_name, 1)) || LOWER(SUBSTRING(e.last_name, 2)) AS display,
           COALESCE(m.first_name || ' ' || m.last_name, '(none)') AS reports_to,
           REPEAT('  ', CASE WHEN e.manager_id IS NULL THEN 0 WHEN m.manager_id IS NULL THEN 1 ELSE 2 END) || e.last_name AS indented
         FROM employees e LEFT JOIN employees m ON m.id = e.manager_id ORDER BY e.id"""),
]

DATES = [
    ("ages_to_the_day",
     """SELECT id, birth_date,
           DATEDIFF(YEAR, birth_date, '2026-01-15')
             - CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, birth_date, '2026-01-15'), birth_date) > '2026-01-15' THEN 1 ELSE 0 END AS age,
           DATEDIFF(DAY, DATEADD(YEAR, DATEDIFF(YEAR, birth_date, '2026-01-15')
             - CASE WHEN DATEADD(YEAR, DATEDIFF(YEAR, birth_date, '2026-01-15'), birth_date) > '2026-01-15' THEN 1 ELSE 0 END, birth_date),
             '2026-01-15') AS days_since_birthday
         FROM customers WHERE birth_date IS NOT NULL ORDER BY id"""),
    ("fiscal_and_week_labels",
     """SELECT id, CAST(order_ts AS DATE) AS order_date,
           'FY' || RIGHT(CAST(YEAR(DATEADD(MONTH, 6, order_ts)) AS VARCHAR(4)), 2) || '-Q' || CAST(QUARTER(DATEADD(MONTH, 6, order_ts)) AS VARCHAR(1)) AS fiscal_quarter,
           CAST(YEAR(order_ts) AS VARCHAR(4)) || '-W' || RIGHT('0' || CAST(DATEPART(WEEK, order_ts) AS VARCHAR(2)), 2) AS week_label,
           DATENAME(MONTH, order_ts) || ' ' || CAST(DAY(order_ts) AS VARCHAR(2)) AS spoken_date
         FROM orders WHERE id <= 30 ORDER BY id"""),
    ("first_monday_and_last_weekday",
     """SELECT DISTINCT YMD(YEAR(order_ts), MONTH(order_ts), 1) AS month_start,
           DATEADD(DAY, MOD(9 - DOW(YMD(YEAR(order_ts), MONTH(order_ts), 1)), 7), YMD(YEAR(order_ts), MONTH(order_ts), 1)) AS first_monday,
           DATEADD(DAY, CASE DOW(DATEADD(DAY, -1, DATEADD(MONTH, 1, YMD(YEAR(order_ts), MONTH(order_ts), 1))))
                          WHEN 1 THEN -2 WHEN 7 THEN -1 ELSE 0 END,
                   DATEADD(DAY, -1, DATEADD(MONTH, 1, YMD(YEAR(order_ts), MONTH(order_ts), 1)))) AS last_weekday
         FROM orders WHERE YEAR(order_ts) = 2025 ORDER BY month_start"""),
    ("ship_dates_moved_off_weekends",
     """SELECT id, ship_date, DATENAME(WEEKDAY, ship_date) AS weekday,
           CASE DOW(ship_date) WHEN 7 THEN DATEADD(DAY, 2, ship_date) WHEN 1 THEN DATEADD(DAY, 1, ship_date) ELSE ship_date END AS business_ship_date
         FROM orders WHERE ship_date IS NOT NULL AND id <= 50 ORDER BY id"""),
    ("orders_per_hour_slot",
     """SELECT DATEADD(HOUR, DATEDIFF(HOUR, '2000-01-01', order_ts), '2000-01-01') AS hour_slot, COUNT(*) AS orders
         FROM orders WHERE order_ts >= '2025-12-01' GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, '2000-01-01', order_ts), '2000-01-01')
         ORDER BY hour_slot"""),
    ("months_between_with_fraction",
     """SELECT id, signup_date, DATEDIFF(MONTH, signup_date, '2026-01-01') AS whole_months,
           ROUND(DATEDIFF(DAY, signup_date, '2026-01-01') / 30.44, 1) AS approx_months,
           DATEDIFF(DAY, signup_date, '2026-01-01') - 365 * (DATEDIFF(DAY, signup_date, '2026-01-01') / 365) AS days_past_whole_years
         FROM customers ORDER BY id"""),
]

CALCULATIONS = [
    ("credit_growth_projection",
     """SELECT id, credit_limit, ROUND(credit_limit * POWER(1.05, 3), 2) AS in_three_years,
           ROUND(credit_limit * (1 + 0.05 * 3), 2) AS simple_growth,
           CAST(credit_limit * 1.05 * 1.05 * 1.05 AS NUMERIC(14,2)) AS exact_compound
         FROM customers WHERE id <= 15 ORDER BY id"""),
    ("margin_and_markup",
     """SELECT id, name, unit_price, unit_cost, unit_price - unit_cost AS profit,
           ROUND(100 * (unit_price - unit_cost) / unit_price, 2) AS margin_pct,
           ROUND(100 * (unit_price - unit_cost) / unit_cost, 2) AS markup_pct,
           ROUND(unit_cost / (1 - 0.40), 2) AS price_for_40_margin
         FROM products ORDER BY id"""),
    ("capped_attainment",
     """SELECT e.id, e.last_name, COALESCE(s.revenue, 0) AS revenue,
           LESSER(100, ROUND(100.0 * COALESCE(s.revenue, 0) / (e.salary * 2), 1)) AS capped_pct,
           GREATER(0, COALESCE(s.revenue, 0) - e.salary * 2) AS above_target
         FROM employees e LEFT JOIN (SELECT o.sales_rep_id, SUM(oi.quantity * oi.unit_price) AS revenue
                                     FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE o.status = 'shipped'
                                     GROUP BY o.sales_rep_id) s ON s.sales_rep_id = e.id
         ORDER BY e.id"""),
    ("capped_attainment_by_case",
     """SELECT e.id, e.last_name, COALESCE(s.revenue, 0) AS revenue,
           CASE WHEN ROUND(100.0 * COALESCE(s.revenue, 0) / (e.salary * 2), 1) > 100 THEN 100
                ELSE ROUND(100.0 * COALESCE(s.revenue, 0) / (e.salary * 2), 1) END AS capped_pct
         FROM employees e LEFT JOIN (SELECT o.sales_rep_id, SUM(oi.quantity * oi.unit_price) AS revenue
                                     FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE o.status = 'shipped'
                                     GROUP BY o.sales_rep_id) s ON s.sales_rep_id = e.id
         ORDER BY e.id"""),
    ("prices_rounded_to_a_nickel",
     """SELECT id, unit_price, ROUND(unit_price * 20, 0) / 20 AS to_nickel, CEILING(unit_price) - 0.01 AS charm_price,
           FLOOR(unit_price / 10) * 10 AS price_floor_10, ROUND(unit_price, -1) AS nearest_ten
         FROM products ORDER BY id"""),
    ("order_value_z_scores",
     """SELECT id, total, ROUND((total - AVG(total) OVER ()) / NULLIF(STDDEV(total) OVER (), 0), 3) AS z,
           CASE WHEN ABS((total - AVG(total) OVER ()) / NULLIF(STDDEV(total) OVER (), 0)) > 2 THEN 'outlier' ELSE '' END AS flag
         FROM (SELECT o.id, CAST(SUM(oi.quantity * oi.unit_price - oi.line_discount) AS DOUBLE) AS total
               FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE o.status = 'shipped' GROUP BY o.id) t
         ORDER BY id"""),
    ("weighted_and_simple_averages",
     """SELECT p.category, ROUND(AVG(oi.unit_price), 2) AS simple_avg_price,
           ROUND(SUM(oi.unit_price * oi.quantity) / SUM(oi.quantity), 2) AS weighted_avg_price,
           ROUND(SUM(oi.line_discount) / NULLIF(SUM(oi.quantity * oi.unit_price), 0) * 100, 3) AS discount_rate_pct
         FROM order_items oi JOIN products p ON p.id = oi.product_id GROUP BY p.category ORDER BY p.category"""),
]

CASES = [("text_building", STRINGS), ("calendar", DATES), ("calculations", CALCULATIONS)]
