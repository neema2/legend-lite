"""Realistic report queries over the retail schema, part V: reports as long-time Sybase shops write them, in the
dialect's own idioms -- CONVERT with style codes, STR, IF ... ENDIF, STUFF, accounts-receivable aging by DAYS, a
second page of a ranking by TOP ... START AT, fixed-width export lines by SPACE, REPLICATE and RIGHT, weekday reports
by DATENAME and DATEPART, membership anniversaries by YMD, YEARS and MONTHS, quarter labels by ARGN, SKU validation by
PATINDEX and ISNUMERIC, e-mail addresses masked for privacy, and TRUNCNUM against ROUND on order lines."""

LEGACY = [
    ("customer_export_with_style_codes",
     """SELECT id, CONVERT(VARCHAR(8), signup_date, 112) AS signup_yyyymmdd,
           COALESCE(CONVERT(VARCHAR(10), birth_date, 101), '') AS birth_mmddyyyy,
           STR(credit_limit, 12, 2) AS credit_limit_text,
           IF is_active = 1 THEN 'Y' ELSE 'N' ENDIF AS active_flag,
           IF phone IS NULL THEN '' ELSE STUFF(phone, 1, 0, '+1 ') ENDIF AS phone_e164ish,
           UCASE(LEFT(segment, 1)) || LCASE(SUBSTR(segment, 2)) AS segment_label
         FROM customers ORDER BY id"""),
    ("receivables_aging_by_customer",
     """WITH paid AS (SELECT order_id, SUM(amount) AS amount_paid FROM payments GROUP BY order_id),
         open_orders AS (
           SELECT o.id, o.customer_id, CAST(o.order_ts AS DATE) AS ordered,
                  SUM(oi.quantity * oi.unit_price) - MAX(COALESCE(p.amount_paid, 0)) AS balance
           FROM orders o JOIN order_items oi ON oi.order_id = o.id LEFT JOIN paid p ON p.order_id = o.id
           WHERE o.status IN ('shipped', 'pending')
           GROUP BY o.id, o.customer_id, CAST(o.order_ts AS DATE))
         SELECT customer_id,
           SUM(IF DAYS(ordered, CAST('2025-12-31' AS DATE)) <= 30 THEN balance ELSE 0 ENDIF) AS current_30,
           SUM(IF DAYS(ordered, CAST('2025-12-31' AS DATE)) BETWEEN 31 AND 90 THEN balance ELSE 0 ENDIF) AS days_31_90,
           SUM(IF DAYS(ordered, CAST('2025-12-31' AS DATE)) BETWEEN 91 AND 365 THEN balance ELSE 0 ENDIF) AS days_91_365,
           SUM(IF DAYS(ordered, CAST('2025-12-31' AS DATE)) > 365 THEN balance ELSE 0 ENDIF) AS over_a_year,
           SUM(balance) AS total_open
         FROM open_orders WHERE balance > 0.005
         GROUP BY customer_id ORDER BY total_open DESC, customer_id"""),
    ("second_page_of_customers_by_revenue",
     """SELECT TOP 8 START AT 9 c.id, TRIM(c.last_name) AS customer, c.segment,
           SUM(oi.quantity * oi.unit_price) AS revenue, COUNT(DISTINCT o.id) AS order_count
         FROM customers c JOIN orders o ON o.customer_id = c.id JOIN order_items oi ON oi.order_id = o.id
         WHERE o.status <> 'cancelled'
         GROUP BY c.id, c.last_name, c.segment
         ORDER BY revenue DESC, c.id"""),
    ("fixed_width_price_list",
     """SELECT id,
           STRING(UCASE(LEFT(category, 3)), '/', REPLICATE('0', 4 - LENGTH(CAST(id AS VARCHAR(4)))), id) AS item_code,
           LEFT(name || SPACE(24), 24) || RIGHT(SPACE(10) || CAST(unit_price AS VARCHAR(10)), 10)
             || IF discontinued = 1 THEN ' D' ELSE '  ' ENDIF AS export_line,
           LENGTH(LEFT(name || SPACE(24), 24) || RIGHT(SPACE(10) || CAST(unit_price AS VARCHAR(10)), 10)) AS line_length
         FROM products ORDER BY category, id"""),
    ("orders_by_weekday_name_and_hour",
     """SELECT DOW(o.order_ts) AS day_no, DATENAME(WEEKDAY, o.order_ts) AS weekday, COUNT(*) AS orders,
           MIN(DATEPART(HOUR, o.order_ts)) AS earliest_hour, MAX(DATEPART(HOUR, o.order_ts)) AS latest_hour,
           SUM(IF o.channel = 'web' THEN 1 ELSE 0 ENDIF) AS web_orders,
           MIN(DATEPART(DAYOFYEAR, o.order_ts)) AS first_day_of_year_seen
         FROM orders o WHERE o.status <> 'cancelled'
         GROUP BY DOW(o.order_ts), DATENAME(WEEKDAY, o.order_ts)
         ORDER BY day_no"""),
    ("membership_anniversaries_2025",
     """SELECT id, TRIM(first_name) || ' ' || TRIM(last_name) AS customer, signup_date,
           YMD(2025, MONTH(signup_date), DAY(signup_date)) AS anniversary_2025,
           YEARS(signup_date, CAST('2025-12-31' AS DATE)) AS full_years,
           MONTHS(signup_date, CAST('2025-12-31' AS DATE)) AS full_months,
           DAYNAME(YMD(2025, MONTH(signup_date), DAY(signup_date))) AS anniversary_weekday
         FROM customers
         WHERE MONTH(signup_date) IN (11, 12)
         ORDER BY MONTH(signup_date), DAY(signup_date), id"""),
    ("quarter_labels_by_argn",
     """SELECT YEAR(o.order_ts) AS order_year,
           ARGN(QUARTER(o.order_ts), 'Q1 Jan-Mar', 'Q2 Apr-Jun', 'Q3 Jul-Sep', 'Q4 Oct-Dec') AS quarter_label,
           COUNT(DISTINCT o.id) AS orders, SUM(oi.quantity) AS units,
           SUM(oi.quantity * oi.unit_price) AS revenue
         FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE o.status <> 'cancelled'
         GROUP BY YEAR(o.order_ts), QUARTER(o.order_ts)
         ORDER BY order_year, quarter_label"""),
    ("sku_validation_by_patindex",
     """SELECT id, sku,
           CASE WHEN PATINDEX('%[^A-Z0-9-]%', sku) > 0 THEN 'unexpected character'
                WHEN CHARINDEX('-', sku) <> 4 THEN 'bad category code'
                WHEN ISNUMERIC(RIGHT(sku, 3)) = 0 THEN 'bad sequence'
                WHEN CAST(RIGHT(sku, 3) AS INTEGER) <> id THEN 'sequence differs from id'
                ELSE 'ok' END AS sku_check,
           LENGTH(sku) - LENGTH(REPLACE(sku, '-', '')) AS dashes
         FROM products ORDER BY id"""),
    ("emails_masked_for_privacy",
     """SELECT id,
           IF email IS NULL THEN '(none)'
           ELSE LEFT(email, 2) || REPLICATE('*', GREATER(LOCATE(email, '@') - 3, 1)) || SUBSTR(email, LOCATE(email, '@'))
           ENDIF AS masked_email,
           LOCATE(email, '@') - 1 AS local_part_length
         FROM customers WHERE MOD(id, 2) = 1 ORDER BY id"""),
    ("truncnum_against_round_on_lines",
     """SELECT o.id, COUNT(*) AS lines,
           SUM(ROUND(oi.quantity * oi.unit_price * (1 - o.discount_pct), 2)) AS rounded_total,
           SUM(TRUNCNUM(oi.quantity * oi.unit_price * (1 - o.discount_pct), 2)) AS truncated_total,
           SUM(ROUND(oi.quantity * oi.unit_price * (1 - o.discount_pct), 2))
             - SUM(TRUNCNUM(oi.quantity * oi.unit_price * (1 - o.discount_pct), 2)) AS rounding_gain
         FROM orders o JOIN order_items oi ON oi.order_id = o.id
         WHERE o.discount_pct > 0 AND o.id <= 60
         GROUP BY o.id ORDER BY o.id"""),
]

CASES = [("legacy_idiom_reports", LEGACY)]
