"""Realistic report queries over the retail schema, part T: customer data hygiene and service reports, as CRM and
support analysts write them -- e-mail domains, phone numbers stripped to digits and formatted again, duplicate
customer candidates by a self-join on OR'd conditions, exact ages from birth dates in bands, upcoming birthdays across
the year end, ticket SLAs by priority, ticket topics from subject keywords, business days to ship, SKUs taken apart and
product slugs, customer status by correlated subqueries, open balances of billed against paid, and a supplier
directory cleaned up."""

CUSTOMERS = [
    ("email_domain_breakdown",
     """SELECT LOWER(SUBSTRING(email, LOCATE(email, '@') + 1)) AS domain,
           CASE WHEN LOWER(SUBSTRING(email, LOCATE(email, '@') + 1)) IN ('gmail.com', 'yahoo.com', 'outlook.com')
                THEN 'consumer' ELSE 'business' END AS kind,
           COUNT(*) AS customers, SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active,
           SUM(CASE WHEN email <> LOWER(email) THEN 1 ELSE 0 END) AS not_lower_case
         FROM customers WHERE email IS NOT NULL
         GROUP BY LOWER(SUBSTRING(email, LOCATE(email, '@') + 1))
         ORDER BY customers DESC, domain"""),
    ("phone_numbers_normalized",
     """WITH cleaned AS (
           SELECT id, TRIM(first_name) || ' ' || TRIM(last_name) AS customer, phone,
                  REPLACE(REPLACE(REPLACE(REPLACE(phone, '(', ''), ')', ''), '-', ''), ' ', '') AS digits
           FROM customers WHERE phone IS NOT NULL)
         SELECT id, customer, phone, digits,
           '(' || LEFT(digits, 3) || ') ' || SUBSTRING(digits, 4, 3) || '-' || RIGHT(digits, 4) AS formatted,
           CASE WHEN LENGTH(digits) = 10 THEN 'ok' ELSE 'check' END AS validity,
           LEFT(digits, 3) AS area_code
         FROM cleaned ORDER BY area_code, id"""),
    ("duplicate_customer_candidates",
     """SELECT a.id AS first_id, b.id AS second_id,
           TRIM(a.first_name) || ' ' || TRIM(a.last_name) AS first_customer,
           TRIM(b.first_name) || ' ' || TRIM(b.last_name) AS second_customer,
           CASE WHEN LOWER(a.email) = LOWER(b.email) THEN 'same e-mail'
                WHEN TRIM(a.last_name) = TRIM(b.last_name) AND a.city = b.city THEN 'same name and city'
                ELSE 'similar name, same region' END AS reason
         FROM customers a JOIN customers b ON a.id < b.id
           AND (LOWER(a.email) = LOWER(b.email)
                OR TRIM(a.last_name) = TRIM(b.last_name) AND a.city = b.city
                OR SOUNDEX(a.last_name) = SOUNDEX(b.last_name) AND a.region_id = b.region_id)
         ORDER BY a.id, b.id"""),
    ("age_bands_by_segment",
     """WITH ages AS (
           SELECT id, segment, credit_limit,
                  2025 - YEAR(birth_date) - CASE WHEN MONTH(birth_date) * 100 + DAY(birth_date) > 630 THEN 1 ELSE 0 END
                    AS age
           FROM customers WHERE birth_date IS NOT NULL)
         SELECT CASE WHEN age < 35 THEN 'under 35' WHEN age < 50 THEN '35-49' WHEN age < 65 THEN '50-64' ELSE '65+' END
             AS age_band,
           segment, COUNT(*) AS customers, CAST(AVG(credit_limit) AS NUMERIC(10,2)) AS average_limit,
           MIN(age) AS youngest, MAX(age) AS oldest
         FROM ages
         GROUP BY CASE WHEN age < 35 THEN 'under 35' WHEN age < 50 THEN '35-49' WHEN age < 65 THEN '50-64' ELSE '65+' END,
           segment
         ORDER BY 1, 2"""),
    ("birthdays_in_the_next_45_days",
     """WITH birthdays AS (
           SELECT id, TRIM(first_name) || ' ' || TRIM(last_name) AS customer, birth_date,
                  CASE WHEN DATEADD(YEAR, 2025 - YEAR(birth_date), birth_date) >= '2025-11-20'
                       THEN DATEADD(YEAR, 2025 - YEAR(birth_date), birth_date)
                       ELSE DATEADD(YEAR, 2026 - YEAR(birth_date), birth_date) END AS upcoming
           FROM customers WHERE birth_date IS NOT NULL)
         SELECT id, customer, birth_date, CAST(upcoming AS DATE) AS upcoming_birthday,
           DATEDIFF(DAY, CAST('2025-11-20' AS DATE), upcoming) AS days_away,
           DATENAME(WEEKDAY, upcoming) AS weekday
         FROM birthdays WHERE DATEDIFF(DAY, CAST('2025-11-20' AS DATE), upcoming) <= 45
         ORDER BY days_away, id"""),
    ("customer_status_by_subqueries",
     """SELECT c.id, TRIM(c.last_name) AS customer, c.signup_date,
           DATEDIFF(MONTH, c.signup_date, CAST('2025-12-31' AS DATE)) AS tenure_months,
           (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id AND o.status <> 'cancelled') AS order_count,
           (SELECT MAX(CAST(o.order_ts AS DATE)) FROM orders o WHERE o.customer_id = c.id) AS last_order,
           COALESCE((SELECT SUM(p.amount) FROM payments p JOIN orders o ON o.id = p.order_id
                     WHERE o.customer_id = c.id), 0) AS total_paid,
           CASE WHEN NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id) THEN 'never ordered'
                WHEN (SELECT MAX(o.order_ts) FROM orders o WHERE o.customer_id = c.id) < '2025-01-01' THEN 'dormant'
                ELSE 'active' END AS customer_status
         FROM customers c ORDER BY c.id"""),
    ("open_balances_billed_against_paid",
     """WITH billed AS (
           SELECT o.id, o.customer_id,
                  SUM(oi.quantity * oi.unit_price - oi.line_discount) * (1 - o.discount_pct) AS amount_billed
           FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE o.status <> 'cancelled'
           GROUP BY o.id, o.customer_id, o.discount_pct),
         paid AS (SELECT order_id, SUM(amount) AS amount_paid, COUNT(*) AS payment_count FROM payments GROUP BY order_id)
         SELECT b.customer_id, COUNT(*) AS orders_billed, SUM(b.amount_billed) AS total_billed,
           SUM(COALESCE(p.amount_paid, 0)) AS total_paid,
           SUM(b.amount_billed) - SUM(COALESCE(p.amount_paid, 0)) AS open_balance,
           SUM(CASE WHEN p.order_id IS NULL THEN 1 ELSE 0 END) AS unpaid_orders
         FROM billed b LEFT JOIN paid p ON p.order_id = b.id
         GROUP BY b.customer_id
         HAVING SUM(b.amount_billed) - SUM(COALESCE(p.amount_paid, 0)) > 1
             OR SUM(CASE WHEN p.order_id IS NULL THEN 1 ELSE 0 END) > 0
         ORDER BY open_balance DESC, b.customer_id"""),
]

SERVICE = [
    ("support_sla_by_priority",
     """SELECT priority, COUNT(*) AS tickets, COUNT(closed_ts) AS closed,
           SUM(CASE WHEN closed_ts IS NULL THEN 1 ELSE 0 END) AS still_open,
           CAST(AVG(DATEDIFF(MINUTE, opened_ts, closed_ts) / 60.0) AS NUMERIC(8,1)) AS average_hours,
           MAX(DATEDIFF(HOUR, opened_ts, closed_ts)) AS longest_hours,
           SUM(CASE WHEN DATEDIFF(HOUR, opened_ts, closed_ts) >
                         CASE priority WHEN 'urgent' THEN 4 WHEN 'high' THEN 24 WHEN 'normal' THEN 48 ELSE 120 END
                    THEN 1 ELSE 0 END) AS sla_breaches
         FROM support_tickets GROUP BY priority
         ORDER BY CASE priority WHEN 'urgent' THEN 1 WHEN 'high' THEN 2 WHEN 'normal' THEN 3 ELSE 4 END"""),
    ("ticket_topics_from_subjects",
     """SELECT topic, COUNT(*) AS tickets, COUNT(DISTINCT customer_id) AS customers,
           SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS without_order, MAX(LENGTH(TRIM(subject))) AS longest_subject
         FROM (SELECT customer_id, order_id, subject,
                 CASE WHEN LOWER(subject) LIKE '%refund%' THEN 'refund'
                      WHEN LOWER(subject) LIKE '%cancel%' THEN 'cancellation'
                      WHEN LOWER(subject) LIKE '%damaged%' OR LOWER(subject) LIKE '%missing%'
                        OR LOWER(subject) LIKE '%wrong%' THEN 'fulfilment issue'
                      WHEN LOWER(subject) LIKE '%where is%' OR LOWER(subject) LIKE '%shipping%' THEN 'delivery'
                      WHEN LOWER(subject) LIKE '%login%' THEN 'account'
                      ELSE 'other' END AS topic
               FROM support_tickets) t
         GROUP BY topic ORDER BY tickets DESC, topic"""),
    ("business_days_to_ship_inclusive",
     """WITH shipped AS (
           SELECT id, channel, CAST(order_ts AS DATE) AS ordered, ship_date FROM orders WHERE ship_date IS NOT NULL)
         SELECT id, channel, ordered, ship_date, DATEDIFF(DAY, ordered, ship_date) AS calendar_days,
           DATEDIFF(DAY, ordered, ship_date) + 1 - 2 * DATEDIFF(WEEK, ordered, ship_date)
             - CASE WHEN DOW(ordered) = 1 THEN 1 ELSE 0 END - CASE WHEN DOW(ship_date) = 7 THEN 1 ELSE 0 END
             AS business_days
         FROM shipped WHERE id <= 40 ORDER BY id"""),
    ("sku_parts_and_product_slugs",
     """SELECT id, sku, LEFT(sku, 3) AS category_code, SUBSTRING(sku, 5, 3) AS subcategory_code,
           CAST(RIGHT(sku, 3) AS INTEGER) AS sequence_no,
           LOWER(REPLACE(TRIM(name), ' ', '-')) || '-' || CAST(id AS VARCHAR(5)) AS slug,
           CASE WHEN weight_kg IS NULL THEN 'unknown' WHEN weight_kg < 1 THEN 'light' WHEN weight_kg < 5 THEN 'medium'
                ELSE 'heavy' END AS weight_class,
           CAST(ROUND(100 * (unit_price - unit_cost) / unit_price, 1) AS NUMERIC(5,1)) AS margin_pct
         FROM products ORDER BY category_code, sequence_no"""),
    ("supplier_directory_cleaned",
     """SELECT s.id, UPPER(LEFT(TRIM(s.name), 1)) || SUBSTRING(TRIM(s.name), 2) AS supplier,
           COALESCE(NULLIF(TRIM(s.contact), ''), '(no contact)') AS contact_name,
           LOWER(TRIM(s.email)) AS clean_email,
           CASE WHEN s.email IS NULL THEN 'missing' WHEN LOCATE(s.email, '@') = 0 THEN 'invalid' ELSE 'ok' END
             AS email_check,
           DATEDIFF(YEAR, s.since, CAST('2025-12-31' AS DATE)) AS years_as_supplier,
           (SELECT COUNT(*) FROM supplier_products sp WHERE sp.supplier_id = s.id) AS products_supplied
         FROM suppliers s ORDER BY s.id"""),
]

CASES = [("customer_hygiene_reports", CUSTOMERS), ("service_reports", SERVICE)]
