"""Realistic report queries over the retail schema, part K: IQ's own idioms as its shops write them -- IF ... ENDIF
and IFNULL in labels, STRING, STR, STUFF, REPLICATE, PATINDEX, INSERTSTR and SUBSTR in text, CONVERT's date styles,
DATEFORMAT's pictures, DAYNAME, MONTHNAME and DATENAME, DATEFLOOR, DATECEILING and DATEROUND for buckets, YEARS,
MONTHS, WEEKS and DAYS for ages and tenures, ARGN, ASCII, CHAR and SIGN, VAR_POP, COVAR_POP and the REGR_ family
for dispersion and trend, and RANGE frames over a value."""

LABELS = [
    ("tiers_with_if_endif",
     """SELECT p.id, p.name,
           IF p.unit_price >= 1000 THEN 'premium' ELSE IF p.unit_price >= 100 THEN 'mid' ELSE 'value' ENDIF ENDIF AS tier,
           IF p.discontinued = 1 THEN 'retired' ELSE 'live' ENDIF AS state, IFNULL(p.subcategory, p.category) AS shelf
         FROM products p ORDER BY p.id"""),
    ("stock_actions_by_cover",
     """SELECT i.product_id, p.sku, i.on_hand, i.reorder_point,
           IF i.on_hand <= i.reorder_point THEN 'reorder' ELSE IF i.on_hand <= 2 * i.reorder_point THEN 'watch' ELSE 'ok' ENDIF ENDIF AS action,
           i.on_hand - i.reorder_point AS headroom, SIGN(i.on_hand - i.reorder_point) AS headroom_sign
         FROM inventory i JOIN products p ON p.id = i.product_id
         WHERE i.snapshot_date = (SELECT MAX(i2.snapshot_date) FROM inventory i2) ORDER BY i.product_id"""),
    ("tickets_by_hour_band",
     """SELECT IF HOUR(opened_ts) < 9 THEN 'before 9' ELSE IF HOUR(opened_ts) < 17 THEN 'office hours' ELSE 'evening' ENDIF ENDIF AS band,
           priority, COUNT(*) AS tickets, COUNT(closed_ts) AS closed,
           ROUND(AVG(IF closed_ts IS NULL THEN NULL ELSE DATEDIFF(MINUTE, opened_ts, closed_ts) * 1.0 ENDIF), 1) AS avg_minutes
         FROM support_tickets
         GROUP BY IF HOUR(opened_ts) < 9 THEN 'before 9' ELSE IF HOUR(opened_ts) < 17 THEN 'office hours' ELSE 'evening' ENDIF ENDIF, priority
         ORDER BY band, priority"""),
    ("seasons_by_argn",
     """SELECT ARGN(QUARTER(o.order_ts), 'winter', 'spring', 'summer', 'autumn') AS season, COUNT(*) AS orders,
           SUM(o.shipping_fee) AS fees, COUNT(DISTINCT o.customer_id) AS buyers
         FROM orders o GROUP BY ARGN(QUARTER(o.order_ts), 'winter', 'spring', 'summer', 'autumn') ORDER BY season"""),
    ("contact_fallbacks",
     """SELECT c.id, IFNULL(c.email, IFNULL(c.phone, 'no contact')) AS reach_by,
           IF c.email IS NULL AND c.phone IS NULL THEN 'none' ELSE IF c.email IS NULL THEN 'phone' ELSE 'email' ENDIF ENDIF AS channel,
           IF c.is_active = 1 THEN 'active' ELSE 'lapsed' ENDIF AS state
         FROM customers c ORDER BY c.id"""),
]

TEXT = [
    ("price_list_text",
     """SELECT id, STR(unit_price, 9, 2) AS price_text, STR(unit_cost, 6) AS cost_whole, LCASE(sku) AS sku_lower,
           UCASE(LEFT(name, 1)) || SUBSTR(name, 2) AS display_name, STUFF(sku, 1, 0, 'P-') AS catalog_code,
           REPLICATE('#', CAST(FLOOR(unit_price / 100) AS INTEGER)) AS price_bar
         FROM products ORDER BY id"""),
    ("sku_parts_by_patindex",
     """SELECT id, sku, PATINDEX('%[0-9]%', sku) AS first_digit_at, LEFT(sku, PATINDEX('%[0-9]%', sku) - 1) AS prefix,
           CAST(SUBSTR(sku, PATINDEX('%[0-9]%', sku)) AS INTEGER) AS serial, STUFF(sku, 5, 3, LCASE(SUBSTR(sku, 5, 3))) AS mixed
         FROM products ORDER BY id"""),
    ("masked_contacts",
     """SELECT id, IF email IS NULL THEN NULL ELSE STUFF(email, 2, LOCATE(email, '@') - 3, REPLICATE('*', LOCATE(email, '@') - 3)) ENDIF AS masked_email,
           IF phone IS NULL THEN NULL ELSE REPLICATE('x', CHAR_LENGTH(phone) - 4) || RIGHT(phone, 4) ENDIF AS masked_phone
         FROM customers ORDER BY id"""),
    ("ticket_durations_as_text",
     """SELECT id, priority,
           IF closed_ts IS NULL THEN 'open'
              ELSE STRING(DATEDIFF(MINUTE, opened_ts, closed_ts) / 60, 'h ', MOD(DATEDIFF(MINUTE, opened_ts, closed_ts), 60), 'm') ENDIF AS took,
           STRING('#', id, ' ', UCASE(LEFT(priority, 1)), ' ', LEFT(subject, 20)) AS label
         FROM support_tickets ORDER BY id"""),
    ("letter_numbers_and_codes",
     """SELECT id, last_name, ASCII(UPPER(LEFT(TRIM(last_name), 1))) - 64 AS letter_number,
           CHAR(ASCII(UPPER(LEFT(TRIM(first_name), 1)))) || CHAR(ASCII(UPPER(LEFT(last_name, 1)))) AS initials,
           INSERTSTR(3, CAST(region_id AS VARCHAR(3)) || '0000', '-') AS region_code
         FROM customers ORDER BY id"""),
    ("campaign_from_urls",
     """SELECT IF LOCATE(url, 'utm_campaign=') = 0 THEN '(none)'
              ELSE SUBSTR(url || '&', LOCATE(url, 'utm_campaign=') + 13,
                          LOCATE(url || '&', '&', LOCATE(url, 'utm_campaign=')) - LOCATE(url, 'utm_campaign=') - 13) ENDIF AS campaign,
           COUNT(*) AS hits, COUNT(DISTINCT customer_id) AS known_visitors, MIN(event_ts) AS first_seen
         FROM web_events
         GROUP BY IF LOCATE(url, 'utm_campaign=') = 0 THEN '(none)'
              ELSE SUBSTR(url || '&', LOCATE(url, 'utm_campaign=') + 13,
                          LOCATE(url || '&', '&', LOCATE(url, 'utm_campaign=')) - LOCATE(url, 'utm_campaign=') - 13) ENDIF
         ORDER BY hits DESC, campaign"""),
]

DATES = [
    ("dates_in_regional_styles",
     """SELECT id, CONVERT(VARCHAR(10), order_ts, 101) AS us_date, CONVERT(VARCHAR(10), order_ts, 103) AS uk_date,
           CONVERT(VARCHAR(8), order_ts, 112) AS compact, CONVERT(VARCHAR(19), order_ts, 120) AS iso_stamp,
           CONVERT(VARCHAR(8), order_ts, 108) AS clock
         FROM orders WHERE id <= 40 ORDER BY id"""),
    ("dates_in_report_pictures",
     """SELECT id, DATEFORMAT(order_ts, 'Mmm dd, yyyy') AS ordered, DATEFORMAT(order_ts, 'hh:nn') AS at_time,
           DATEFORMAT(ship_date, 'Dddd, Mmmm dd') AS shipped_on, MONTHNAME(ship_date) AS ship_month,
           DATEFORMAT(order_ts, 'yyyy') || '-Q' || CAST(QUARTER(order_ts) AS VARCHAR(1)) AS fiscal_quarter
         FROM orders WHERE id <= 40 ORDER BY id"""),
    ("orders_by_weekday_name",
     """SELECT DOW(order_ts) AS dow, DAYNAME(order_ts) AS weekday, DATENAME(WEEKDAY, order_ts) AS weekday_too, COUNT(*) AS orders,
           SUM(shipping_fee) AS fees, ROUND(AVG(shipping_fee), 2) AS avg_fee
         FROM orders GROUP BY DOW(order_ts), DAYNAME(order_ts), DATENAME(WEEKDAY, order_ts) ORDER BY dow"""),
    ("monthly_buckets_by_datefloor",
     """SELECT DATEFLOOR(MONTH, o.order_ts) AS month_start, DATEADD(DAY, -1, DATEADD(MONTH, 1, DATEFLOOR(MONTH, o.order_ts))) AS month_end,
           COUNT(DISTINCT o.id) AS orders, SUM(oi.quantity * oi.unit_price) AS gross
         FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE o.status <> 'cancelled'
         GROUP BY DATEFLOOR(MONTH, o.order_ts) ORDER BY month_start"""),
    ("hourly_load_by_dateceiling",
     """SELECT DATECEILING(HOUR, event_ts) AS hour_ending, device, COUNT(*) AS events, COUNT(DISTINCT customer_id) AS visitors
         FROM web_events WHERE event_ts >= '2025-12-01' GROUP BY DATECEILING(HOUR, event_ts), device ORDER BY hour_ending, device"""),
    ("tickets_rounded_to_quarter_hours",
     """SELECT id, opened_ts, DATEROUND(MINUTE, opened_ts) AS opened_minute, DATEFLOOR(MINUTE, opened_ts, 15) AS quarter_hour,
           DATEFLOOR(DAY, opened_ts) AS opened_day, DATEDIFF(DAY, DATEFLOOR(WEEK, opened_ts), opened_ts) AS day_of_week_index
         FROM support_tickets WHERE id <= 30 ORDER BY id"""),
    ("ages_and_tenures",
     """SELECT id, birth_date, YEARS(birth_date, CAST('2026-01-01' AS DATE)) AS age,
           MONTHS(signup_date, CAST('2026-01-01' AS DATE)) AS tenure_months, WEEKS(signup_date, CAST('2026-01-01' AS DATE)) AS tenure_weeks,
           DAYS(signup_date, CAST('2026-01-01' AS DATE)) AS tenure_days,
           IF YEARS(birth_date, CAST('2026-01-01' AS DATE)) >= 65 THEN 'senior' ELSE 'adult' ENDIF AS band
         FROM customers ORDER BY id"""),
    ("shipping_lag_with_date_and_datetime",
     """SELECT id, DATE(order_ts) AS order_day, DATETIME(ship_date) AS ship_midnight,
           DATEDIFF(HOUR, order_ts, DATETIME(ship_date)) AS hours_to_ship, YMD(YEAR(order_ts), MONTH(order_ts), 1) AS month_start,
           DAYS(DATE(order_ts), ship_date) AS days_to_ship
         FROM orders WHERE ship_date IS NOT NULL AND id <= 40 ORDER BY id"""),
]

STATS = [
    ("price_dispersion_by_category",
     """SELECT category, COUNT(*) AS products, ROUND(VAR_POP(unit_price), 3) AS var_pop, ROUND(VAR_SAMP(unit_price), 3) AS var_samp,
           ROUND(COVAR_POP(unit_price, unit_cost), 3) AS covar, ROUND(REGR_SLOPE(unit_price, unit_cost), 4) AS markup_slope,
           ROUND(REGR_INTERCEPT(unit_price, unit_cost), 3) AS intercept, ROUND(REGR_R2(unit_price, unit_cost), 4) AS r2
         FROM products GROUP BY category ORDER BY category"""),
    ("fee_trend_by_channel",
     """SELECT channel, REGR_COUNT(fee, lines) AS n, ROUND(REGR_SLOPE(fee, lines), 4) AS fee_per_line,
           ROUND(REGR_INTERCEPT(fee, lines), 3) AS base_fee, ROUND(REGR_AVGX(fee, lines), 3) AS avg_lines,
           ROUND(REGR_AVGY(fee, lines), 3) AS avg_fee, ROUND(CORR(fee, lines), 4) AS corr
         FROM (SELECT o.id, o.channel, o.shipping_fee AS fee, COUNT(oi.id) AS lines
               FROM orders o JOIN order_items oi ON oi.order_id = o.id GROUP BY o.id, o.channel, o.shipping_fee) x
         GROUP BY channel ORDER BY channel"""),
    ("payments_near_in_value",
     """SELECT id, method, amount,
           COUNT(*) OVER (PARTITION BY method ORDER BY amount RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING) AS near_count,
           SUM(amount) OVER (PARTITION BY method ORDER BY amount RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING) AS near_total,
           MAX(amount) OVER (PARTITION BY method ORDER BY amount RANGE BETWEEN UNBOUNDED PRECEDING AND 50 PRECEDING) AS below_by_50
         FROM payments WHERE id <= 60 ORDER BY id"""),
    ("salary_spread_by_region",
     """SELECT r.name AS region, COUNT(*) AS staff, ROUND(STDDEV_POP(e.salary), 2) AS sd_pop, ROUND(VAR_POP(e.salary), 1) AS var_pop,
           ROUND(MAX(e.salary) / NULLIF(MIN(e.salary), 0), 3) AS max_to_min,
           ROUND(REGR_SLOPE(e.salary, DATEDIFF(DAY, e.hire_date, CAST('2026-01-01' AS DATE))), 4) AS pay_per_day_served
         FROM employees e JOIN regions r ON r.id = e.region_id GROUP BY r.name ORDER BY r.name"""),
]

# DATEFLOOR, DATECEILING and DATEROUND to a multiple of the part, one part per query (an error in one stays its own):
# whether the multiple counts from the containing unit's start or from an epoch, and a multiple that divides no unit
BUCKETS = [
    ("event_buckets_of_minutes",
     """SELECT id, event_ts, DATEFLOOR(MINUTE, event_ts, 15) AS m15, DATEFLOOR(MINUTE, event_ts, 7) AS m7,
           DATECEILING(MINUTE, event_ts, 15) AS up15, DATEROUND(MINUTE, event_ts, 15) AS near15, DATEROUND(MINUTE, event_ts, 7) AS near7
         FROM web_events WHERE id <= 80 ORDER BY id"""),
    ("event_buckets_of_hours",
     """SELECT id, event_ts, DATEFLOOR(HOUR, event_ts, 6) AS h6, DATEFLOOR(HOUR, event_ts, 5) AS h5,
           DATECEILING(HOUR, event_ts, 6) AS up6, DATEROUND(HOUR, event_ts, 6) AS near6
         FROM web_events WHERE id <= 80 ORDER BY id"""),
    ("event_buckets_of_days",
     """SELECT id, event_ts, DATEFLOOR(DAY, event_ts, 7) AS d7, DATEFLOOR(DAY, event_ts, 10) AS d10, DATECEILING(DAY, event_ts, 7) AS up7
         FROM web_events WHERE id <= 80 ORDER BY id"""),
    ("event_buckets_of_months",
     """SELECT id, event_ts, DATEFLOOR(MONTH, event_ts, 3) AS mo3, DATEFLOOR(MONTH, event_ts, 5) AS mo5,
           DATEFLOOR(MONTH, event_ts, 6) AS half, DATECEILING(MONTH, event_ts, 3) AS up3
         FROM web_events WHERE id <= 80 ORDER BY id"""),
    ("order_buckets_of_years",
     """SELECT id, order_ts, DATEFLOOR(YEAR, order_ts, 2) AS y2, DATEFLOOR(YEAR, order_ts, 10) AS y10,
           DATEFLOOR(QUARTER, order_ts, 2) AS q2, DATECEILING(YEAR, order_ts, 2) AS up2
         FROM orders WHERE id <= 60 ORDER BY id"""),
    ("ticket_load_by_quarter_hour",
     """SELECT DATEFLOOR(MINUTE, opened_ts, 15) AS slot, COUNT(*) AS opened, MIN(priority) AS a_priority
         FROM support_tickets GROUP BY DATEFLOOR(MINUTE, opened_ts, 15) ORDER BY slot"""),
]

CASES = [("iq_labels", LABELS), ("iq_text", TEXT), ("iq_dates", DATES), ("iq_stats", STATS), ("iq_buckets", BUCKETS)]
