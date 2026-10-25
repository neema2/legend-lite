"""The IQ scale program's realistic report schema (2026-10-25): a small retail database -- regions, employees,
customers, products, orders, their lines and payments -- with the data real tables hold: NULLs where a value is not
known yet (a phone, a ship date, a birth date), mixed-case e-mails, names with blanks around them, discontinued
products, cancelled and pending orders, partial payments. Deterministic (a seeded generator). Writes schema.yaml."""
import datetime
import random
import sys

OUT = sys.argv[1]
rnd = random.Random(20261025)


def q(v):
    """A cell as schema.yaml writes it: integers and bits plain, NULL as null, everything else quoted text."""
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, int):
        return str(v)
    s = str(v)
    assert "'" not in s and "\n" not in s, s
    return "'" + s + "'"


tables = []


def table(name, columns, rows):
    tables.append((name, columns, rows))


regions = [(1, "North", "USA"), (2, "South", "USA"), (3, "West", "USA"), (4, "Ontario", "Canada"),
           (5, "Bavaria", "Germany")]
table("regions", [("id", "integer"), ("name", "varchar(20)"), ("country", "varchar(20)")], regions)

employees = [
    (1, "Grace", "Hopper", "VP Sales", None, "2015-03-01", "185000.00", 1),
    (2, "Alan", "Turing", "Sales Manager", 1, "2016-07-15", "132000.00", 1),
    (3, "Ada", "Lovelace", "Sales Manager", 1, "2017-01-09", "128500.00", 3),
    (4, "Linus", "Pauling", "Account Executive", 2, "2019-05-20", "86000.00", 1),
    (5, "Marie", "Curie", "Account Executive", 2, "2020-11-02", "91000.00", 2),
    (6, "Niels", "Bohr", "Account Executive", 3, "2021-02-14", "84500.00", 3),
    (7, "Emmy", "Noether", "Account Executive", 3, "2022-08-29", "79000.00", 5),
    (8, "Carl", "Gauss", "Sales Associate", 4, "2023-04-03", "58000.00", 4),
    (9, "Rosalind", "Franklin", "Sales Associate", 6, "2024-01-22", "56500.00", 3),
    (10, "Kurt", "Godel", "Sales Associate", None, "2024-09-30", "54000.00", None),
]
table("employees", [("id", "integer"), ("first_name", "varchar(20)"), ("last_name", "varchar(20)"),
                    ("title", "varchar(30)"), ("manager_id", "integer"), ("hire_date", "date"),
                    ("salary", "numeric(9,2)"), ("region_id", "integer")], employees)

firsts = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth",
          "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen",
          "Chen", "Priya", "Jose", "Fatima", "Lukas", "Sofia", "Noah", "Amelie", "Oliver", "Zoe"]
lasts = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
         "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
         "Wang", "Patel", "Silva", "Khan", "Muller", "Rossi", "Lee", "Dubois", "Schmidt", "Nguyen"]
domains = ["gmail.com", "yahoo.com", "outlook.com", "acme-corp.com", "example.org", "GMAIL.COM"]
cities = {1: ["Chicago", "Detroit", "Minneapolis"], 2: ["Houston", "Atlanta", "Miami"], 3: ["Seattle", "Denver",
          "San Diego"], 4: ["Toronto", "Ottawa"], 5: ["Munich", "Nuremberg"]}
segments = ["retail", "smb", "enterprise"]
customers = []
for i in range(1, 31):
    first, last = firsts[i - 1], lasts[(i * 7) % len(lasts)]
    region = 1 + (i * 3) % 5
    email = None if i % 11 == 0 else (first.lower() + "." + last.lower() + "@" + domains[i % len(domains)])
    if i % 9 == 0 and email:
        email = email.upper()
    phone = None if i % 4 == 0 else "(%03d) %03d-%04d" % (200 + i * 7 % 700, 100 + i * 13 % 800, 1000 + i * 37)
    if i % 6 == 0 and phone:
        phone = phone.replace("(", "").replace(")", "").replace(" ", "-")
    signup = datetime.date(2022, 1, 1) + datetime.timedelta(days=rnd.randrange(0, 900))
    birth = None if i % 7 == 0 else datetime.date(1950 + rnd.randrange(0, 50), rnd.randrange(1, 13),
                                                   rnd.randrange(1, 29))
    name_first = (" " + first + " ") if i % 10 == 3 else first
    customers.append((i, name_first, last, email, phone, rnd.choice(cities[region]), region, signup.isoformat(),
                      birth.isoformat() if birth else None, segments[i % 3],
                      "%d.00" % (rnd.randrange(5, 200) * 500), 0 if i % 13 == 0 else 1))
table("customers", [("id", "integer"), ("first_name", "varchar(20)"), ("last_name", "varchar(20)"),
                    ("email", "varchar(60)"), ("phone", "varchar(20)"), ("city", "varchar(20)"),
                    ("region_id", "integer"), ("signup_date", "date"), ("birth_date", "date"),
                    ("segment", "varchar(10)"), ("credit_limit", "numeric(10,2)"), ("is_active", "bit")], customers)

catalog = [
    ("Electronics", "Laptops", ["Ultrabook 13", "Workstation 15", "Chromebook"]),
    ("Electronics", "Phones", ["Phone X", "Phone Lite", "Phone Pro Max"]),
    ("Electronics", "Audio", ["Earbuds", "Studio Headphones"]),
    ("Home", "Kitchen", ["Espresso Machine", "Chef Knife", "Blender"]),
    ("Home", "Furniture", ["Standing Desk", "Office Chair", "Bookshelf"]),
    ("Outdoor", "Camping", ["Tent 2P", "Sleeping Bag", "Camp Stove"]),
    ("Outdoor", "Cycling", ["Road Helmet", "Bike Lights", "Water Bottle"]),
]
products = []
pid = 0
for cat, sub, names in catalog:
    for nm in names:
        pid += 1
        price = rnd.choice([9.99, 14.5, 24.99, 49.0, 79.95, 129.0, 249.99, 499.0, 899.0, 1299.0, 2199.5])
        cost = round(price * rnd.choice([0.45, 0.55, 0.6, 0.72]), 2)
        sku = "%s-%s-%03d" % (cat[:3].upper(), sub[:3].upper(), pid)
        launched = datetime.date(2019, 1, 1) + datetime.timedelta(days=rnd.randrange(0, 1800))
        products.append((pid, sku, nm, cat, sub, "%.2f" % price, "%.2f" % cost,
                         None if pid % 8 == 0 else "%.3f" % (rnd.randrange(50, 9000) / 1000.0), launched.isoformat(),
                         1 if pid % 9 == 0 else 0))
table("products", [("id", "integer"), ("sku", "varchar(12)"), ("name", "varchar(30)"), ("category", "varchar(15)"),
                   ("subcategory", "varchar(15)"), ("unit_price", "numeric(8,2)"), ("unit_cost", "numeric(8,2)"),
                   ("weight_kg", "numeric(6,3)"), ("launched", "date"), ("discontinued", "bit")], products)

statuses = ["shipped"] * 7 + ["pending", "cancelled", "returned"]
channels = ["web", "web", "store", "phone"]
orders, items, payments = [], [], []
iid = 0
payid = 0
for oid in range(1, 121):
    cust = 1 + (oid * 17) % 30
    if cust in (5, 22):
        cust = 1 + (cust + 3) % 30  # two customers never order
    ts = datetime.datetime(2023, 1, 1, 8, 0) + datetime.timedelta(minutes=rnd.randrange(0, 3 * 365 * 24 * 60))
    status = statuses[rnd.randrange(len(statuses))]
    ship = None if status in ("pending", "cancelled") else (ts.date() + datetime.timedelta(days=rnd.randrange(0, 9)))
    discount = rnd.choice(["0.00", "0.00", "0.05", "0.10", "0.15", "0.25"])
    rep = None if oid % 12 == 0 else 1 + (oid * 5) % 10
    orders.append((oid, cust, ts.strftime("%Y-%m-%d %H:%M:%S"), ship.isoformat() if ship else None, status,
                   channels[oid % 4], discount, "%.2f" % rnd.choice([0, 0, 4.99, 9.99, 19.5]), rep))
    total = 0.0
    for _ in range(1 + rnd.randrange(0, 4)):
        iid += 1
        p = products[rnd.randrange(len(products))]
        qty = rnd.choice([1, 1, 1, 2, 2, 3, 5, 10])
        price = float(p[5]) if rnd.random() < 0.8 else round(float(p[5]) * 0.9, 2)
        line_disc = rnd.choice(["0.00", "0.00", "0.00", "5.00", "12.50"])
        items.append((iid, oid, p[0], qty, "%.2f" % price, line_disc))
        total += qty * price - float(line_disc)
    if status != "cancelled":
        paid = round(total * (1 - float(discount)), 2)
        parts = [paid] if oid % 7 else [round(paid / 2, 2), round(paid - round(paid / 2, 2), 2)]
        if status == "pending" and oid % 3 == 0:
            parts = []
        for k, amount in enumerate(parts):
            payid += 1
            pts = ts + datetime.timedelta(hours=rnd.randrange(0, 72) + 24 * k * 10)
            payments.append((payid, oid, pts.strftime("%Y-%m-%d %H:%M:%S"), "%.2f" % amount,
                             rnd.choice(["card", "card", "paypal", "invoice"])))
table("orders", [("id", "integer"), ("customer_id", "integer"), ("order_ts", "timestamp"), ("ship_date", "date"),
                 ("status", "varchar(10)"), ("channel", "varchar(10)"), ("discount_pct", "numeric(4,2)"),
                 ("shipping_fee", "numeric(6,2)"), ("sales_rep_id", "integer")], orders)
table("order_items", [("id", "integer"), ("order_id", "integer"), ("product_id", "integer"),
                      ("quantity", "smallint"), ("unit_price", "numeric(8,2)"), ("line_discount", "numeric(6,2)")],
      items)
table("payments", [("id", "integer"), ("order_id", "integer"), ("paid_ts", "timestamp"), ("amount", "numeric(10,2)"),
                   ("method", "varchar(10)")], payments)

# The tables of part E (2026-10-26), drawn from a generator of their own so the tables above keep their rows: monthly
# exchange rates (one month missing, for an as-of join), promotions over date ranges (two overlapping), a web shop's
# events (URLs with UTM parameters, sessions), support tickets (some still open) and monthly inventory snapshots.
rnd2 = random.Random(20261026)

rates = []
rid = 0
for cur, start in (("EUR", 1.08), ("CAD", 0.74), ("GBP", 1.26)):
    rate = start
    for m in range(36):
        rate = round(rate + rnd2.choice([-0.012, -0.006, -0.003, 0.0, 0.004, 0.007, 0.011]), 6)
        if cur == "CAD" and m == 18:
            continue  # July 2024 was never loaded
        rid += 1
        rates.append((rid, datetime.date(2023 + m // 12, 1 + m % 12, 1).isoformat(), cur, "%.6f" % rate))
table("exchange_rates", [("id", "integer"), ("rate_date", "date"), ("currency", "varchar(3)"),
                         ("usd_rate", "numeric(10,6)")], rates)

promotions = [(1, "NEWYEAR23", "2023-01-01", "2023-01-15", "0.10", None),
              (2, "SPRINGHOME", "2023-04-01", "2023-04-30", "0.15", "Home"),
              (3, "BACK2SCHOOL", "2023-08-15", "2023-09-10", "0.10", "Electronics"),
              (4, "BLACKFRI23", "2023-11-24", "2023-11-27", "0.25", None),
              (5, "SUMMER24", "2024-06-01", "2024-08-31", "0.05", "Outdoor"),
              (6, "CYBER24", "2024-11-29", "2024-12-02", "0.20", "Electronics"),
              (7, "HOLIDAY24", "2024-12-01", "2024-12-24", "0.10", None),
              (8, "SPRING25", "2025-03-15", "2025-04-15", "0.12", "Home"),
              (9, "FLASH25", "2025-07-04", "2025-07-04", "0.30", None)]
table("promotions", [("id", "integer"), ("code", "varchar(12)"), ("starts", "date"), ("ends", "date"),
                     ("pct_off", "numeric(4,2)"), ("category", "varchar(15)")], promotions)

host = "https://shop.example.com"
landings = ["?utm_source=google&utm_medium=cpc&utm_campaign=fall_sale", "?utm_source=newsletter&utm_campaign=nov",
            "?ref=partner42", "", "", "?utm_medium=social&utm_source=facebook"]
referrers = {"?utm_source=google&utm_medium=cpc&utm_campaign=fall_sale": "https://www.google.com/",
             "?utm_source=newsletter&utm_campaign=nov": "https://mail.example.org/",
             "?ref=partner42": "https://partner42.example.net/deals", "": None,
             "?utm_medium=social&utm_source=facebook": "https://www.facebook.com/"}
terms = ["desk", "phone+case", "tent", "espresso", "headphones"]
events = []
eid = 0
for sess in range(70):
    cust = None if sess % 6 == 0 else 1 + rnd2.randrange(30)
    ts = datetime.datetime(2025, 9, 1, 0, 0) + datetime.timedelta(minutes=rnd2.randrange(0, 122 * 24 * 60))
    device = rnd2.choice(["mobile", "mobile", "desktop", "tablet"])
    land = landings[sess % len(landings)]
    pid = 1 + rnd2.randrange(20)
    path = rnd2.choice(["/", "/products/%d" % pid, "/category/" + products[pid - 1][3].lower(),
                        "/search?q=" + rnd2.choice(terms)])
    steps = [path + (land if "?" not in path else land.replace("?", "&"))]
    depth = rnd2.randrange(0, 6)
    funnel = ["/products/%d" % pid, "/cart", "/checkout?step=1", "/checkout?step=2", "/order/confirmation"]
    steps += funnel[:depth]
    for k, s in enumerate(steps):
        eid += 1
        if k:
            ts += datetime.timedelta(minutes=rnd2.randrange(1, 16), seconds=rnd2.randrange(0, 60))
        url = (host.upper().replace("SHOP.EXAMPLE.COM", "Shop.Example.com") if eid % 37 == 0 else host) + s
        events.append((eid, cust, ts.strftime("%Y-%m-%d %H:%M:%S"), url, referrers[land] if k == 0 else None, device))
table("web_events", [("id", "integer"), ("customer_id", "integer"), ("event_ts", "timestamp"),
                     ("url", "varchar(160)"), ("referrer", "varchar(80)"), ("device", "varchar(10)")], events)

subjects = ["Refund request for order", "Where is my package?", "Damaged item received", "Change shipping address",
            "Invoice copy needed", "Product question: battery life", "Cancel my order please", "  Login problem  ",
            "REFUND NOT RECEIVED", "Wrong size delivered - exchange", "Missing parts in box",
            "Question about warranty coverage for a product bought last year"]
hours = {"urgent": (1, 12), "high": (2, 30), "normal": (4, 80), "low": (8, 200)}
orders_of = {}
for o in orders:
    orders_of.setdefault(o[1], []).append(o[0])
tickets = []
for tid in range(1, 61):
    cust = 1 + rnd2.randrange(30)
    while cust not in orders_of:
        cust = 1 + rnd2.randrange(30)
    oid = rnd2.choice(orders_of[cust]) if rnd2.random() < 0.7 else None
    opened = datetime.datetime(2024, 6, 1, 0, 0) + datetime.timedelta(minutes=rnd2.randrange(0, 580 * 24 * 60))
    priority = rnd2.choice(["low", "normal", "normal", "high", "urgent"])
    lo, hi = hours[priority]
    closed = None if tid % 8 == 0 else opened + datetime.timedelta(minutes=rnd2.randrange(lo * 60, hi * 60))
    agent = None if tid % 10 == 0 else rnd2.choice([4, 5, 6, 7, 8, 9, 10])
    tickets.append((tid, cust, oid, opened.strftime("%Y-%m-%d %H:%M:%S"),
                    closed.strftime("%Y-%m-%d %H:%M:%S") if closed else None, priority,
                    rnd2.choice(["email", "phone", "chat"]), subjects[rnd2.randrange(len(subjects))], agent))
table("support_tickets", [("id", "integer"), ("customer_id", "integer"), ("order_id", "integer"),
                          ("opened_ts", "timestamp"), ("closed_ts", "timestamp"), ("priority", "varchar(8)"),
                          ("channel", "varchar(10)"), ("subject", "varchar(80)"), ("agent_id", "integer")], tickets)

inventory = []
sid = 0
for p in products:
    on_hand = rnd2.randrange(20, 200)
    reorder = rnd2.choice([10, 15, 20, 30, 40])
    for m in range(12):
        sid += 1
        inventory.append((sid, p[0], datetime.date(2025, 1 + m, 1).isoformat(), on_hand, reorder))
        sold = rnd2.randrange(0, 60)
        on_hand = max(0, on_hand - sold)
        if on_hand < reorder and rnd2.random() < 0.6:
            on_hand += rnd2.choice([50, 100, 150])
table("inventory", [("id", "integer"), ("product_id", "integer"), ("snapshot_date", "date"), ("on_hand", "integer"),
                    ("reorder_point", "smallint")], inventory)

with open(OUT, "w") as f:
    f.write("# The IQ scale program's realistic report schema (2026-10-25): a small retail database -- regions,\n"
            "# employees, customers, products, orders, their lines and payments -- with the data real tables hold\n"
            "# (NULLs where a value is not known yet, mixed-case e-mails, padded names, cancelled and pending orders,\n"
            "# partial payments) -- and, from part E (2026-10-26), monthly exchange rates, promotions over date ranges, a\n"
            "# web shop's events, support tickets and inventory snapshots. Generated, deterministically, by the program's\n"
            "# gen_schema.py; tools/iq-oracle's README.md has the format.\n"
            "tables:\n")
    for name, cols, rows in tables:
        f.write("  - name: %s\n    columns:\n" % name)
        for c, t in cols:
            t = "'%s'" % t if "," in t else t
            f.write("      - {name: %s, type: %s}\n" % (c, t))
        f.write("    rows:\n")
        for r in rows:
            assert len(r) == len(cols), (name, r)
            f.write("      - [%s]\n" % ", ".join(q(v) for v in r))
print({n: len(r) for n, c, r in tables})
