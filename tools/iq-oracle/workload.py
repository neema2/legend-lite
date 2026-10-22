"""The #IQ{...}# island's WORKLOAD suite's data (IQ island, landing 6, 2026-10-22): a trading book -- instruments,
accounts, traders, trades, daily prices and FX rates -- written to core/src/test/resources/iq-workload/schema.yaml in the
parity schema's YAML (SqlParity.Yaml's subset; snap.py's reader), so snap.sh can load it into SQL Anywhere and the live
IQ (as ll_<table>) and the Java lanes into every backend.

The data is made by a seeded generator and is byte-stable: running this twice writes the same file. Changing the
generator changes schema.yaml, which makes every workload answer STALE: re-snap after.

    python3 -I tools/iq-oracle/workload.py [--check]

It carries the values the island's semantics turn on, at a size where a report has something to aggregate:
CHAR(n) codes and VARCHAR names that differ only in case or trailing blanks, Latin-1 accents (the parity schema's
charset), NULLs in every nullable column, NUMERIC money at several scales, DOUBLE FX rates, BIGINT trade ids past 2^31,
microsecond timestamps, dates across month, quarter and year ends and a leap day, and BIT flags.
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "..", "..", "core", "src", "test", "resources", "iq-workload", "schema.yaml")


class Rand:
    """A 64-bit LCG (Knuth's MMIX constants): the same numbers on every Python, no library."""

    def __init__(self, seed):
        self.s = seed & 0xFFFFFFFFFFFFFFFF

    def next(self):
        self.s = (self.s * 6364136223846793005 + 1442695040888963407) & 0xFFFFFFFFFFFFFFFF
        return self.s >> 11

    def below(self, n):
        return self.next() % n

    def pick(self, xs):
        return xs[self.below(len(xs))]

    def chance(self, per_thousand):
        return self.below(1000) < per_thousand


def days_in(y, m):
    return [31, 29 if (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1]


def add_days(d, n):
    y, m, dd = d
    dd += n
    while dd > days_in(y, m):
        dd -= days_in(y, m)
        m += 1
        if m > 12:
            m, y = 1, y + 1
    while dd < 1:
        m -= 1
        if m < 1:
            m, y = 12, y - 1
        dd += days_in(y, m)
    return (y, m, dd)


def iso(d):
    return "%04d-%02d-%02d" % d


def weekday(d):
    # 0 = Monday (Zeller-free: days since 2023-01-02, a Monday)
    y, m, dd = d
    n = 0
    c = (2023, 1, 2)
    if d >= c:
        while c != d:
            c = add_days(c, 1)
            n += 1
    else:
        while c != d:
            c = add_days(c, -1)
            n -= 1
    return n % 7


def money(cents, scale):
    """A NUMERIC's text at its scale from an integer of its smallest unit."""
    sign = "-" if cents < 0 else ""
    cents = abs(cents)
    if scale == 0:
        return sign + str(cents)
    whole, frac = divmod(cents, 10 ** scale)
    return "%s%d.%0*d" % (sign, whole, scale, frac)


def q(v):
    """A YAML cell: an integer plain, NULL null, anything else quoted text (single quotes doubled)."""
    if v is None:
        return "null"
    if isinstance(v, int) and not isinstance(v, bool):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def build():
    r = Rand(20261022)
    tables = []

    # ---- currencies and FX: USD is 1.0 exactly; the others vary day by day (DOUBLE, exact in binary where it can be)
    ccys = ["USD", "EUR", "GBP", "JPY", "CHF", "CAD", "HKD", "SEK"]
    base_rate = {"USD": 1.0, "EUR": 1.0859375, "GBP": 1.265625, "JPY": 0.0067138671875, "CHF": 1.1279296875,
                 "CAD": 0.736328125, "HKD": 0.1279296875, "SEK": 0.09521484375}

    # ---- the business days: 2023-12-26 .. 2024-03-08 (a year end, a quarter end, the 2024-02-29 leap day)
    days = []
    d = (2023, 12, 26)
    while d <= (2024, 3, 8):
        if weekday(d) < 5 and d not in [(2023, 12, 25), (2024, 1, 1)]:
            days.append(d)
        d = add_days(d, 1)

    fx_rows = []
    for c in ccys:
        rate = base_rate[c]
        for i, day in enumerate(days):
            if c == "USD":
                fx_rows.append([c, iso(day), "1.0"])
                continue
            # a step of a multiple of 2^-16 keeps every rate exact in binary
            step = (r.below(41) - 20) * rate / 4096.0
            rate = max(rate + step, rate / 2)
            rate = round(rate * 65536) / 65536.0
            if c == "SEK" and i == 7:
                fx_rows.append([c, iso(day), None])   # a missing fixing
            else:
                fx_rows.append([c, iso(day), repr(rate)])
    tables.append(("fx_rates", [("ccy", "char(3)"), ("rate_date", "date"), ("usd_rate", "double")], fx_rows))

    # ---- instruments: 48, across asset classes and sectors; CHAR(8) symbols, names with case, blanks and accents
    sectors = ["Tech", "Energy", "Financials", "Health", "Utilities", "Materials", None]
    classes = ["EQUITY", "EQUITY", "EQUITY", "ETF", "BOND", "FUTURE"]
    stems = ["Acme", "Borealis", "Cygnus", "Delta", "Éclair", "Fjord", "Gamma", "Helix", "Iris", "Jupiter",
             "Kestrel", "Lumen", "Müller", "Nova", "Orion", "Pyxis"]
    suffixes = [" Corp", " Holdings", " plc", " AG", " Trust", " Fund"]
    instruments = []
    for i in range(48):
        iid = 1001 + i
        stem = stems[i % len(stems)]
        sym = (stem[:3].upper().replace("É", "E").replace("Ü", "U") + str(i % 10))
        name = stem + suffixes[(i // len(stems)) % len(suffixes)]
        if i == 5:
            name = name.upper()          # differs from another only in case
        if i == 9:
            name = name + "  "           # trailing blanks
        ccy = ccys[i % 6] if i % 4 else "USD"
        lot = [1, 10, 100, 1000][i % 4]
        listed = add_days((2001, 1, 1), r.below(8000))
        active = 0 if i in (17, 33) else 1
        instruments.append([iid, sym, name, classes[i % len(classes)], sectors[i % len(sectors)], ccy, lot,
                            iso(listed), active])
    tables.append(("instruments", [("instrument_id", "integer"), ("symbol", "char(8)"), ("name", "varchar(40)"),
                                   ("asset_class", "varchar(10)"), ("sector", "varchar(16)"), ("ccy", "char(3)"),
                                   ("lot_size", "integer"), ("listed_date", "date"), ("active", "bit")],
                   instruments))

    # ---- traders and desks
    desks = ["Rates", "Credit", "EQ Cash", "EQ Derivs", "Macro"]
    first = ["Ann", "Bob", "Chloé", "Dmitri", "Eve", "Farah", "Gus", "Hana", "Ivo", "Jun", "Kai", "Lena", "Mo",
             "Nils", "Olga", "Pia", "Quinn", "Raj", "Sven", "Tess", "Uma", "Vic", "Wes", "Xia", "Yan"]
    traders = []
    for i, n in enumerate(first):
        login = n.lower().replace("é", "e")
        if i == 3:
            login = login + " "          # a login with a trailing blank
        hired = add_days((2010, 3, 1), r.below(4500))
        boss = None if i < 3 else 1 + (i % 3)
        traders.append([i + 1, login, n + " " + chr(65 + i) + ".", desks[i % len(desks)], iso(hired), boss])
    tables.append(("traders", [("trader_id", "integer"), ("login", "varchar(12)"), ("full_name", "varchar(30)"),
                               ("desk", "varchar(12)"), ("hired", "date"), ("manager_id", "integer")], traders))

    # ---- accounts: limits as NUMERIC(15,2), tiers as SMALLINT, some closed
    regions = ["AMER", "EMEA", "APAC"]
    accounts = []
    for i in range(36):
        aid = 50 + i * 3
        opened = add_days((2015, 1, 1), r.below(3000))
        limit = None if i % 11 == 0 else money((r.below(5000) + 1) * 100000, 2)
        closed = iso(add_days(opened, 1500 + r.below(600))) if i % 9 == 4 else None
        accounts.append([aid, "ACCT-%03d" % aid, "Client " + chr(65 + i % 26) + ("" if i < 26 else "2"),
                         regions[i % 3], 1 + i % 4, limit, iso(opened), closed])
    tables.append(("accounts", [("account_id", "integer"), ("code", "char(8)"), ("client", "varchar(30)"),
                                ("region", "char(4)"), ("tier", "smallint"), ("credit_limit", "numeric(15,2)"),
                                ("opened", "date"), ("closed", "date")], accounts))

    # ---- daily prices: NUMERIC(14,4), volume BIGINT; a few gaps
    price_rows = []
    last = {}
    for inst in instruments:
        iid = inst[0]
        px = (r.below(90000) + 1000) * 100   # in 1/10000ths: 10.0000 .. 1000.0000
        for i, day in enumerate(days):
            if (iid + i) % 37 == 0:
                continue                      # no price that day
            move = (r.below(401) - 200) * px // 10000
            px = max(px + move, 500)
            hi = px + r.below(px // 50 + 1)
            lo = max(px - r.below(px // 50 + 1), 1)
            op = lo + r.below(hi - lo + 1)
            vol = r.below(5000000) * (1 if iid % 5 else 1000)
            price_rows.append([iid, iso(day), money(op, 4), money(hi, 4), money(lo, 4), money(px, 4), vol])
            last[(iid, day)] = px
    tables.append(("prices", [("instrument_id", "integer"), ("price_date", "date"), ("open_px", "numeric(14,4)"),
                              ("high_px", "numeric(14,4)"), ("low_px", "numeric(14,4)"),
                              ("close_px", "numeric(14,4)"), ("volume", "bigint")], price_rows))

    # ---- trades: 1,600, BIGINT ids past 2^31, microsecond timestamps, statuses with case, NULL commissions
    statuses = ["FILLED", "FILLED", "FILLED", "PARTIAL", "CANCELLED", "filled"]
    trades = []
    tid = 3000000000
    for k in range(1600):
        tid += 1 + r.below(7)
        day = r.pick(days)
        inst = instruments[r.below(len(instruments))]
        iid = inst[0]
        acct = accounts[r.below(len(accounts))][0]
        trader = 1 + r.below(len(first))
        side = "B" if r.chance(530) else "S"
        qty = (1 + r.below(200)) * inst[6] // (10 if inst[6] >= 100 else 1)
        base = last.get((iid, day))
        if base is None:
            base = (r.below(90000) + 1000) * 100
        px = base + (r.below(201) - 100) * base // 100000
        price = money(px * 100, 6)          # NUMERIC(18,6)
        comm = None if r.chance(80) else money(r.below(25000), 4)
        h, mi, s, us = 8 + r.below(9), r.below(60), r.below(60), r.below(1000000)
        if k % 97 == 0:
            us = 0
        if k % 211 == 0:
            h, mi, s, us = 23, 59, 59, 999999
        ts = "%s %02d:%02d:%02d.%06d" % (iso(day), h, mi, s, us)
        status = r.pick(statuses)
        note = None if r.chance(700) else r.pick(["late booking", "Late Booking", "amended  ", "give-up", "DMA",
                                                   "algo:VWAP", "algo:TWAP", "manual; check"])
        trades.append([tid, acct, iid, trader, iso(day), ts, side, qty, price, comm, status, note])
    tables.append(("trades", [("trade_id", "bigint"), ("account_id", "integer"), ("instrument_id", "integer"),
                              ("trader_id", "integer"), ("trade_date", "date"), ("trade_ts", "timestamp"),
                              ("side", "char(1)"), ("quantity", "integer"), ("price", "numeric(18,6)"),
                              ("commission", "numeric(12,4)"), ("status", "varchar(10)"),
                              ("note", "varchar(20)")], trades))
    return tables


def render(tables):
    out = ["# The #IQ{...}# island's WORKLOAD suite's schema and data (IQ island, landing 6, 2026-10-22): a trading book",
           "# for complex reports. GENERATED by tools/iq-oracle/workload.py -- edit the generator, not this file; every",
           "# answer in iq-answers.jsonl is STALE after a change (its sqlSha covers this file). The YAML is the parity",
           "# schema's subset (SqlParity.Yaml, snap.py's reader); text stays inside ISO-8859-1.",
           "tables:"]
    for name, cols, rows in tables:
        out.append("  - name: %s" % name)
        out.append("    columns:")
        for c, t in cols:
            out.append("      - {name: %s, type: %s}" % (c, ("'%s'" % t) if "," in t else t))
        out.append("    rows:")
        for row in rows:
            out.append("      - [" + ", ".join(q(v) for v in row) + "]")
    return "\n".join(out) + "\n"


def main(argv):
    text = render(build())
    if "--check" in argv:
        have = open(OUT, encoding="utf-8").read() if os.path.exists(OUT) else ""
        if have != text:
            print("schema.yaml is not the generator's: run tools/iq-oracle/workload.py")
            return 1
        print("schema.yaml is the generator's")
        return 0
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)
    print("wrote %s (%d bytes)" % (os.path.normpath(OUT), len(text.encode("utf-8"))))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
