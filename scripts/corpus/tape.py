"""
The market's day: a hand-written tape and its quotes, for the trader features (features.py).

The expansion ring (expand.py) seeds MARKET_QUOTE and MARKET_TRADE as it seeds every table one hop from the core: five
rows each, a NULL key, a dangling key, values that are functions of the row's index. That is the right data for what the
ring tests -- navigation, outer joins, NULLs -- and the wrong data for a market: every quote's bid equals its ask, the
prints are a month apart, nothing ticks. A VWAP, a candle, a moving average or a trade's prevailing quote over it is
arithmetic on noise.

So the market's own day is written here by hand, as the core seed is, and APPENDED to the ring's rows after the
expansion (flat.all_tables) rather than seeding the tables before it -- a table seeded before the expansion leaves the
ring's candidate list, which shifts the deterministic values of every table after it in the ring and changes a few
thousand expectations for no reason. The ring's five rows stay, with their adversarial shapes.

The day is 2024-06-03 to 2024-06-05, the same hours as the hand-written trades (seed.py's TRADE: AAPL's at 09:07 to
09:49 on the 3rd, MSFT's on the 4th, HSBA's on the 5th), so a booked trade has a prevailing market quote and a markout.
Shapes the features turn on, each on purpose:

  * a print before the day's first quote (AAPL's 08:59:58, Form T): an as-of join finds no quote, NULLs
  * a print AT a quote's instant (09:06:00, 09:14:00): at-or-before (`>=`) takes that quote, strictly-before (`>`) the
    one before it
  * two prints in one second (09:14:00, sequence 6 and 7): a time order ties, the sequence does not
  * off-exchange prints with no aggressor (a TRF print): a NULL in a column a group reads
  * a quote with no sizes (a delayed feed's): NULLs in a column a window reads
  * three instruments in three currencies' price scales (190, 415, 6.9): a statistic that pools them is visibly wrong
"""
from __future__ import annotations

# (id, instrument, time, bid, ask, bid size, ask size, last, day volume, source, delayed)
_QUOTES = [
    ("MQ-AAPL-01", "INST-AAPL", "2024-06-03 09:00:00", 190.40, 190.60, 300.0, 500.0, None, None, "SIP", False),
    ("MQ-AAPL-02", "INST-AAPL", "2024-06-03 09:06:00", 190.45, 190.55, 200.0, 400.0, 190.55, 600.0, "SIP", False),
    ("MQ-AAPL-03", "INST-AAPL", "2024-06-03 09:14:00", 190.90, 191.10, 500.0, 300.0, 190.55, 2100.0, "SIP", False),
    ("MQ-AAPL-04", "INST-AAPL", "2024-06-03 09:20:00", 191.20, 191.40, 100.0, 200.0, 191.25, 2900.0, "SIP", False),
    ("MQ-AAPL-05", "INST-AAPL", "2024-06-03 09:30:00", 190.70, 190.90, 400.0, 600.0, 190.95, 5500.0, "SIP", False),
    ("MQ-AAPL-06", "INST-AAPL", "2024-06-03 09:40:00", 190.10, 190.30, None, None, 190.75, 6300.0, "DELAYED", True),
    ("MQ-AAPL-07", "INST-AAPL", "2024-06-03 09:48:00", 189.90, 190.10, 700.0, 300.0, 190.25, 7500.0, "SIP", False),
    ("MQ-AAPL-08", "INST-AAPL", "2024-06-04 09:00:00", 189.70, 189.90, 200.0, 200.0, None, None, "SIP", False),
    ("MQ-MSFT-01", "INST-MSFT", "2024-06-04 09:05:00", 414.80, 415.20, 100.0, 300.0, None, None, "SIP", False),
    ("MQ-MSFT-02", "INST-MSFT", "2024-06-04 09:15:00", 415.90, 416.30, 200.0, 100.0, 415.20, 600.0, "SIP", False),
    ("MQ-MSFT-03", "INST-MSFT", "2024-06-04 09:25:00", 417.50, 417.90, 300.0, 400.0, 416.30, 1250.0, "SIP", False),
    ("MQ-HSBA-01", "INST-HSBA", "2024-06-05 09:30:00", 6.84, 6.86, 20000.0, 15000.0, None, None, "LSE", False),
    ("MQ-HSBA-02", "INST-HSBA", "2024-06-05 09:35:00", 6.88, 6.90, 10000.0, 25000.0, 6.85, 17000.0, "LSE", False),
]

# (instrument, day, [(sequence, time, price, quantity, aggressor, venue, condition, off exchange)])
_PRINTS = [
    ("INST-AAPL", "AAPL", [
        (1, "2024-06-03 08:59:58", 190.35, 100.0, "S", "ARCX", "T", False),
        (2, "2024-06-03 09:00:12", 190.60, 200.0, "B", "XNAS", "@", False),
        (3, "2024-06-03 09:03:40", 190.55, 300.0, "B", "BATS", "@", False),
        (4, "2024-06-03 09:06:00", 190.50, 500.0, "S", "XNAS", "@", False),
        (5, "2024-06-03 09:09:31", 190.55, 1000.0, "B", "IEXG", "F", False),
        (6, "2024-06-03 09:14:00", 191.00, 250.0, "B", "XNAS", "@", False),
        (7, "2024-06-03 09:14:00", 191.05, 150.0, "B", "BATS", "@", False),
        (8, "2024-06-03 09:17:25", 191.25, 400.0, "B", "XNAS", "@", False),
        (9, "2024-06-03 09:21:10", 191.35, 2000.0, None, "FINRA", "@", True),
        (10, "2024-06-03 09:26:45", 190.95, 600.0, "S", "XNAS", "@", False),
        (11, "2024-06-03 09:33:05", 190.75, 800.0, "S", "ARCX", "@", False),
        (12, "2024-06-03 09:41:30", 190.25, 1200.0, "S", "XNAS", "F", False),
        (13, "2024-06-03 09:47:55", 190.05, 300.0, "B", "BATS", "@", False),
        (14, "2024-06-03 09:49:20", 189.95, 700.0, "S", "XNAS", "@", False),
    ]),
    ("INST-MSFT", "MSFT", [
        (1, "2024-06-04 09:06:10", 415.10, 200.0, "B", "XNAS", "@", False),
        (2, "2024-06-04 09:09:45", 415.05, 100.0, "S", "BATS", "@", False),
        (3, "2024-06-04 09:12:30", 415.20, 300.0, "B", "XNAS", "@", False),
        (4, "2024-06-04 09:16:05", 416.25, 500.0, "B", "IEXG", "@", False),
        (5, "2024-06-04 09:21:40", 416.30, 150.0, None, "FINRA", "@", True),
        (6, "2024-06-04 09:26:15", 417.85, 400.0, "B", "XNAS", "@", False),
    ]),
    ("INST-HSBA", "HSBA", [
        (1, "2024-06-05 09:30:20", 6.86, 5000.0, "B", "XLON", "@", False),
        (2, "2024-06-05 09:33:50", 6.85, 12000.0, "S", "XLON", "@", False),
        (3, "2024-06-05 09:36:05", 6.90, 8000.0, "B", "CHIX", "@", False),
        (4, "2024-06-05 09:39:40", 6.89, 3000.0, "S", "XLON", "@", False),
    ]),
]


def quotes() -> list[dict]:
    out = []
    for qid, inst, at, bid, ask, bsz, asz, last, vol, src, delayed in _QUOTES:
        mid = round((bid + ask) / 2, 4)
        out.append({"QUOTE_ID": qid, "QUOTE_TIME": at, "BID": bid, "ASK": ask, "MID": mid, "LAST": last,
                    "VOLUME": vol, "OPEN_PRICE": None, "HIGH_PRICE": None, "LOW_PRICE": None, "CLOSE_PRICE": None,
                    "SOURCE": src, "IS_DELAYED": delayed, "SNAPSHOT_ID": None, "INSTRUMENT_ID": inst,
                    "BID_SIZE": bsz, "ASK_SIZE": asz})
    return out


def prints() -> list[dict]:
    out = []
    for inst, ticker, rows in _PRINTS:
        for seq, at, px, qty, side, venue, cond, off in rows:
            out.append({"PRINT_ID": f"MT-{ticker}-{seq:03d}", "SEQUENCE_NUMBER": seq, "PRINT_TIME": at, "PRICE": px,
                        "QUANTITY": qty, "AGGRESSOR_SIDE": side, "VENUE": venue, "TRADE_CONDITION": cond,
                        "IS_OFF_EXCHANGE": off, "INSTRUMENT_ID": inst})
    return out


def overlay(tables: dict[str, list[dict]]) -> None:
    """Append the day to the ring's rows. Every column the table declares is named, so nothing is left to a default."""
    for name, rows in (("MARKET_QUOTE", quotes()), ("MARKET_TRADE", prints())):
        have = {k for r in tables[name][:1] for k in r}
        for r in rows:
            if have and set(r) != have:
                raise SystemExit(f"tape.py: a {name} row names {sorted(set(r) ^ have)} unlike the ring's rows")
        tables[name] = list(tables[name]) + rows


def check(tables: dict[str, list[dict]]) -> list[str]:
    """The shapes the docstring promises, so a hand edit cannot quietly remove the case a feature turns on."""
    bad = []
    q = [r for r in tables["MARKET_QUOTE"] if r["QUOTE_ID"].startswith("MQ-") and "-" in r["QUOTE_ID"][3:]]
    p = [r for r in tables["MARKET_TRADE"] if r["PRINT_ID"].count("-") == 2]
    qt = {(r["INSTRUMENT_ID"], r["QUOTE_TIME"]) for r in q}
    if not any((r["INSTRUMENT_ID"], r["PRINT_TIME"]) in qt for r in p):
        bad.append("tape: no print at a quote's instant")
    firsts = {}
    for r in q:
        firsts[r["INSTRUMENT_ID"]] = min(firsts.get(r["INSTRUMENT_ID"], r["QUOTE_TIME"]), r["QUOTE_TIME"])
    if not any(r["PRINT_TIME"] < firsts.get(r["INSTRUMENT_ID"], "") for r in p):
        bad.append("tape: no print before its instrument's first quote")
    times = [(r["INSTRUMENT_ID"], r["PRINT_TIME"]) for r in p]
    if len(set(times)) == len(times):
        bad.append("tape: no two prints share an instant")
    if not any(r["AGGRESSOR_SIDE"] is None for r in p):
        bad.append("tape: no print without an aggressor")
    if not any(r["BID_SIZE"] is None for r in q):
        bad.append("tape: no quote without sizes")
    return bad
