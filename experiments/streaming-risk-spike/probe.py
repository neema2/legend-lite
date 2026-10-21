"""A latency probe that needs no MAX column: one instrument, quantity 1, multiplier 1, in USD
(whose rate never ticks), close 0, held by a book of its own desk, and priced at the send time
of every batch. The Probe desk's mv_usd is then exactly that timestamp, so a subscriber seeing
the Probe row of a desk grouping knows which batch it reflects, using SUM alone.

Added after the schedule is drawn, so no booking or new position ever touches it.
"""

PROBE = {"book": "PROBE-B1", "trader": "PRB-T1", "desk": "Probe", "instrument": "PROBE"}


def add_probe(ref):
    ref.books.append({"book": PROBE["book"], "trader": PROBE["trader"], "desk": PROBE["desk"]})
    ref.instruments.append({"instrument": PROBE["instrument"], "sector": "Probe", "ccy": "USD",
                            "beta": 0.0, "multiplier": 1.0})
    ref.closes.append({"instrument": PROBE["instrument"], "close_px": 0.0})
    ref.positions[(PROBE["book"], PROBE["instrument"])] = 1.0
    ref.prices[PROBE["instrument"]] = 0.0


def stamp(batch, ts):
    batch["prices"][PROBE["instrument"]] = float(ts)
