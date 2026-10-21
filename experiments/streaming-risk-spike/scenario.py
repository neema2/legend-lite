"""The risk scenario every engine runs: reference data, positions, and a seeded tick schedule.

Everything is deterministic from the seeds; only the timestamps on prices and FX rates are real,
stamped by the runner when it sends a batch.

Shape (defaults):
  books        48  = 4 desks x 3 traders x 4 books                       (reference)
  instruments  2,000, each with sector, currency, beta, contract multiplier (reference)
  closes       yesterday's close per instrument                          (reference)
  positions    19,200 = 400 per book; trade bookings change them          (slow)
  prices       one per instrument; 1,000 ticks a second                   (ticking)
  fx           USD rates for EUR, GBP, JPY, CHF; 5 ticks a second         (ticking)

A price tick touches the ~10 positions in that instrument; an FX tick touches every position in
that currency (~2,000 to 4,000) and so every book: the fan-out a risk view lives on.
"""

import random
from dataclasses import dataclass, field

DESKS = ["Equities", "Rates", "Credit", "Macro"]
SECTORS = ["Tech", "Financials", "Energy", "Health", "Industrials",
           "Consumer", "Utilities", "Materials", "Telecom", "RealEstate"]
CCYS = ["USD", "EUR", "GBP", "JPY", "CHF"]
CCY_WEIGHTS = [0.5, 0.2, 0.1, 0.1, 0.1]
FX0 = {"USD": 1.0, "EUR": 1.08, "GBP": 1.27, "JPY": 0.0067, "CHF": 1.12}


@dataclass
class Reference:
    books: list
    instruments: list
    closes: list
    positions: dict          # (book, instrument) -> qty
    prices: dict             # instrument -> px
    fx: dict                 # ccy -> usd_rate
    held: set = field(default_factory=set)


def reference(seed=7, instruments=2000, traders_per_desk=3, books_per_trader=4, positions_per_book=400):
    rng = random.Random(seed)
    books = []
    for desk in DESKS:
        for t in range(traders_per_desk):
            trader = f"{desk[:3].upper()}-T{t + 1}"
            for b in range(books_per_trader):
                books.append({"book": f"{trader}-B{b + 1}", "trader": trader, "desk": desk})
    insts, closes, prices = [], [], {}
    for i in range(instruments):
        name = f"I{i:04d}"
        close = round(rng.uniform(10, 500), 2)
        insts.append({"instrument": name, "sector": rng.choice(SECTORS),
                      "ccy": rng.choices(CCYS, CCY_WEIGHTS)[0],
                      "beta": round(rng.uniform(0.5, 1.5), 3),
                      "multiplier": rng.choice([1.0, 1.0, 1.0, 10.0, 100.0])})
        closes.append({"instrument": name, "close_px": close})
        prices[name] = round(close * (1 + rng.gauss(0, 0.01)), 4)
    names = [x["instrument"] for x in insts]
    positions = {}
    for b in books:
        for inst in rng.sample(names, positions_per_book):
            positions[(b["book"], inst)] = float(rng.choice([-1, 1]) * rng.randint(1, 100) * 100)
    return Reference(books, insts, closes, positions, prices, dict(FX0), set(positions))


def schedule(ref, seed=11, seconds=60, batch_ms=100, ticks_per_s=1000, fx_ticks_per_s=5,
             bookings_per_s=20, new_position_share=0.1):
    """One batch per batch_ms: {prices: {inst: px}, fx: {ccy: rate}, positions: {(book, inst): qty}}.

    Values within a batch are per key (the last one wins), so every engine receives one
    unambiguous upsert per key per batch.
    """
    rng = random.Random(seed)
    names = [x["instrument"] for x in ref.instruments]
    book_names = [b["book"] for b in ref.books]
    px = dict(ref.prices)
    fx = dict(ref.fx)
    qty = dict(ref.positions)
    held = list(qty)
    acc = {"px": 0.0, "fx": 0.0, "bk": 0.0}
    out = []
    for _ in range(int(seconds * 1000 / batch_ms)):
        batch = {"prices": {}, "fx": {}, "positions": {}}
        for kind, rate in (("px", ticks_per_s), ("fx", fx_ticks_per_s), ("bk", bookings_per_s)):
            acc[kind] += rate * batch_ms / 1000
        while acc["px"] >= 1:
            acc["px"] -= 1
            inst = rng.choice(names)
            px[inst] = round(px[inst] * (1 + rng.gauss(0, 0.0005)), 4)
            batch["prices"][inst] = px[inst]
        while acc["fx"] >= 1:
            acc["fx"] -= 1
            ccy = rng.choice(CCYS[1:])
            fx[ccy] = round(fx[ccy] * (1 + rng.gauss(0, 0.0002)), 8)
            batch["fx"][ccy] = fx[ccy]
        while acc["bk"] >= 1:
            acc["bk"] -= 1
            if rng.random() < new_position_share:
                key = (rng.choice(book_names), rng.choice(names))
                if key not in qty:
                    held.append(key)
                qty[key] = qty.get(key, 0.0) + rng.choice([-1, 1]) * rng.randint(1, 50) * 100
            else:
                key = rng.choice(held)
                qty[key] = qty[key] + rng.choice([-1, 1]) * rng.randint(1, 20) * 100
            batch["positions"][key] = qty[key]
        out.append(batch)
    return out
