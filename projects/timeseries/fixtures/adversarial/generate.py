#!/usr/bin/env python3
# Copyright 2026 Legend Contributors
# SPDX-License-Identifier: Apache-2.0
"""The adversarial markets fixture: known answers, made here, never by the code under test.

Writes prices.csv, prices_adjusted.csv, calendar.csv and manifest.yaml beside this file from one fixed seed. Five symbols over two calendar
years (2024-01-01 to 2025-12-31): two US equities on the NYSE calendar, a currency pair on a five-day week, a coin on
a seven-day week, and a listing that starts in March 2025. Into them are planted the traps manifest.yaml names, each
with the answer a naive SQL expression gives and the correct one, both worked here in plain arithmetic over the very
numbers written to prices.csv (each value parsed back from its text, so the doubles are the database's).

Standard library only; every float sum is math.fsum, correctly rounded, so the files are the same on every Python
(sum() over floats compensates since 3.12 and does not before). Run it with no arguments to write the files; with --check to compare them with what it would
write (exit 1 on any difference).
"""

import datetime as dt
import decimal
import math
import random
import sys
from pathlib import Path

SEED = 20261107
FIRST = dt.date(2024, 1, 1)
LAST = dt.date(2025, 12, 31)
# the directory this file is in, as it is found: under Bazel the test's runfiles, never the source tree behind them
HERE = Path(__file__).absolute().parent

# The NYSE's full-day closures of 2024 and 2025 (its published holiday calendar; 2025-01-09 the national day of
# mourning). Early closes are trading days and are not marked.
NYSE_HOLIDAYS = {
    dt.date(2024, 1, 1), dt.date(2024, 1, 15), dt.date(2024, 2, 19), dt.date(2024, 3, 29), dt.date(2024, 5, 27),
    dt.date(2024, 6, 19), dt.date(2024, 7, 4), dt.date(2024, 9, 2), dt.date(2024, 11, 28), dt.date(2024, 12, 25),
    dt.date(2025, 1, 1), dt.date(2025, 1, 9), dt.date(2025, 1, 20), dt.date(2025, 2, 17), dt.date(2025, 4, 18),
    dt.date(2025, 5, 26), dt.date(2025, 6, 19), dt.date(2025, 7, 4), dt.date(2025, 9, 1), dt.date(2025, 11, 27),
    dt.date(2025, 12, 25),
}

MARKETS = ("NYSE", "FX", "CRYPTO")

# symbol, market, first trading day, first close, daily volatility, decimals, typical volume
SYMBOLS = (
    ("ALPHA", "NYSE", dt.date(2024, 1, 2), 182.40, 0.015, 2, 4_000_000),
    ("BRAVO", "NYSE", dt.date(2024, 1, 2), 418.70, 0.017, 2, 1_500_000),
    ("EURUSD", "FX", dt.date(2024, 1, 1), 1.10380, 0.004, 5, 90_000),
    ("BTCUSD", "CRYPTO", dt.date(2024, 1, 1), 42_150.00, 0.030, 2, 20_000),
    ("NEWCO", "NYSE", dt.date(2025, 3, 3), 24.00, 0.025, 2, 6_000_000),
)

# The planted facts the traps read.
SPLIT_SYMBOL, SPLIT_EX_DATE, SPLIT_RATIO = "BRAVO", dt.date(2024, 6, 10), 2
STALE_SYMBOL, STALE_DAYS = "BRAVO", [dt.date(2024, 10, d) for d in (14, 15, 16, 17, 18)]
LATE_SYMBOL, LATE_DATE, LATE_AFTER = "BRAVO", dt.date(2025, 3, 12), dt.date(2025, 3, 14)
DUP_SYMBOL, DUP_DATE = "BTCUSD", dt.date(2025, 2, 5)
# the corrected print re-sent for DUP_DATE, right after the original: its close and its volume moved by these
CORRECTION_CLOSE, CORRECTION_VOLUME = 1.006, 1.15
ZERO_SYMBOL, ZERO_DATE, ZERO_PREMIUM = "BRAVO", dt.date(2025, 8, 6), 1.08
VOL_WINDOW, VOL_YEAR = 10, 252
# the gs-quant whose definitions the correct answers follow: prose for the manifest's why, no file any test reads
GS_QUANT = "gs-quant 2.2.3, the release the companion oracle PR pins in tools/gs-quant-oracle/pins.env"


def days():
    d = FIRST
    while d <= LAST:
        yield d
        d += dt.timedelta(days=1)


def business(market, d):
    if market == "CRYPTO":
        return True
    if d.weekday() >= 5:
        return False
    return market == "FX" or d not in NYSE_HOLIDAYS


def text(x, decimals):
    return f"{x:.{decimals}f}"


def generate():
    """Each symbol's rows, as written: (symbol, date, open, high, low, close, volume), every price its text."""
    rng = random.Random(SEED)
    out = {}
    for symbol, market, first, start, sigma, dp, volume in SYMBOLS:
        rows = []
        true_close = start
        prev_text = None
        for d in days():
            if d < first or not business(market, d):
                continue
            if rows:
                # a move whose rounded close differs from the last one written: the planted stale run is the only
                # repeat of a close in the file, so a naive change-only filter removes exactly it
                while True:
                    move = rng.gauss(0.0004, sigma)
                    candidate = true_close * math.exp(move)
                    if symbol == SPLIT_SYMBOL and d == SPLIT_EX_DATE:
                        candidate /= SPLIT_RATIO
                    if text(candidate, dp) != prev_text:
                        break
                true_close = candidate
            close = text(true_close, dp)
            gap = abs(rng.gauss(0, sigma * 0.3))
            reference = float(prev_text) if prev_text else true_close
            if symbol == SPLIT_SYMBOL and d == SPLIT_EX_DATE:
                reference /= SPLIT_RATIO
            open_ = reference * math.exp(rng.gauss(0, sigma * 0.3))
            high = max(open_, true_close) * (1 + gap)
            low = min(open_, true_close) * (1 - gap)
            vol = int(volume * math.exp(rng.gauss(0, 0.35)))
            if symbol == SPLIT_SYMBOL and d >= SPLIT_EX_DATE:
                vol *= SPLIT_RATIO
            row = [symbol, d, text(open_, dp), text(high, dp), text(low, dp), close, vol]
            if symbol == STALE_SYMBOL and d in STALE_DAYS[1:]:
                # a stale feed: the first stale day's close repeated, every price of the row the same
                stale = rows[-1][5]
                row[2:6] = [stale] * 4
                true_close = float(stale)
                close = stale
            if symbol == ZERO_SYMBOL and d == ZERO_DATE:
                # a zero-volume print off the market: its price never trades, and the walk goes on from the true close
                bad = text(true_close * ZERO_PREMIUM, dp)
                row[2:7] = [bad, bad, bad, bad, 0]
            rows.append(row)
            prev_text = row[5] if not (symbol == ZERO_SYMBOL and d == ZERO_DATE) else close
        repeats = [b[1] for a, b in zip(rows, rows[1:]) if a[5] == b[5]]
        assert repeats == (STALE_DAYS[1:] if symbol == STALE_SYMBOL else []), (symbol, repeats)
        out[symbol] = rows
    return out


def file_order(rows_by_symbol):
    """The rows in prices.csv's order: by symbol as declared, then date, but for the late print (written after the
    row it should precede) and the duplicate (the original print, then its correction for the same date, a different
    close and volume: the second row is the correction)."""
    out = []
    for symbol, *_ in SYMBOLS:
        rows = list(rows_by_symbol[symbol])
        if symbol == LATE_SYMBOL:
            late = next(r for r in rows if r[1] == LATE_DATE)
            rows.remove(late)
            rows.insert(next(i for i, r in enumerate(rows) if r[1] == LATE_AFTER) + 1, late)
        if symbol == DUP_SYMBOL:
            i = next(i for i, r in enumerate(rows) if r[1] == DUP_DATE)
            original = rows[i]
            close = text(float(original[5]) * CORRECTION_CLOSE, 2)
            correction = original[:2] + [original[2], text(max(float(original[3]), float(close)), 2),
                                         text(min(float(original[4]), float(close)), 2), close,
                                         int(original[6] * CORRECTION_VOLUME)]
            assert correction[5] != original[5] and correction[6] != original[6]
            rows.insert(i + 1, correction)
        out.extend(rows)
    return out


# ---------------------------------------------------------------------------------------------------- the answers


def series(rows, symbol, column=5):
    """symbol's rows in date order (one per date), {date: float of the column}; the file's own text parsed."""
    return {r[1]: float(r[column]) for r in sorted((r for r in rows if r[0] == symbol), key=lambda r: r[1])}


def asof(s, d):
    """s's latest date at or before d, and its value."""
    best = max(k for k in s if k <= d)
    return best, s[best]


def prev_obs(s, d, n=1):
    keys = sorted(s)
    return keys[keys.index(d) - n]


def minus_months(d, n):
    """d less n calendar months, a month's end clamped (DuckDB's date - INTERVAL n MONTH; Pure's adjust)."""
    y, m = divmod(d.year * 12 + d.month - 1 - n, 12)
    m += 1
    last = (dt.date(y + m // 12, m % 12 + 1, 1) - dt.timedelta(days=1)).day
    return dt.date(y, m, min(d.day, last))


def week_of(d):
    monday = d - dt.timedelta(days=d.weekday())
    return [monday + dt.timedelta(days=i) for i in range(7)]


def volatility(closes):
    """gs-quant's volatility over the returns of closes (each against the one before): the returns' sample standard
    deviation (N - 1), times the square root of VOL_YEAR, times 100."""
    r = [b / a - 1 for a, b in zip(closes, closes[1:])]
    m = math.fsum(r) / len(r)
    return math.sqrt(math.fsum((x - m) ** 2 for x in r) / (len(r) - 1)) * math.sqrt(VOL_YEAR) * 100


def iso(d):
    return d.isoformat()


def answer(date, value):
    return {"date": iso(date), "value": value}


def adjusted(written):
    """The rows split-adjusted, in the same order: SPLIT_SYMBOL's rows before the ex-date with every price divided by
    the ratio (exact: a price's text halved, its trailing zeros kept to the file's decimals or more) and the volume
    multiplied by it; every other row as written."""
    out = []
    for r in written:
        if r[0] == SPLIT_SYMBOL and r[1] < SPLIT_EX_DATE:
            prices = [decimal.Decimal(x) / SPLIT_RATIO for x in r[2:6]]
            r = r[:2] + [f"{x:.3f}" if x != x.quantize(decimal.Decimal("0.01")) else f"{x:.2f}" for x in prices]\
                + [r[6] * SPLIT_RATIO]
        out.append(list(r))
    return out


def traps(rows_by_symbol, written, adjusted_rows):
    alpha = series(written, "ALPHA")
    bravo = series(written, "BRAVO")
    bravo_adjusted = series(adjusted_rows, "BRAVO")
    newco = series(written, "NEWCO")
    out = []

    # 1. a holiday gap: the day after Thanksgiving 2025
    d = dt.date(2025, 11, 28)
    p = prev_obs(alpha, d)
    assert p == dt.date(2025, 11, 26), p
    HOLIDAY = dt.date(2025, 11, 27)
    assert HOLIDAY in NYSE_HOLIDAYS and p < HOLIDAY < d
    # the positive control: the same naive SQL on the trading day before, whose calendar day before traded too
    control = p
    assert control - dt.timedelta(days=1) in alpha

    def holiday_sql(day):
        return ("SELECT t.date, (SELECT t.close / y.close - 1 FROM prices y\n"
                "    WHERE y.symbol = t.symbol AND y.date = t.date - INTERVAL 1 DAY) AS value\n"
                f"FROM prices t WHERE t.symbol = 'ALPHA' AND t.date = DATE '{iso(day)}'")

    out.append({
        "name": "holiday_gap",
        "symbol": "ALPHA",
        "date_range": [iso(p), iso(d)],
        "query": "returns(close['ALPHA'], 1) on " + iso(d),
        "market": "NYSE",
        "holiday": iso(HOLIDAY),
        "naive_sql": holiday_sql(d),
        "naive_sql_answer": answer(d, None),
        "correct_answer": answer(d, alpha[d] / alpha[p] - 1),
        "control_sql": holiday_sql(control),
        "control_answer": answer(control, alpha[control] / alpha[control - dt.timedelta(days=1)] - 1),
        "why": "The NYSE is closed on 2025-11-27 (calendar.csv). A one-day return is over one observation, the previous"
               " trading day's close (2025-11-26); gs-quant's reading: see the oracle PR's README table. The naive join"
               " reads 'yesterday' as the calendar day before, finds no row for the holiday, and loses the return.",
    })

    # 2. FILL(PREV) across a series start: NEWCO lists on 2025-03-03
    d = dt.date(2025, 2, 28)
    first = min(newco)
    assert first == dt.date(2025, 3, 3)
    out.append({
        "name": "fill_prev_series_start",
        "symbol": "NEWCO",
        "date_range": [iso(d), iso(first)],
        "query": "close['ALPHA'] - close['NEWCO'] AS spread ... FILL(PREV), on " + iso(d),
        "naive_sql": (
            "SELECT a.close - COALESCE(\n"
            "    (SELECT n.close FROM prices n WHERE n.symbol = 'NEWCO' AND n.date <= a.date ORDER BY n.date DESC LIMIT 1),\n"
            "    (SELECT n.close FROM prices n WHERE n.symbol = 'NEWCO' ORDER BY n.date LIMIT 1)) AS value, a.date\n"
            "FROM prices a WHERE a.symbol = 'ALPHA' AND a.date = DATE '2025-02-28'"),
        "naive_sql_answer": answer(d, alpha[d] - newco[first]),
        "correct_answer": answer(d, None),
        "witness": answer(first, alpha[first] - newco[first]),
        "why": "Before a line's first value there is nothing to carry forward: FILL(PREV) gives NEWCO none on"
               " 2025-02-28, so the spread has no value and no row (R12). The naive fill is pandas' ffill().bfill() --"
               " gs-quant's step alignment, register S145 -- which backfills NEWCO's first close into the dates before"
               " it listed: a price read from the future.",
    })

    # 3. a weekly SAMPLE BY period ending on a holiday: the week of 2025-07-04
    week = week_of(dt.date(2025, 7, 4))
    traded = [x for x in week if x in alpha]
    assert traded[-1] == dt.date(2025, 7, 3), traded
    # the positive control: the week before, whose Friday traded
    control = week_of(week[0] - dt.timedelta(days=7))
    assert control[4] in alpha

    def friday_sql(monday):
        friday = f"CAST(date_trunc('week', DATE '{iso(monday)}') + INTERVAL 4 DAY AS DATE)"
        return (f"SELECT {friday} AS date, (SELECT close FROM prices WHERE symbol = 'ALPHA'\n"
                f"    AND date = {friday}) AS value")

    out.append({
        "name": "weekly_sample_holiday_end",
        "symbol": "ALPHA",
        "date_range": [iso(week[0]), iso(week[-1])],
        "query": "last(close['ALPHA']) ... SAMPLE BY 1w (AT LAST), the week of 2025-06-30",
        "naive_sql": friday_sql(week[0]),
        "naive_sql_answer": answer(week[4], None),
        "correct_answer": answer(traded[-1], alpha[traded[-1]]),
        "control_sql": friday_sql(control[0]),
        "control_answer": answer(control[4], alpha[control[4]]),
        "why": "The week's last trading day is Thursday 2025-07-03: Friday is Independence Day. SAMPLE BY 1w takes each"
               " week's last observation and dates the period AT LAST, that observation's day (R5). The naive 'weekly"
               " close is Friday's close' finds no Friday row and loses the week (dated at a day nothing traded).",
    })

    # 4. a late print: BRAVO's 2025-03-12 row arrives after 2025-03-14's
    d = dt.date(2025, 3, 13)
    p = prev_obs(bravo, d)
    assert p == LATE_DATE
    loaded = [r[1] for r in written if r[0] == "BRAVO"]
    naive_prev = loaded[loaded.index(d) - 1]
    assert naive_prev == dt.date(2025, 3, 11), naive_prev
    out.append({
        "name": "late_print",
        "symbol": "BRAVO",
        "date_range": [iso(dt.date(2025, 3, 11)), iso(dt.date(2025, 3, 17))],
        "query": "returns(close['BRAVO'], 1) on " + iso(d),
        "note": "prices.csv is in file order, not date order, on purpose; sorting by (symbol, date) is the consumer's"
                " job.",
        "file_order": [iso(x) for x in loaded[loaded.index(naive_prev):loaded.index(naive_prev) + 5]],
        "naive_sql": (
            "SELECT date, value FROM (SELECT date, close / LAG(close) OVER (PARTITION BY symbol ORDER BY seq) - 1 AS value\n"
            "    FROM prices WHERE symbol = 'BRAVO') WHERE date = DATE '2025-03-13'"),
        "naive_sql_answer": answer(d, bravo[d] / bravo[naive_prev] - 1),
        "correct_answer": answer(d, bravo[d] / bravo[p] - 1),
        "why": "prices.csv holds BRAVO's 2025-03-12 row after 2025-03-14's, as a late print arrives. A line is ordered"
               " by its dates, never by arrival: 2025-03-13's previous observation is 2025-03-12. Ordering by arrival"
               " (seq, the ingest sequence) takes 2025-03-11 as its previous. (gs-quant's reading, after the"
               " oracle's sort by date: see the oracle PR's README table.)",
    })

    # 5. a duplicate row: BTCUSD's 2025-02-05 print, then its correction re-sent for the same date
    week = week_of(DUP_DATE)
    btc_rows = [r for r in written if r[0] == "BTCUSD" and r[1] in week]
    assert len(btc_rows) == 8, len(btc_rows)
    both = sum(r[6] for r in btc_rows)
    first_print = {}
    for r in btc_rows:
        first_print.setdefault(r[1], r[6])
    last_print = {r[1]: r[6] for r in btc_rows}  # file order is seq order: the later row wins
    once, last = sum(first_print.values()), sum(last_print.values())
    assert len({both, once, last}) == 3, (both, once, last)
    original, correction = [r for r in btc_rows if r[1] == DUP_DATE]
    out.append({
        "name": "duplicate_row",
        "symbol": "BTCUSD",
        "duplicate": iso(DUP_DATE),
        "date_range": [iso(week[0]), iso(week[-1])],
        "query": "sum(volume['BTCUSD']) ... SAMPLE BY 1w over the rows deduplicated by (symbol, date), the greatest"
                 " seq kept (last print wins), the week of 2025-02-03",
        "naive_sql": (
            "SELECT max(date) AS date, sum(volume) AS value FROM prices\n"
            "WHERE symbol = 'BTCUSD' AND date BETWEEN DATE '2025-02-03' AND DATE '2025-02-09'"),
        "naive_sql_answer": answer(week[-1], both),
        "correct_answer": answer(week[-1], last),
        "why": "BTCUSD's 2025-02-05 print (close " + original[5] + ", volume " + str(original[6]) + ") is re-sent"
               " corrected for the same date (close " + correction[5] + ", volume " + str(correction[6]) + "), the"
               " correction the later row, its seq the greater. Last print wins: of a line's rows on one date the"
               f" one with the greatest ingest sequence stands, so the week's volume is {last}. Counting both rows,"
               f" as the naive sum and lite's SAMPLE BY sum today do, gives {both}; counting the date once by its"
               f" first print would give {once} (this generator's arithmetic, no query's measurement). File order"
               " is not an order the database sees; seq is.",
    })

    # 6. a stale price repeated five days: BRAVO 2024-10-14 to 2024-10-18
    d = dt.date(2024, 10, 21)
    assert len({bravo[x] for x in STALE_DAYS}) == 1
    keys = sorted(bravo)
    window = keys[keys.index(d) - VOL_WINDOW:keys.index(d) + 1]
    assert window[0] == dt.date(2024, 10, 7) and set(STALE_DAYS) <= set(window), window
    changes = [k for i, k in enumerate(keys) if i == 0 or bravo[k] != bravo[keys[i - 1]]]
    naive_window = changes[changes.index(d) - VOL_WINDOW:changes.index(d) + 1]
    out.append({
        "name": "stale_price_5d",
        "symbol": "BRAVO",
        "date_range": [iso(window[0]), iso(d)],
        "query": f"volatility(close['BRAVO'], {VOL_WINDOW}, annualization_factor => {VOL_YEAR}) on " + iso(d),
        "naive_sql": (
            "SELECT date, value FROM (\n"
            f"    SELECT date, stddev_samp(r) OVER (ORDER BY date ROWS BETWEEN {VOL_WINDOW - 1} PRECEDING AND CURRENT ROW)"
            f" * sqrt({VOL_YEAR}) * 100 AS value\n"
            "    FROM (SELECT date, close / LAG(close) OVER (ORDER BY date) - 1 AS r FROM (\n"
            "        SELECT * FROM prices WHERE symbol = 'BRAVO'\n"
            "        QUALIFY close IS DISTINCT FROM LAG(close) OVER (ORDER BY date))))\n"
            "WHERE date = DATE '2024-10-21'"),
        "naive_sql_answer": answer(d, volatility([bravo[k] for k in naive_window])),
        "correct_answer": answer(d, volatility([bravo[k] for k in window])),
        "why": "BRAVO's close is " + text(bravo[STALE_DAYS[0]], 2) + " on each of 2024-10-14 to 2024-10-18: a stale"
               " feed, five observations all the same, four returns of 0. correct_answer is gs-quant's, not ours:"
               " in " + GS_QUANT + f", volatility(x, {VOL_WINDOW}, annualization_factor={VOL_YEAR}) is returns(x) (x /"
               f" x.shift(1) - 1), then ret.rolling({VOL_WINDOW}, 0).std() (pandas' sample deviation, N - 1), times"
               f" sqrt({VOL_YEAR}) times 100 (timeseries/econometrics.py volatility, statistics.py std, helper.py"
               " normalize_window and apply_ramp), so its window of ten returns counts the four zeros. A change-only"
               " filter (keep a row where the close moved) drops four of the rows, so the zero returns never exist, a"
               " five-day move is one return, and the window reaches back to " + iso(naive_window[0]) + ".",
    })

    # 7. a 2:1 split: BRAVO ex-date 2024-06-10
    d = SPLIT_EX_DATE
    p = prev_obs(bravo, d)
    out.append({
        "name": "split_2_for_1",
        "symbol": "BRAVO",
        "date_range": [iso(p), iso(d)],
        "split": {"ex_date": iso(d), "ratio": SPLIT_RATIO},
        "query": "returns(close['BRAVO'], 1) on " + iso(d) + ", the closes before the ex-date divided by the ratio",
        "naive_sql": (
            "SELECT t.date, t.close / y.close - 1 AS value FROM prices t JOIN prices y ON y.symbol = t.symbol\n"
            "    AND y.date = (SELECT max(date) FROM prices WHERE symbol = t.symbol AND date < t.date)\n"
            "WHERE t.symbol = 'BRAVO' AND t.date = DATE '2024-06-10'"),
        "naive_sql_answer": answer(d, bravo[d] / bravo[p] - 1),
        "correct_answer": answer(d, bravo_adjusted[d] / bravo_adjusted[p] - 1),
        "why": "BRAVO splits 2 for 1 effective 2024-06-10: prices.csv holds the raw prints, which halve that day, and"
               " the volumes, which double. A holder lost nothing; the return is against the previous close adjusted"
               " by the ratio (this entry's split). The raw ratio reads the split as a loss of about half. prices.csv"
               " carries no corporate actions: the adjustment is this entry's, and prices_adjusted.csv holds the rows"
               " with it applied (BRAVO's prices before the ex-date divided by the ratio, its volumes multiplied by it);"
               " the correct answer is the raw query's over that file.",
    })

    # 8. a 3-month tenor crossing a month-end: ALPHA on 2024-05-31
    d = dt.date(2024, 5, 31)
    target = minus_months(d, 3)
    assert target == dt.date(2024, 2, 29)
    got, base = asof(alpha, target)
    naive_d, naive_base = asof(alpha, d - dt.timedelta(days=90))
    assert got == target and naive_d == dt.date(2024, 3, 1), (got, naive_d)
    out.append({
        "name": "tenor_3m_month_end",
        "symbol": "ALPHA",
        "date_range": [iso(target), iso(d)],
        "query": "returns(close['ALPHA'], 3m) on " + iso(d),
        "naive_sql": (
            "SELECT t.date, t.close / b.close - 1 AS value FROM prices t ASOF JOIN prices b\n"
            "    ON b.symbol = t.symbol AND b.date <= t.date - INTERVAL 90 DAY\n"
            "WHERE t.symbol = 'ALPHA' AND t.date = DATE '2024-05-31'"),
        "naive_sql_answer": answer(d, alpha[d] / naive_base - 1),
        "correct_answer": answer(d, alpha[d] / base - 1),
        "why": "3m is three months (R14: m is months, as gs-quant's tenors and RDates have it). Three months before"
               " 2024-05-31 is February's last day, 2024-02-29 (a month's end clamped; a leap year), a trading day."
               " The naive 3m = 90 days lands on Saturday 2024-03-02 and takes 2024-03-01's close as of it. (Read as"
               " three minutes, as SQL's interval '3m' is in Postgres, the base is the same day and the return 0.)",
    })

    # 9. a VWAP under SAMPLE BY over a week with a zero-volume print: BRAVO, the week of 2025-08-04
    week = week_of(ZERO_DATE)
    rows = [r for r in written if r[0] == "BRAVO" and r[1] in week]
    assert len(rows) == 5 and any(r[6] == 0 for r in rows)
    vwap = math.fsum(float(r[5]) * r[6] for r in rows) / sum(r[6] for r in rows)
    mean = math.fsum(float(r[5]) for r in rows) / len(rows)
    out.append({
        "name": "vwap_zero_volume",
        "symbol": "BRAVO",
        "date_range": [iso(week[0]), iso(rows[-1][1])],
        "query": "wavg(close['BRAVO'], volume['BRAVO']) ... SAMPLE BY 1w, the week of 2025-08-04",
        "naive_sql": (
            "SELECT max(date) AS date, avg(close) AS value FROM prices\n"
            "WHERE symbol = 'BRAVO' AND date BETWEEN DATE '2025-08-04' AND DATE '2025-08-10'"),
        "naive_sql_answer": answer(rows[-1][1], mean),
        "correct_answer": answer(rows[-1][1], vwap),
        "why": "BRAVO's 2025-08-06 row is a print of no volume " + str(round((ZERO_PREMIUM - 1) * 100)) + "% off the"
               " market. The week's volume-weighted price, sum(close * volume) / sum(volume), gives it no weight; the"
               " plain mean of the closes counts it as a fifth of the week. (Daily closes stand in for the trades.)",
    })

    for t in out:
        n, c = t["naive_sql_answer"], t["correct_answer"]
        if n["value"] is not None and c["value"] is not None:
            assert abs(n["value"] - c["value"]) > 1e-6 * max(1, abs(c["value"])), t["name"]
        else:
            assert (n["value"] is None) != (c["value"] is None), t["name"]
    return out


# ---------------------------------------------------------------------------------------------------- the files


COLUMNS = ("symbol", "date", "open", "high", "low", "close", "volume", "seq")


def prices_csv(written):
    """The rows, each with seq, its ingest sequence: 1, 2, ... in file order, the last column."""
    lines = [",".join(COLUMNS)]
    lines += [",".join([r[0], iso(r[1]), r[2], r[3], r[4], r[5], str(r[6]), str(seq)])
              for seq, r in enumerate(written, start=1)]
    return "\n".join(lines) + "\n"


def calendar_csv():
    lines = ["date,market,is_business_day"]
    lines += [f"{iso(d)},{m},{'true' if business(m, d) else 'false'}" for d in days() for m in MARKETS]
    return "\n".join(lines) + "\n"


def scalar(v):
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        return repr(v)
    return "'" + str(v).replace("'", "''") + "'"


def flow(m):
    return "{" + ", ".join(f"{k}: {scalar(v)}" for k, v in m.items()) + "}"


def manifest(trap_list):
    lines = [
        "# GENERATED by generate.py -- DO NOT EDIT. Regenerate: python3 generate.py (--check compares).",
        "# Each trap: the answer a naive SQL expression gives (naive_sql, DuckDB, over a table prices with the",
        "# rows of prices.csv, loaded in file order) and the correct one, both worked by generate.py over prices.csv's",
        "# own numbers. An answer is the value read at a date; a value null is no value. Every naive_sql answers one",
        "# row, its date and its value; a control_sql is a trap's naive SQL on a neighbouring date, which must answer",
        "# its control_answer (the trap is the date, not the SQL failing everywhere); a witness is a date the correct",
        "# query must answer on, so an answer of none is no empty result.",
        "# prices.csv (and prices_adjusted.csv, the same rows with the split applied) is in FILE order, not date order,",
        "# on purpose: a late print and a duplicate row are where they arrived. Sorting by (symbol, date) is the",
        "# consumer's job. seq, the last column, is each row's ingest sequence: an integer strictly increasing in",
        "# file order, the order the database can see (file order it cannot).",
        f"seed: {SEED}",
        f"columns: [{', '.join(COLUMNS)}]",
        f"range: [{scalar(iso(FIRST))}, {scalar(iso(LAST))}]",
        "symbols:",
    ]
    for symbol, market, first, *_ in SYMBOLS:
        lines.append(f"  - {flow({'symbol': symbol, 'market': market, 'first': iso(first)})}")
    lines.append("traps:")
    for t in trap_list:
        lines.append(f"  - name: {t['name']}")
        for k in ("symbol", "market", "holiday", "duplicate", "date_range", "split", "note", "file_order", "query", "naive_sql",
                  "naive_sql_answer", "correct_answer", "control_sql", "control_answer", "witness", "why"):
            if k not in t:
                continue
            v = t[k]
            if k in ("naive_sql", "control_sql"):
                lines.append(f"    {k}: |")
                lines += [f"      {x}" for x in v.split("\n")]
            elif isinstance(v, dict):
                lines.append(f"    {k}: {flow(v)}")
            elif isinstance(v, list):
                lines.append(f"    {k}: [{', '.join(scalar(x) for x in v)}]")
            else:
                lines.append(f"    {k}: {scalar(v)}")
    return "\n".join(lines) + "\n"


def main(argv):
    rows = generate()
    written = file_order(rows)
    adjusted_rows = adjusted(written)
    files = {
        "prices.csv": prices_csv(written),
        "prices_adjusted.csv": prices_csv(adjusted_rows),
        "calendar.csv": calendar_csv(),
        "manifest.yaml": manifest(traps(rows, written, adjusted_rows)),
    }
    if "--check" in argv:
        stale = [n for n, body in files.items() if not (HERE / n).exists() or (HERE / n).read_text() != body]
        for n in stale:
            have = (HERE / n).read_text().split("\n") if (HERE / n).exists() else []
            want = files[n].split("\n")
            line = next((i for i, (x, y) in enumerate(zip(have, want)) if x != y), min(len(have), len(want)))
            print(f"stale: {n}, from line {line + 1}:\n  committed: {have[line] if line < len(have) else '(end)'}"
                  f"\n  generated: {want[line] if line < len(want) else '(end)'}", file=sys.stderr)
        if stale:
            print("run python3 generate.py (Python " + sys.version.split()[0] + " here)", file=sys.stderr)
            return 1
        return 0
    for name, body in files.items():
        (HERE / name).write_text(body)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
