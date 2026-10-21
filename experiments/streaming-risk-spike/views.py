"""The risk views, written once as ANSI SQL (what a legend-lite dialect would emit), plus each
engine's spelling where it differs.

position_risk joins every position to its instrument, book, live price, live FX rate and close:
  mv_usd          qty * px * multiplier * usd_rate
  pnl_usd         qty * (px - close_px) * multiplier * usd_rate
  beta_delta_usd  mv_usd * beta
px_ts / fx_ts carry the send time of the price and rate a row reflects, so a subscriber can tell
how old the newest data in an update is.

The aggregate views are DataCube's levels: the grand total (a groupBy on a constant ROOT),
desks, books (desk > trader > book), and a pivot of P&L by currency as conditional sums.
"""

from scenario import CCYS

MEASURES = ("SUM(mv_usd) AS mv_usd, SUM(pnl_usd) AS pnl_usd, SUM(beta_delta_usd) AS beta_delta_usd, "
            "COUNT(*) AS n, MAX(px_ts) AS last_px_ts, MAX(fx_ts) AS last_fx_ts")
# Pivot column names are lower case (Feldera folds unquoted identifiers: pnl_USD comes back as pnl_usd)
# and unlike any input column (Proton, as ClickHouse, lets an alias shadow a source column, so
# "SUM(...pnl_usd...) AS pnl_usd" nests the sum in itself).
PIVOT = ", ".join(f"SUM(CASE WHEN ccy = '{c}' THEN pnl_usd END) AS pnl_in_{c.lower()}" for c in CCYS)

# view name -> (group columns, select list after the group columns)
VIEWS = {
    "risk_total": (["root"], MEASURES),
    "risk_by_desk": (["desk"], MEASURES),
    "risk_by_book": (["desk", "trader", "book"], MEASURES),
    "pnl_by_desk_ccy": (["desk"], PIVOT + ", MAX(px_ts) AS last_px_ts, MAX(fx_ts) AS last_fx_ts"),
}
# the values compared with the oracle, per view
CHECKED = {
    "risk_total": ["mv_usd", "pnl_usd", "beta_delta_usd", "n"],
    "risk_by_desk": ["mv_usd", "pnl_usd", "beta_delta_usd", "n"],
    "risk_by_book": ["mv_usd", "pnl_usd", "beta_delta_usd", "n"],
    "pnl_by_desk_ccy": [f"pnl_in_{c.lower()}" for c in CCYS],
}


def position_risk(t=lambda name: name):
    """The join, with each input's name passed through t (Proton wraps them in table() for a snapshot)."""
    return f"""SELECT b.desk AS desk, b.trader AS trader, p.book AS book, i.sector AS sector, i.ccy AS ccy,
  'ROOT' AS root,
  p.qty * q.px * i.multiplier * f.usd_rate AS mv_usd,
  p.qty * (q.px - c.close_px) * i.multiplier * f.usd_rate AS pnl_usd,
  p.qty * q.px * i.multiplier * f.usd_rate * i.beta AS beta_delta_usd,
  q.ts AS px_ts, f.ts AS fx_ts
FROM {t('positions')} AS p
JOIN {t('instruments')} AS i ON p.instrument = i.instrument
JOIN {t('books')} AS b ON p.book = b.book
JOIN {t('prices')} AS q ON p.instrument = q.instrument
JOIN {t('fx')} AS f ON i.ccy = f.ccy
JOIN {t('closes')} AS c ON p.instrument = c.instrument"""


def position_risk_fx_first(t=lambda name: name):
    """The same rows, joined in another order: instruments to FX first (2,000 rows keyed by currency),
    then positions to that by instrument, so no join stage holds thousands of rows under one key."""
    return f"""SELECT b.desk AS desk, b.trader AS trader, p.book AS book, x.sector AS sector, x.ccy AS ccy,
  'ROOT' AS root,
  p.qty * q.px * x.multiplier * x.usd_rate AS mv_usd,
  p.qty * (q.px - c.close_px) * x.multiplier * x.usd_rate AS pnl_usd,
  p.qty * q.px * x.multiplier * x.usd_rate * x.beta AS beta_delta_usd,
  q.ts AS px_ts, x.fx_ts AS fx_ts
FROM {t('positions')} AS p
JOIN (SELECT i.instrument AS instrument, i.sector AS sector, i.ccy AS ccy, i.beta AS beta,
             i.multiplier AS multiplier, f.usd_rate AS usd_rate, f.ts AS fx_ts
      FROM {t('instruments')} AS i JOIN {t('fx')} AS f ON i.ccy = f.ccy) AS x ON p.instrument = x.instrument
JOIN {t('books')} AS b ON p.book = b.book
JOIN {t('prices')} AS q ON p.instrument = q.instrument
JOIN {t('closes')} AS c ON p.instrument = c.instrument"""


def aggregate(view, source):
    group, select = VIEWS[view]
    cols = ", ".join(group)
    return f"SELECT {cols}, {select} FROM {source} GROUP BY {cols}"


# ANSI DDL: Feldera runs it as written; the oracle (DuckDB) runs it too.
TABLES_ANSI = """
CREATE TABLE books (book VARCHAR NOT NULL PRIMARY KEY, trader VARCHAR NOT NULL, desk VARCHAR NOT NULL);
CREATE TABLE instruments (instrument VARCHAR NOT NULL PRIMARY KEY, sector VARCHAR NOT NULL, ccy VARCHAR NOT NULL,
                          beta DOUBLE NOT NULL, multiplier DOUBLE NOT NULL);
CREATE TABLE closes (instrument VARCHAR NOT NULL PRIMARY KEY, close_px DOUBLE NOT NULL);
CREATE TABLE positions (book VARCHAR NOT NULL, instrument VARCHAR NOT NULL, qty DOUBLE NOT NULL,
                        PRIMARY KEY (book, instrument));
CREATE TABLE prices (instrument VARCHAR NOT NULL PRIMARY KEY, px DOUBLE NOT NULL, ts BIGINT NOT NULL);
CREATE TABLE fx (ccy VARCHAR NOT NULL PRIMARY KEY, usd_rate DOUBLE NOT NULL, ts BIGINT NOT NULL);
"""

# Proton: versioned key-value streams (an upsert per primary key, the latest version joined)
TABLES_PROTON = [
    "CREATE STREAM books (book string, trader string, desk string) PRIMARY KEY book SETTINGS mode='versioned_kv'",
    "CREATE STREAM instruments (instrument string, sector string, ccy string, beta float64, multiplier float64) "
    "PRIMARY KEY instrument SETTINGS mode='versioned_kv'",
    "CREATE STREAM closes (instrument string, close_px float64) PRIMARY KEY instrument SETTINGS mode='versioned_kv'",
    "CREATE STREAM positions (book string, instrument string, qty float64) PRIMARY KEY (book, instrument) "
    "SETTINGS mode='versioned_kv'",
    "CREATE STREAM prices (instrument string, px float64, ts int64) PRIMARY KEY instrument SETTINGS mode='versioned_kv'",
    "CREATE STREAM fx (ccy string, usd_rate float64, ts int64) PRIMARY KEY ccy SETTINGS mode='versioned_kv'",
]
