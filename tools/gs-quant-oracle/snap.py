# Copyright 2026 Legend Contributors
# SPDX-License-Identifier: Apache-2.0
"""THE GS-QUANT ORACLE (README.md; the contract of CLAUDE.local.md): gs-quant's own answer for every mapped word of
#TS, snapped, so that //core:ts_gsquant_parity_test holds lite's Pure library to gs_quant.timeseries itself.

  snap.sh --write [--only GLOB]     run every case of mapping.yaml, write answers.jsonl.gz and ledger.tsv (its
                                    gsquant and digest columns)
  snap.sh --check [--only GLOB]     run them again, report DRIFT (a series' digest against the ledger's), UNSNAPPED,
                                    STALE, GONE, VERSION, TAG and READER; exit 1 on any
  snap.sh --write --fixture DIR     the same over an adversarial fixture (DIR/manifest.yaml names the traps): its
                                    prices.csv into answers-adversarial-raw.jsonl.gz and ledger-adversarial-raw.tsv, its
                                    prices_adjusted.csv (the split applied) into the -adversarial-adjusted pair

Each case of mapping.yaml is a lite #TS item and its gs-quant twin; this tool runs the twin (gs-quant at the pin,
pins.env's GS_QUANT_VERSION: any other installed release is refused, VERSION) over the samples the gallery reads (//datacube:ts_replay_samples, built first: `bazel build
//datacube:ts_replay_samples`), and writes one row per series, gzipped (README.md, "The files"): {word, args,
symbol, dates, values} -- values in date order along the case's spine, dates [first, last, count] (or the dates
themselves, for a series off its spine). A case gs-quant raises on is GS_ERROR, its message kept; a SKIP word is
ledgered with its reason and runs nothing. The ledger's duckdb column is the parity test's (-Dts.gsquant.update
rewrites it); this tool keeps it as it finds it, and writes each series' digest beside it.

gs-quant is not one of the repository's locked requirements: it runs here only, by hand (the IQ and Neo4j oracles'
rule), and CI judges from the committed files.
"""

import argparse
import datetime as dt
import fnmatch
import gzip
import hashlib
import io
import json
import math
import os
import re
import sys
import warnings

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
MAPPING = os.path.join(HERE, "mapping.yaml")
LIBRARY = os.path.join(ROOT, "projects", "timeseries", "model.pure")
SAMPLES = os.path.join(ROOT, "bazel-bin", "datacube", "generated", "ts-replay-samples.json")
PINS = os.path.join(HERE, "pins.env")
REQUIREMENTS = os.path.join(HERE, "requirements.txt")
FORMAT = 3
X, Y, MACRO = "NVDA", "SPX", "CPI"
LEDGER_HEADER = (
    "# THE GS-QUANT ORACLE'S LEDGER (tools/gs-quant-oracle/README.md): each case of mapping.yaml, `<word> <args>`, its\n"
    "# gs-quant side (snap.py: OK, GS_ERROR, SKIP) and its lite side on DuckDB (//core:ts_gsquant_parity_test: PASS,\n"
    "# DIFF, REFUSED, GS_ERROR, SKIP), the register row a DIFF is ruled by, gs-quant's series' sha256 (snap.py: --check\n"
    "# holds a live run to it), a note (dup-span: the series covers a date with two rows of one line), and why. A\n"
    "# verdict moves only with the test's -Dts.gsquant.update, never by hand.\n"
    "# snapped under pins.env: {pin}\n"
    "case\tgsquant\tduckdb\tregister\tdigest\tnote\twhy\n")
COLUMNS = ("case", "gsquant", "duckdb", "register", "digest", "note", "why")
# `why`'s second half, the parity test's account of a DIFF or a REFUSED; the first is gs-quant's, this tool's
LITE = " | lite: "
PIN_LINE = re.compile(r"# snapped under pins.env: (\S+)$")
# the stack an answer is computed on, each a distribution pins.env holds exactly (gs-quant declares only ranges:
# numpy>1.17.0, pandas>=1.4, scipy>=1.2.0, statsmodels>=0.14.5), in requirements.txt's order; and Python's minor
DISTRIBUTIONS = (("GS_QUANT_VERSION", "gs-quant"), ("PANDAS_VERSION", "pandas"), ("NUMPY_VERSION", "numpy"),
                 ("SCIPY_VERSION", "scipy"), ("STATSMODELS_VERSION", "statsmodels"))


def pins():
    """pins.env (KEY=VALUE, as tools/oracle-pins.env): each distribution's release and PYTHON_VERSION, every one
    required; and requirements.txt reading each, in order. No pin is defaulted."""
    values = {}
    for line in open(PINS, encoding="utf-8"):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            values[k] = v
    keys = [k for k, _ in DISTRIBUTIONS] + ["PYTHON_VERSION"]
    missing = [k for k in keys if k not in values]
    if missing or set(values) != set(keys):
        raise SystemExit("PIN: " + PINS + " must hold exactly " + ", ".join(keys) + " (missing " + repr(missing)
                         + ", extra " + repr(sorted(set(values) - set(keys))) + ")")
    wanted = [l.strip() for l in open(REQUIREMENTS, encoding="utf-8") if l.strip() and not l.startswith("#")]
    if wanted != [d + "==${" + k + "}" for k, d in DISTRIBUTIONS]:
        raise SystemExit("PIN: " + REQUIREMENTS + " must read each pin, in order: "
                         + ", ".join(d + "==${" + k + "}" for k, d in DISTRIBUTIONS) + "; not " + repr(wanted))
    return values


def stack(values):
    """The pins as one text, every answer's hash and every ledger's header holding it: KEY=VALUE, sorted, `;`-joined
    (TsGsquantParityTest.stack computes the same)."""
    return ";".join(k + "=" + values[k] for k in sorted(values))


def installed_problems(values):
    """VERSION: an installed distribution, or Python's minor, that is not its pin. No fallback: an answer computed on
    another stack is not this oracle's (snap.sh installs the pins)."""
    import importlib.metadata
    out = []
    for key, dist in DISTRIBUTIONS:
        have = importlib.metadata.version(dist)
        if have != values[key]:
            out.append("VERSION: " + dist + " " + have + " is installed, the pin is " + values[key] + " (pins.env " + key
                       + "); tools/gs-quant-oracle/snap.sh installs it from requirements.txt")
    minor = str(sys.version_info[0]) + "." + str(sys.version_info[1])
    if minor != values["PYTHON_VERSION"]:
        out.append("VERSION: Python " + minor + " runs this, the pin is " + values["PYTHON_VERSION"] + " (pins.env)")
    return out


# ------------------------------------------------------------------ the mapping


def mapping():
    import yaml
    with open(MAPPING, encoding="utf-8") as f:
        return yaml.safe_load(f)


def cases(m):
    """Every case of the mapping, in its order: (key, word entry, args, ts, gs, column) -- a SKIP word one case
    keyed by its word alone, with neither text."""
    out = []
    for w in m["words"]:
        if "skip" in w:
            out.append((w["word"], w, None, None, None, None))
            continue
        for c in w["cases"]:
            args, ts_text, gs_text = str(c[0]), c[1], c[2]
            column = c[3] if len(c) > 3 else None
            out.append((w["word"] + " " + args, w, args, ts_text, gs_text, column))
    return out


def line_cases(path=MAPPING):
    """The mapping as //core:ts_gsquant_parity_test's line reader reads it (TsGsquantParityTest.Mapping): a word's
    `- word:` line, its `known:` flow map, each case one `- [...]` line of double-quoted or plain scalars; a SKIP word
    one `- {word: ..., skip: ...}` line. snap.py holds the YAML reader's cases to these (READER), so the two sides
    read one list."""
    words = []
    for line in open(path, encoding="utf-8").read().split("\n"):
        m = re.match(r"  - word: (\S+)$", line)
        if m:
            words.append({"word": m.group(1), "cases": [], "known": {}})
            continue
        m = re.match(r"  - \{word: (\w+),", line)
        if m:
            words.append({"word": m.group(1), "skip": True})
            continue
        m = re.match(r"    known: \{(.*)\}$", line)
        if m:
            for part in m.group(1).split(", "):
                k, v = part.split(": ")
                words[-1]["known"][k] = v
            continue
        m = re.match(r"      - \[(.*)\]$", line)
        if m:
            words[-1]["cases"].append(flow_scalars(m.group(1)))
    return words


def flow_scalars(text):
    """A YAML flow sequence's scalars, the subset the mapping writes: "..." (with \" and \\) or plain, split at ", "."""
    out, i = [], 0
    while i < len(text):
        if text[i] == '"':
            j, buf = i + 1, []
            while text[j] != '"':
                if text[j] == "\\":
                    j += 1
                buf.append(text[j])
                j += 1
            out.append("".join(buf))
            i = j + 1
        else:
            j = text.find(", ", i)
            j = len(text) if j < 0 else j
            out.append(text[i:j])
            i = j
        if text.startswith(", ", i):
            i += 2
    return out


def reader_problems(m):
    """READER: a case the YAML reader and the line reader read otherwise."""
    lines_ = {w["word"]: w for w in line_cases()}
    out = []
    for w in m["words"]:
        got = lines_.get(w["word"])
        if got is None:
            out.append("READER: " + w["word"] + " is not read by the line reader")
        elif "skip" in w:
            if "skip" not in got:
                out.append("READER: " + w["word"] + " is SKIP, the line reader reads cases")
        elif [[str(x) for x in c] for c in w["cases"]] != got["cases"] or \
                {str(k): str(v) for k, v in (w.get("known") or {}).items()} != got["known"]:
            out.append("READER: " + w["word"] + "'s cases or known rows read otherwise by the line reader")
    if len(lines_) != len(m["words"]):
        out.append("READER: the line reader reads " + str(len(lines_)) + " words, YAML " + str(len(m["words"])))
    return out


def library_tags():
    """Each word's Words.gsQuant tag in the library: the gs-quant module.function it names, DataCube's, none, or
    absent (a word's overloads carry one tag between them)."""
    src = open(LIBRARY, encoding="utf-8").read()
    tags = {}
    # a function's annotation block: braces only inside its quoted values, so a match never runs across functions
    block = r"function\s+(?:<<[^>]*>>\s*)?\{((?:[^{}']|'(?:[^'\\]|\\.)*')*)\}\s*timeseries::line::(\w+)<"
    for m in re.finditer(block, src):
        g = re.search(r"Words\.gsQuant = '((?:[^'\\]|\\.)*)'", m.group(1))
        word = m.group(2).rstrip("_")
        if g:
            tags.setdefault(word, set()).add(g.group(1).replace("\\'", "'"))
    return tags


def word_problems(m):
    """WORDS: the mapping against the library's words (its timeseries::line:: functions, which WORDS.md is generated
    from): every word a row, and a row that is no word only where it names its ruling (`ruled:`, as wavg and wsum)."""
    src = open(LIBRARY, encoding="utf-8").read()
    words = {n.rstrip("_") for n in re.findall(r"timeseries::line::(\w+)<", src)}
    rows = {w["word"]: w for w in m["words"]}
    out = ["WORDS: " + w + " is a word of the library with no row in mapping.yaml" for w in sorted(words - set(rows))]
    out += ["WORDS: " + w + " has a row but is no word of the library, and names no ruling (ruled:)"
            for w in sorted(set(rows) - words) if "ruled" not in rows[w]]
    return out


def tag_problems(m):
    """TAG: a MAPPED word whose gs is not its library tag's function, or a SKIP word the library names a twin for --
    but for a word whose `tag:` says why."""
    tags = library_tags()
    out = []
    for w in m["words"]:
        if "tag" in w:
            continue
        named = {t for tag in tags.get(w["word"], set()) for t in tag.split(", ") if re.fullmatch(r"[a-z]+\.\w+", t)}
        if "skip" in w:
            if named:
                out.append("TAG: " + w["word"] + " is SKIP, its library tag names " + ", ".join(sorted(named)))
        elif w["gs"] not in named:
            out.append("TAG: " + w["word"] + " maps to " + w["gs"] + ", its library tag is "
                       + (", ".join(sorted(tags.get(w["word"], set()))) or "absent"))
    return out


# ------------------------------------------------------------------ the data


def samples(path):
    with open(path, encoding="utf-8") as f:
        text = f.read()
    return json.loads(text), hashlib.sha256(text.encode("utf-8")).hexdigest()


def lines(data, x_symbol=X, y_symbol=Y):
    """The cases' lines as pandas Series over their dates: x, y, cpi, and the prices frame."""
    import pandas as pd
    # The columns a case reads, by NAME: a price file may carry others (a fixture's trailing `seq`, its ingest order),
    # which gs-quant never sees.
    px = pd.read_csv(io.StringIO(data["prices"]), usecols=lambda c: c in ("symbol", "date", "close", "seq"),
                     parse_dates=["date"])
    # SORTED BY (symbol, date) BEFORE gs-quant SEES IT: a fixture's prices.csv is in FILE order, not date order (its
    # late_print trap holds a row after a later date's, as a late print arrives), and gs-quant counts observations by
    # position in the series as it is handed in (x.shift(1), x.rolling(w): register S149). A line is ordered by its
    # dates, never by arrival, so the frame is sorted first -- stably (mergesort), so two rows of one symbol on one date
    # (the duplicate_row trap) keep their file order between them, and gs-quant answers over them as it would. Where
    # the file carries `seq`, that file order is its ingest order: held below, never assumed.
    px = px.sort_values(["symbol", "date"], kind="mergesort").reset_index(drop=True)
    if "seq" in px.columns:
        same_day = (px["symbol"] == px["symbol"].shift(1)) & (px["date"] == px["date"].shift(1))
        out_of_order = px[same_day & (px["seq"] <= px["seq"].shift(1))]
        if not out_of_order.empty:
            raise SystemExit("SEQ: rows of one symbol on one date out of their ingest order in the file: "
                             + ", ".join(r.symbol + " " + r.date.date().isoformat() for r in out_of_order.itertuples()))
        px = px.drop(columns=["seq"])
    # each line's dates that hold more than one row (a fixture's duplicate_row: a print and its correction), for the
    # ledger's dup-span note
    counts = px.groupby(["symbol", "date"]).size()
    dups = {}
    for (symbol, date), n in counts.items():
        if n > 1:
            dups.setdefault(symbol, []).append(date.date().isoformat())

    def line(symbol):
        rows = px[px["symbol"] == symbol]
        return pd.Series(rows["close"].astype(float).values, index=pd.DatetimeIndex(rows["date"]), name=symbol)

    env = {"x": line(x_symbol), "y": line(y_symbol), "px": px, "dups": dups}
    if "macro" in data:
        macro = pd.read_csv(io.StringIO(data["macro"]), parse_dates=["date"])
        rows = macro[macro["series"] == MACRO]
        rows = rows.sort_values("date", kind="mergesort")
        env["cpi"] = pd.Series(rows["value"].astype(float).values, index=pd.DatetimeIndex(rows["date"]), name=MACRO)
    return env


def namespace():
    """What a case's gs text reads: gs_quant.timeseries as ts, its enums, Window, dt."""
    import gs_quant.timeseries as ts
    from gs_quant.timeseries.algebra import FilterOperator
    from gs_quant.timeseries.analysis import LagMode, SignDirection, ThresholdType
    from gs_quant.timeseries.backtesting import RebalFreq
    from gs_quant.timeseries.helper import CurveType, Interpolate, Returns, Window
    from gs_quant.timeseries.technicals import Frequency, SeasonalModel
    return {"ts": ts, "Window": Window, "Returns": Returns, "Interpolate": Interpolate, "CurveType": CurveType,
            "LagMode": LagMode, "SignDirection": SignDirection, "ThresholdType": ThresholdType,
            "FilterOperator": FilterOperator, "RebalFreq": RebalFreq, "Frequency": Frequency,
            "SeasonalModel": SeasonalModel, "dt": dt}


def symbol_of(gs_text, env):
    """The lines a case reads, as its rows name them: x's symbol, then y's, or the macro series."""
    names = [n for n in ("cpi", "x", "y") if re.search(r"\b" + n + r"\b", gs_text)]
    return ",".join({"x": env["x"].name, "y": env["y"].name, "cpi": MACRO}[n] for n in names)


def spine(gs_text, env):
    """A case's spine: the dates of the lines it reads (x, y, cpi), their sorted union -- what a row's values are
    laid along (TsGsquantParityTest.spine computes the same from the samples)."""
    names = [n for n in ("cpi", "x", "y") if re.search(r"\b" + n + r"\b", gs_text)]
    return sorted({d.date().isoformat() for n in names for d in env[n].index})


def series(points, along):
    """A case's answer as its row holds it: its values in date order along the spine from its first date to its last
    (null where it has none), and dates [first, last, count] -- or, where a date of the answer is off the spine
    (gs-quant's lag of a calendar period moves each date: 2020-02-01, a Saturday) or holds two values (a line with two
    rows on one date), the answer's own dates and values, in gs-quant's order, every point kept."""
    if not points:
        return [None, None, 0], []
    by = dict(points)
    dates = sorted(by)
    on = set(along)
    if not repeated(points) and all(d in on for d in dates):
        span = along[along.index(dates[0]):along.index(dates[-1]) + 1]
        return [dates[0], dates[-1], len(span)], [by.get(d) for d in span]
    ordered = sorted(points, key=lambda p: p[0])   # stable: a date's values keep gs-quant's order between them
    return [d for d, _ in ordered], [v for _, v in ordered]


def dup_span(gs_text, env, dates):
    """dup-span where a line the case reads has a date holding two rows and the series covers it: the date is on or
    after the line's first and on or before the series' last (a window to that date reads it)."""
    if not dates or dates[0] is None:
        return "-"
    last = dates[1] if len(dates) == 3 and isinstance(dates[2], int) else dates[-1]
    for n in ("x", "y"):
        if re.search(r"\b" + n + r"\b", gs_text):
            line = env[n]
            first = line.index[0].date().isoformat() if len(line) else None
            if any(first is not None and first <= d <= last for d in env["dups"].get(line.name, [])):
                return "dup-span"
    return "-"


def digest(dates, values):
    """A series' sha256, as the ledger's digest column holds it: its row's dates and values, written canonically."""
    text = json.dumps({"dates": dates, "values": values}, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def value_text(v):
    """A point's value as the rows hold it: a float, or the text of one that is not finite (pandas' NaN is no
    value and is not written)."""
    if math.isinf(v):
        return "Inf" if v > 0 else "-Inf"
    return float(v)


def run_case(gs_text, column, env, ns):
    """gs-quant's answer for one case: its points as (date, value) in gs-quant's own order -- EVERY point, so a line
    with two rows on one date answers two values there where gs-quant does (keyed by date, the second would overwrite
    the first) -- or raises. A number is gs-quant's one answer on every date of x (percentile with no window, value); a
    DataFrame's column is picked by position (0 lower, 1 upper)."""
    import pandas as pd
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        got = eval(gs_text, dict(ns), dict(env))   # noqa: S307 -- the mapping's own texts, this repository's file
    if isinstance(got, pd.DataFrame):
        got = got.iloc[:, {"lower": 0, "upper": 1}[column]]
    elif column is not None:
        raise ValueError("a column (" + column + ") of gs-quant's " + type(got).__name__)
    if isinstance(got, pd.Series):
        out = []
        for d, v in got.items():
            if v is None or (isinstance(v, float) and math.isnan(v)):
                continue
            out.append((pd.Timestamp(d).date().isoformat(), value_text(float(v))))
        return out
    if got is None:
        return []
    v = float(got)
    return [] if math.isnan(v) else [(pd.Timestamp(d).date().isoformat(), value_text(v)) for d in env["x"].index]


def raised_at(e):
    """Where gs-quant raised: the innermost frame of its traceback inside gs_quant, as file:line under
    gs_quant/timeseries/ (econometrics.py:497 for 'multiple data points on same date' at the pin)."""
    import traceback
    frames = [f for f in traceback.extract_tb(e.__traceback__) if os.sep + "gs_quant" + os.sep in f.filename]
    if not frames:
        return None
    f = frames[-1]
    path = f.filename.split(os.sep + "gs_quant" + os.sep, 1)[1]
    return (path[len("timeseries" + os.sep):] if path.startswith("timeseries" + os.sep) else path) + ":" + str(f.lineno)


def value_counts(points, gs_text, env):
    """gs-quant's count of values on each date worth saying, in date order: every date a line the case reads holds two
    rows on (0, 1, 2 or 4 values there), and every other date gs-quant answers more than one value on (a calendar lag
    moves a duplicate's date)."""
    dates = set(repeated(points))
    for n in ("x", "y"):
        if re.search(r"\b" + n + r"\b", gs_text):
            dates |= set(env["dups"].get(env[n].name, []))
    counts = {}
    for d, _ in points:
        counts[d] = counts.get(d, 0) + 1
    return {d: counts.get(d, 0) for d in sorted(dates)}


def repeated(points):
    """The dates gs-quant answers more than one value on, each with its count, in date order."""
    counts = {}
    for d, _ in points:
        counts[d] = counts.get(d, 0) + 1
    return {d: n for d, n in sorted(counts.items()) if n > 1}


def sha(ts_text, gs_text, column, data_sha, version):
    """What a case's answer is for: its two texts, its column, the samples and gs-quant's release (the pin); a change
    to any makes the answer STALE (the parity test computes the same)."""
    parts = [ts_text, gs_text, column or "", data_sha, version]
    return hashlib.sha256("\n\x00\n".join(parts).encode("utf-8")).hexdigest()


# ------------------------------------------------------------------ the files


def read_answers(path, fixture=False):
    meta, points = None, {}
    if not os.path.exists(path):
        return meta, points
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            o = json.loads(line)
            if "meta" in o:
                meta = o["meta"]
            else:
                points[row_key(o, fixture)] = o
    return meta, points


def write_answers(path, meta, rows):
    """The answers, gzipped with no name and no time in its header (mtime 0), so a re-snap of the same answers is the
    same bytes."""
    with open(path, "wb") as raw, gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0, compresslevel=9) as z:
        z.write((json.dumps({"meta": meta}, separators=(",", ":"), sort_keys=True) + "\n").encode("utf-8"))
        for r in rows:
            z.write((json.dumps(r, separators=(",", ":")) + "\n").encode("utf-8"))


def read_ledger(path):
    """The ledger's rows, each its cells in COLUMNS' order (read by the header's names, so a ledger of fewer columns
    reads as far as it goes), and the pin its header names (None for a ledger written before the pin)."""
    out, pinned, names = {}, None, None
    if not os.path.exists(path):
        return out, pinned
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            m = PIN_LINE.match(line)
            if m:
                pinned = m.group(1)
            if line.startswith("#") or not line.strip():
                continue
            cells = line.split("\t")
            if names is None:
                names = cells
                if tuple(names) != COLUMNS:
                    raise SystemExit("LEDGER: " + path + "'s columns are " + repr(names) + ", not " + repr(COLUMNS))
                continue
            if len(cells) != len(COLUMNS):
                raise SystemExit("LEDGER: " + path + ": a row of " + str(len(cells)) + " cells: " + cells[0])
            out[cells[0]] = cells
    return out, pinned


def write_ledger(path, rows, version):
    with open(path, "w", encoding="utf-8") as f:
        f.write(LEDGER_HEADER.format(pin=version))
        for r in rows:
            f.write("\t".join(r) + "\n")


def known(word_entry, args):
    return (word_entry.get("known") or {}).get(args, "-") if args is not None else "-"


# ------------------------------------------------------------------ the fixture (CLAUDE.local.md's contract)


# An adversarial fixture's two price files, each snapped and ledgered on its own: the raw prints and the same rows
# with the split applied (the fixture's split_2_for_1 trap).
FIXTURE_FILES = (("adversarial-raw", "prices.csv"), ("adversarial-adjusted", "prices_adjusted.csv"))


def fixture_data(fixture_dir, file_name):
    """One price file of an adversarial fixture as the samples' shape (symbol, date, open, high, low, close, volume:
    the prices sample), and the distinct symbols of its manifest.yaml's traps, each run as x."""
    import yaml
    with open(os.path.join(fixture_dir, file_name), encoding="utf-8") as f:
        text = f.read()
    with open(os.path.join(fixture_dir, "manifest.yaml"), encoding="utf-8") as f:
        manifest = yaml.safe_load(f)
    symbols = list(dict.fromkeys(str(t["symbol"]) for t in manifest["traps"]))
    return {"prices": text}, hashlib.sha256(text.encode("utf-8")).hexdigest(), symbols


def fixture_y(symbol):
    """The second line of a pair over the fixture: ALPHA, or BRAVO beside ALPHA."""
    return "BRAVO" if symbol == "ALPHA" else "ALPHA"


def case_key(word, args, at=None):
    """A case's key in the answers and the ledger: `<word> <args>`, and over a fixture ` @<x's symbol>`."""
    return word + " " + args + ("" if at is None else " @" + at)


def row_key(row, fixture):
    return case_key(row["word"], row["args"], row["symbol"].split(",")[0] if fixture else None)


# ------------------------------------------------------------------ main


def snap(selected, env, data_sha, ns, version, at=None):
    """Every selected case run over env: its rows, its ledger cells and its meta entries."""
    rows, ledger, meta_cases = [], {}, {}
    for _, w, args, ts_text, gs_text, column in selected:
        if args is None:
            key = w["word"] if at is None else w["word"] + " @" + at
            ledger[key] = [key, "SKIP", "SKIP", "-", "-", "-", w["skip"]]
            continue
        key = case_key(w["word"], args, at)
        if at is not None and (ts_text.startswith("WITH") or "cpi" in gs_text):
            ledger[key] = [key, "SKIP", "SKIP", "-", "-", "-", "a whole statement over the samples' own lines (NVDA,"
                           " SPX, CPI): not run over the fixture"]
            continue
        case_sha = sha(ts_text, gs_text, column, data_sha + ("" if at is None else "@" + at), version)
        try:
            points = run_case(gs_text, column, env, ns)
        except Exception as e:   # noqa: BLE001 -- gs-quant's error is the case's answer; any other is this tool's
            at_line = raised_at(e)
            if at_line is None:
                # raised by no frame of gs-quant: the mapping's text or this tool is wrong, never a GS_ERROR
                raise SystemExit("HARNESS: " + key + ": " + type(e).__name__ + ": " + str(e).split("\n")[0]
                                 + " -- raised outside gs-quant") from e
            message = (type(e).__name__ + ": " + str(e)).split("\n")[0][:300] + " (" + at_line + ")"
            meta_cases[key] = {"sha": case_sha, "status": "GS_ERROR", "error": message}
            ledger[key] = [key, "GS_ERROR", "GS_ERROR", known(w, args), "-", "-", message]
            continue
        dates, values = series(points, spine(gs_text, env))
        rows.append({"word": w["word"], "args": args, "symbol": symbol_of(gs_text, env), "dates": dates,
                     "values": values})
        meta_cases[key] = {"sha": case_sha, "status": "OK", "points": len(points)}
        counted = value_counts(points, gs_text, env)
        ledger[key] = [key, "OK", "-", known(w, args), digest(dates, values), dup_span(gs_text, env, dates),
                       str(len(points)) + " points"
                       + "".join(", " + str(n) + " values on " + d for d, n in counted.items())
                       + ("" if len(dates) == 3 and isinstance(dates[2], int) else
                          "" if repeated(points) else ", off its spine: its own dates")]
    return rows, ledger, meta_cases


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--only", default=None, help="a glob over case keys, `<word> <args>` (over a fixture, ` @<symbol>`)")
    ap.add_argument("--samples", default=SAMPLES)
    ap.add_argument("--fixture", default=None, help="an adversarial fixture's directory (prices.csv, prices_adjusted.csv,"
                    " manifest.yaml): each price file its own answers and ledger, and traps.tsv")
    ap.add_argument("--release-diff", default=None, metavar="PYTHON",
                    help="an interpreter with another gs-quant: write release-diff.tsv, each mapped definition there"
                         " against the pin's")
    args = ap.parse_args()
    if not (args.write or args.check or args.release_diff):
        ap.error("--write, --check or --release-diff")
    values = pins()
    wrong = installed_problems(values)
    if wrong:
        raise SystemExit("\n".join(wrong))
    version = stack(values)
    m = mapping()
    if args.release_diff:
        return release_diff(m, args.release_diff, values)
    if args.fixture:
        datasets = []
        for name, file_name in FIXTURE_FILES:
            if not os.path.exists(os.path.join(args.fixture, file_name)):
                # never a dataset passed over: a --check that skipped one would say nothing of it
                raise SystemExit("MISSING: " + os.path.join(args.fixture, file_name) + " (" + name + ")")
            data, data_sha, symbols = fixture_data(args.fixture, file_name)
            runs = [(s_, lines(data, x_symbol=s_, y_symbol=fixture_y(s_))) for s_ in symbols]
            where = os.path.relpath(os.path.abspath(os.path.join(args.fixture, file_name)), ROOT)
            datasets.append(("-" + name, data_sha, runs, where))
    else:
        data, data_sha = samples(args.samples)
        datasets = [("", data_sha, [(None, lines(data))], None)]
    status = 0
    for suffix, data_sha, runs, where in datasets:
        if suffix:
            print("--", suffix[1:], "(" + where + ")")
        status |= one(m, args, suffix, data_sha, runs, where, version)
    if args.fixture:
        status |= traps(m, args.fixture, args.write)
    return status


# ------------------------------------------------------------------ the fixture's traps gs-quant answers


TRAPS = os.path.join(HERE, "traps.tsv")


def traps(m, fixture_dir, write):
    """traps.tsv: each trap of the fixture's manifest that mapping.yaml's `traps:` names a case for, read from each
    price file's answers on the trap's date, against the manifest's correct answer. --write writes it; --check holds
    the file to a fresh reading (TRAPS)."""
    import yaml
    manifest = {t["name"]: t for t in yaml.safe_load(open(os.path.join(fixture_dir, "manifest.yaml")))["traps"]}
    lines = ["# THE TRAPS gs-quant answers (snap.py --fixture, from the answers and the fixture's manifest): each trap"
             " mapping.yaml's traps: names, its case, each price file's value(s) on the trap's date, the manifest's"
             " correct answer, and whether they agree (exactly, or within 1e-8 relative). GENERATED.",
             "trap	case	file	date	gs-quant	correct	agree"]
    for name, t in m["traps"].items():
        if name not in manifest:
            raise SystemExit("TRAPS: mapping.yaml names " + name + ", no trap of the manifest")
        want = manifest[name]["correct_answer"]
        key = t["case"] + " @" + t["symbol"]
        for file_name in ("adversarial-raw", "adversarial-adjusted"):
            _, rows = read_answers(os.path.join(HERE, "answers-" + file_name + ".jsonl.gz"), True)
            if key not in rows:
                raise SystemExit("TRAPS: " + name + ": no answer row " + key + " in " + file_name)
            got = values_on(rows[key], fixture_dir, want["date"])
            agree = "-" if want["value"] is None and not got else (
                "yes" if len(got) == 1 and got[0] == want["value"] else
                "within 1e-8" if len(got) == 1 and abs(got[0] - want["value"]) <= 1e-8 * abs(want["value"]) else "NO")
            lines.append("\t".join([name, key, file_name, want["date"], ",".join(repr(v) for v in got) or "-",
                                    repr(want["value"]), agree]))
    text = "\n".join(lines) + "\n"
    if write:
        open(TRAPS, "w", encoding="utf-8").write(text)
        print("wrote", os.path.relpath(TRAPS, ROOT))
        return 0
    if not os.path.exists(TRAPS) or open(TRAPS, encoding="utf-8").read() != text:
        print("TRAPS: " + os.path.relpath(TRAPS, ROOT) + " is not a fresh reading of the answers (snap.sh --write --fixture)")
        return 1
    return 0


def values_on(row, fixture_dir, date):
    """A fixture row's values on one date: its spine from the price file it was snapped from (its symbol's lines'
    dates), as the parity test reads it."""
    import csv
    if len(row["dates"]) == 3 and isinstance(row["dates"][2], int):
        first, last, count = row["dates"]
        if first is None:
            return []
        lines = row["symbol"].split(",")
        spine = sorted({r["date"] for r in csv.DictReader(open(os.path.join(fixture_dir, "prices.csv")))
                        if r["symbol"] in lines})
        along = spine[spine.index(first):spine.index(last) + 1]
        if len(along) != count:
            raise SystemExit("TRAPS: a row's spine does not hold its count: " + row["word"] + " " + row["args"])
    else:
        along = row["dates"]
    return [v for d, v in zip(along, row["values"]) if d == date and v is not None]


# ------------------------------------------------------------------ release-diff.tsv: the mapped definitions, two releases


RELEASE_DIFF = os.path.join(HERE, "release-diff.tsv")


def definitions(root, fns):
    """Each mapped module.function's definition in the gs_quant tree at root: its AST's sha256 and its line."""
    import ast
    out = {}
    trees = {}
    for f in fns:
        mod, name = f.split(".")
        if mod not in trees:
            trees[mod] = ast.parse(open(os.path.join(root, "timeseries", mod + ".py"), encoding="utf-8").read())
        node = next((n for n in trees[mod].body if isinstance(n, ast.FunctionDef) and n.name == name), None)
        out[f] = None if node is None else (hashlib.sha256(ast.dump(node).encode("utf-8")).hexdigest(),
                                            mod + ".py:" + str(node.lineno))
    return out


def release_diff(m, other_python, values):
    """release-diff.tsv: each definition mapping.yaml calls, at the pin and at another release (another interpreter's
    gs-quant), its AST's sha256 each side and whether they are one -- committed, so the claim that a release moved no
    definition used here is a file, not a session's memory."""
    import subprocess
    import gs_quant
    here_root = os.path.dirname(gs_quant.__file__)
    probe = "import gs_quant, os; print(gs_quant.__version__); print(os.path.dirname(gs_quant.__file__))"
    other_version, other_root = subprocess.run([other_python, "-I", "-c", probe], capture_output=True, text=True,
                                               check=True).stdout.split("\n")[:2]
    called = set()
    for w in m["words"]:
        for c in w.get("cases", []):
            for f in re.findall(r"\bts\.(\w+)\(", c[2]):
                called.add(f)
    import gs_quant.timeseries as ts
    fns = sorted({inspect_module(ts, f) + "." + f for f in called})
    pin_defs, other_defs = definitions(here_root, fns), definitions(other_root, fns)
    lines = ["# THE MAPPED DEFINITIONS AT TWO RELEASES (snap.py --release-diff): every gs_quant.timeseries function a"
             " case of mapping.yaml calls, its AST's sha256 at the pin and at gs-quant " + other_version + ". GENERATED.",
             "definition	at the pin (" + values["GS_QUANT_VERSION"] + ")	line	at " + other_version + "	same"]
    for f in fns:
        a, b = pin_defs[f], other_defs[f]
        lines.append("\t".join([f, a[0] if a else "-", a[1] if a else "-", b[0] if b else "-",
                                "yes" if a and b and a[0] == b[0] else "NO"]))
    open(RELEASE_DIFF, "w", encoding="utf-8").write("\n".join(lines) + "\n")
    moved = [l.split("\t")[0] for l in lines[2:] if l.endswith("\tNO")]
    print("wrote", os.path.relpath(RELEASE_DIFF, ROOT), "-", len(fns), "definitions;", "changed:", moved or "none")
    return 0


def inspect_module(ts, name):
    """The gs_quant.timeseries module a function is defined in (statistics, econometrics, ...)."""
    import inspect
    return inspect.getmodule(inspect.unwrap(getattr(ts, name))).__name__.rsplit(".", 1)[1]


def one(m, args, suffix, data_sha, runs, where, version):
    """One dataset's snap or check: the samples (no suffix) or one of a fixture's price files."""
    ns = namespace()
    fixture = where is not None
    answers_file = os.path.join(HERE, "answers" + suffix + ".jsonl.gz")
    ledger_file = os.path.join(HERE, "ledger" + suffix + ".tsv")
    rows, ledger, meta_cases = [], {}, {}
    order = []
    for at, env in runs:
        r, l, mc = snap(cases(m), env, data_sha, ns, version, at)
        if args.only:
            l = {k: v for k, v in l.items() if fnmatch.fnmatchcase(k, args.only)}
            mc = {k: v for k, v in mc.items() if k in l}
            r = [row for row in r if row_key(row, fixture) in l]
        rows += r
        ledger.update(l)
        meta_cases.update(mc)
        order += list(l)
    meta = {"tool": "tools/gs-quant-oracle/snap.py", "format": FORMAT, "pins": version, "data": data_sha,
            "fixture": where, "cases": meta_cases}
    old_meta, old_points = read_answers(answers_file, fixture)
    old_ledger, ledger_pin = read_ledger(ledger_file)
    if args.write:
        if args.only and old_meta:
            # a glob's cases merged into the file: every other case's rows and entries kept as they are
            meta["cases"] = {**{k: v for k, v in old_meta.get("cases", {}).items() if k not in meta_cases},
                             **meta_cases}
            rows = [r for k, r in old_points.items() if k not in meta_cases] + rows
            order = list(dict.fromkeys(list(old_ledger) + order))
        rank = {k: i for i, k in enumerate(order)}
        rows.sort(key=lambda r: rank.get(row_key(r, fixture), 1 << 30))
        write_answers(answers_file, meta, rows)
        out = []
        for key in order:
            cells = ledger.get(key) or old_ledger.get(key)
            was = old_ledger.get(key)
            if key in ledger and was is not None and cells[1] == "OK" and was[1] == "OK":
                cells[2] = was[2]   # the parity test's verdict, kept while gs-quant's side stands
                if was[2] in ("DIFF", "REFUSED") and LITE in was[6]:
                    # `why` is two halves: gs-quant's (this tool's, rewritten) and lite's (the test's, kept)
                    cells[6] = cells[6] + was[6][was[6].index(LITE):]
                if was[2] == "PASS":
                    cells[3] = "-"   # a register row rules a DIFF; a case that passes names none (as the test writes)
            out.append(cells)
        write_ledger(ledger_file, out, version)
        tally = {}
        for cells in out:
            tally[cells[1]] = tally.get(cells[1], 0) + 1
        print("wrote", os.path.relpath(answers_file, ROOT), "-", len(rows), "series;", os.path.relpath(ledger_file, ROOT),
              "-", len(out), "cases:", ", ".join(k + " " + str(v) for k, v in sorted(tally.items())))
        guards = tag_problems(m) + reader_problems(m) + word_problems(m)
        for p in guards:
            print(p)
        return 1 if guards else 0
    # --check: the live answers against the committed file
    problems = tag_problems(m) + reader_problems(m) + word_problems(m)
    if old_meta is None:
        problems.append("UNSNAPPED: no " + os.path.relpath(answers_file, ROOT))
        old_meta = {"cases": {}}
    if old_meta.get("pins") != version:
        problems.append("STALE: " + os.path.relpath(answers_file, ROOT) + " was snapped under " + str(old_meta.get("pins"))
                        + ", the pins are " + version)
    if ledger_pin != version:
        problems.append("STALE: " + os.path.relpath(ledger_file, ROOT) + "'s header names " + str(ledger_pin)
                        + ", the pins are " + version)
    live = {row_key(r, fixture): digest(r["dates"], r["values"]) for r in rows}
    for key, entry in meta_cases.items():
        was = old_meta["cases"].get(key)
        if was is None:
            problems.append("UNSNAPPED: " + key)
        elif was["sha"] != entry["sha"]:
            problems.append("STALE: " + key)
        elif was["status"] != entry["status"] or was.get("error") != entry.get("error"):
            problems.append("DRIFT: " + key + ": " + was["status"] + " then, " + entry["status"] + " now")
        elif entry["status"] == "OK" and (old_ledger.get(key) or [""] * 7)[4] != live[key]:
            # the ledger's digest alone: a live series that is not the one snapped
            problems.append("DRIFT: " + key + " (its series' digest is not the ledger's)")
    for key in old_meta["cases"]:
        if (not args.only or fnmatch.fnmatchcase(key, args.only)) and key not in meta_cases:
            problems.append("GONE: " + key)
    for p in problems:
        print(p)
    print(len(problems), "problems over", len(meta_cases), "cases")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
