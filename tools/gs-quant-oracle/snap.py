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
import functools
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
                 ("SCIPY_VERSION", "scipy"), ("STATSMODELS_VERSION", "statsmodels"), ("TA_LIB_VERSION", "ta-lib"))


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


#: a case's gs text that is Show Python's call: its #TS item as the printer prints it, read from calls.tsv
PRINTED = "printed"
CALLS = os.path.join(HERE, "calls.tsv")


def printed_calls(path=CALLS):
    """calls.tsv (//datacube:timeseries_builder_test writes it, item 14f step 2): each case's key to its #TS item as
    Show Python prints it -- the gs-quant call -- or STATEMENT (a whole statement, no series the printer prints) or
    `REFUSED: why` (no call of gs-quant's says it)."""
    out = {}
    for line in open(path, encoding="utf-8").read().split("\n"):
        if not line or line.startswith("#") or line.startswith("case\t"):
            continue
        key, call = line.split("\t", 1)
        out[key] = call
    return out


def cases(m, calls=None):
    """Every case of the mapping, in its order: (key, word entry, args, ts, gs, column) -- a SKIP word one case
    keyed by its word alone, with neither text. A case whose gs is `printed` takes Show Python's call (calls.tsv): the
    oracle's gs-quant side the printer's, so the code the tile shows is the code gs-quant answers."""
    calls = printed_calls() if calls is None else calls
    out = []
    for w in m["words"]:
        if "skip" in w:
            out.append((w["word"], w, None, None, None, None))
            continue
        for c in w["cases"]:
            args, ts_text, gs_text = str(c[0]), c[1], c[2]
            column = c[3] if len(c) > 3 else None
            key = w["word"] + " " + args
            if gs_text == PRINTED:
                call = calls.get(key)
                if call is None or call == "STATEMENT" or call.startswith("REFUSED"):
                    raise SystemExit("PRINTED: " + key + " is `printed`, and calls.tsv holds " + (call or "no row for it")
                                     + " (rewrite calls.tsv: //datacube:timeseries_builder_test says how; or write its call)")
                gs_text = call
            out.append((key, w, args, ts_text, gs_text, column))
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
                # a key with a comma is double-quoted (YAML's flow map splits a plain one there)
                words[-1]["known"][k[1:-1] if k.startswith('"') and k.endswith('"') else k] = v
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
        if "tag" in w or "ref" in w:
            # a word a reference answers (ref:) has no gs-quant function to hold to its tag: its tag is DataCube's
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
    px = pd.read_csv(io.StringIO(data["prices"]), usecols=lambda c: c in ("symbol", "date", "seq") + PRICE_COLUMNS,
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

    def line(symbol, column="close"):
        rows = px[px["symbol"] == symbol]
        return pd.Series(rows[column].astype(float).values, index=pd.DatetimeIndex(rows["date"]), name=symbol)

    env = {"x": line(x_symbol), "y": line(y_symbol), "px": px, "dups": dups}
    # each price column of x's and y's symbols (a reference word reads a high, a low, a volume: item 14f, step 4)
    env["cols"] = {(c, n): (env[n] if c == "close" else line(s, c)) for c in PRICE_COLUMNS if c in px.columns
                   for n, s in (("x", x_symbol), ("y", y_symbol))}
    if "macro" in data:
        macro = pd.read_csv(io.StringIO(data["macro"]), parse_dates=["date"])
        rows = macro[macro["series"] == MACRO]
        rows = rows.sort_values("date", kind="mergesort")
        env["cpi"] = pd.Series(rows["value"].astype(float).values, index=pd.DatetimeIndex(rows["date"]), name=MACRO)
    return env


def references():
    """references.py, beside this tool (an isolated interpreter puts no script's directory on the path)."""
    if HERE not in sys.path:
        sys.path.insert(0, HERE)
    import references as refs
    return refs


def namespace():
    """What a case's gs text reads: gs_quant.timeseries as ts, its enums, Window, dt -- and, for Show Python's calls,
    each of gs_quant.timeseries' functions by its own name (the printer writes gs-quant's names: mean, sum_, if_) and
    DataCube's own words that are a gs-quant call by identity (their library tag `DataCube (<the call>)`, ruled)."""
    import gs_quant.timeseries as ts
    from gs_quant.timeseries.algebra import FilterOperator
    from gs_quant.timeseries.analysis import LagMode, SignDirection, ThresholdType
    from gs_quant.timeseries.backtesting import RebalFreq
    from gs_quant.timeseries.helper import CurveType, Interpolate, Returns, Window
    from gs_quant.timeseries.technicals import Frequency, SeasonalModel
    ns = {name: getattr(ts, name) for name in dir(ts) if not name.startswith("_")}
    # rebase(x, base) is index(x, base): rebase's tag, `DataCube (econometrics.index)`, ruled 2026-11-07 (mapping.yaml)
    ns["rebase"] = lambda x, base=100: ts.index(x, base)
    # DataCube's own words, each by its reference (references.py: TA-Lib, statsmodels, pandas; item 14f, step 4) --
    # over gs-quant's names, where gs-quant has one of no function over series (alpha: a Marquee report's read)
    ns.update(references().WORDS)
    ns.update({"ts": ts, "Window": Window, "Returns": Returns, "Interpolate": Interpolate, "CurveType": CurveType,
               "LagMode": LagMode, "SignDirection": SignDirection, "ThresholdType": ThresholdType,
               "FilterOperator": FilterOperator, "RebalFreq": RebalFreq, "Frequency": Frequency,
               "SeasonalModel": SeasonalModel, "dt": dt})
    return ns


#: Show Python's picks of the samples' prices (calls.tsv prints {x} as close['NVDA'], {x.high} as high['NVDA'], {y} as
#: close['SPX']) and the lines they are in a run: x's and y's symbols (over a fixture, its trap symbol and that symbol's y)
PICKS = {"NVDA": "x", "SPX": "y"}
#: the prices' columns a case reads of its symbols: close its x and y themselves, the others a reference word's
PRICE_COLUMNS = ("open", "high", "low", "close", "volume")


def column_of(env):
    """Show Python's column(name, pick) over a run's lines: a price column of NVDA the case's x symbol's, of SPX its
    y's. Any other is this tool's error (HARNESS: raised outside gs-quant), never gs-quant's."""
    def column(name, *picks):
        if name not in PRICE_COLUMNS or len(picks) != 1 or picks[0] not in PICKS or (name, PICKS[picks[0]]) not in env["cols"]:
            raise ValueError("column(" + ", ".join(map(repr, (name,) + picks)) + ") is no line of the oracle's runs")
        return env["cols"][(name, PICKS[picks[0]])]
    return column


def reads(gs_text):
    """The lines a case's call reads, in the order its rows name them (cpi, x, y): a hand call's names, or Show
    Python's column(<a price column>, 'NVDA') (x's symbol) and column(<a price column>, 'SPX') (y's)."""
    if "column(" in gs_text:
        picked = set(re.findall(r"column\('\w+', '(\w+)'\)", gs_text))
        return [n for n in ("x", "y") if any(PICKS.get(p) == n for p in picked)]
    return [n for n in ("cpi", "x", "y") if re.search(r"\b" + n + r"\b", gs_text)]


def symbol_of(gs_text, env):
    """The lines a case reads, as its rows name them: x's symbol, then y's, or the macro series."""
    return ",".join({"x": env["x"].name, "y": env["y"].name, "cpi": MACRO}[n] for n in reads(gs_text))


def spine(gs_text, env):
    """A case's spine: the dates of the lines it reads (x, y, cpi), their sorted union -- what a row's values are
    laid along (TsGsquantParityTest.spine computes the same from the samples)."""
    return sorted({d.date().isoformat() for n in reads(gs_text) for d in env[n].index})


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
        if n in reads(gs_text):
            line = env[n]
            first = line.index[0].date().isoformat() if len(line) else None
            if any(first is not None and first <= d <= last for d in env["dups"].get(line.name, [])):
                return "dup-span"
    return "-"


def digest(dates, values):
    """A series' sha256, as the ledger's digest column holds it: its row's dates and values, written canonically."""
    text = json.dumps({"dates": dates, "values": values}, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


#: how far a live value may sit from the snapped one and be the same answer: a platform's floating point, not
#: gs-quant -- the answers snapped on the contributor's machine and gs-quant at the same pins on Linux x86-64 differ by
#: at most 3.2e-12 of a value (macd; most below 1e-14), measured 2026-11-09 (docs/TS_DECISIONS_2026_11_07.md item 14f).
#: A tenth of the parity test's 1e-9: what moves less is no DRIFT, what the judge could see always is.
DRIFT_TOLERANCE = 1e-10


def drift(live, snapped):
    """Why a live series is not the snapped one, or None where it is, within DRIFT_TOLERANCE of the series' scale (the
    judge's rule for a level: its largest magnitude, at least 1)."""
    if snapped is None:
        return "no snapped series"
    if live["dates"] != snapped["dates"]:
        return "its dates moved"
    if len(live["values"]) != len(snapped["values"]):
        return "its point count moved"
    numbers = [abs(v) for v in snapped["values"] if isinstance(v, float) or isinstance(v, int)]
    scale = max([1.0] + numbers)
    for d, (a, b) in enumerate(zip(live["values"], snapped["values"])):
        if isinstance(a, (int, float)) and isinstance(b, (int, float)):
            if abs(a - b) > DRIFT_TOLERANCE * scale:
                return "a value moved by " + format(abs(a - b), ".3g") + " at its point " + str(d)
        elif a != b:
            return "a value's kind moved at its point " + str(d)
    return None


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
        # the mapping's own texts and the printer's (calls.tsv), this repository's files
        got = eval(gs_text, {**ns, "column": column_of(env)}, dict(env))   # noqa: S307
    if isinstance(got, pd.DataFrame):
        # a reference's frame names its columns (lower, upper, width; open ... close); gs-quant's, by position
        got = got[column] if column in got.columns else got.iloc[:, {"lower": 0, "upper": 1}[column]]
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


# ------------------------------------------------------------------ the gallery (item 14f, step 4b)


#: the gallery's and the markets example's series as gs-quant's calls: //datacube:timeseries_ts_parity_test writes it
GALLERY = os.path.join(HERE, "gallery.jsonl")
#: a gallery case's word: its key `series <view> <id>`, a field's `series <view> <id>.<field>`
GALLERY_WORD = "series"


def gallery_rows(path=GALLERY):
    """gallery.jsonl's rows: each series' key, Show Python's call of it (its other series inlined), its tile's sources,
    its answer's fields (none for a value) and why gs-quant cannot answer it (null: it can)."""
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip() and not line.startswith("#")]


def gallery_known(m):
    """The gallery's cases a register row or a word's own sentence rules a DIFF of: mapping.yaml's `gallery: known:`."""
    return (m.get("gallery") or {}).get("known") or {}


def gallery_frames(data):
    """The pages' samples as frames, by cube (prices, rates, macro, cash)."""
    import pandas as pd
    return {cube: pd.read_csv(io.StringIO(text)) for cube, text in data.items()}


@functools.lru_cache(maxsize=None)
def line_class():
    """A line of a gallery series' run (and every gs-quant answer there, gallery_namespace): a pandas Series whose
    comparisons and `&`, `|` are the program's -- legend_lite.timeseries' Term's, the language's: two lines met on the
    dates both have (S152), where pandas raises on two series of different dates (tools/ts-oracle/calls.py's rule, moved
    here with the gallery) -- and Show Python's `.period(fn)`, each date's value of its rows under the tile's frequency,
    which the gallery reads under none (a frequency is skipped): each date's one row itself (a date of two rows is this
    tool's error). pandas defers its own operators to it (`__pandas_priority__`)."""
    import pandas as pd

    class Line(pd.Series):
        __pandas_priority__ = 5000

        @property
        def _constructor(self):
            return Line

        def period(self, fn):
            if self.index.has_duplicates:
                raise ValueError("a period's " + fn + " over a date of two rows: none of the gallery's samples")
            return self

        def _met(self, other, op):
            if isinstance(other, pd.Series):
                a, b = self.align(other, join="inner")
                return Line(op(pd.Series(a), pd.Series(b)))
            return Line(op(pd.Series(self), other))

        def __lt__(self, other):
            return self._met(other, lambda a, b: a < b)

        def __le__(self, other):
            return self._met(other, lambda a, b: a <= b)

        def __gt__(self, other):
            return self._met(other, lambda a, b: a > b)

        def __ge__(self, other):
            return self._met(other, lambda a, b: a >= b)

        def __eq__(self, other):
            return self._met(other, lambda a, b: a == b)

        def __ne__(self, other):
            return self._met(other, lambda a, b: a != b)

        def __and__(self, other):
            return self._met(other, lambda a, b: (a != 0) & (b != 0))

        def __or__(self, other):
            return self._met(other, lambda a, b: (a != 0) | (b != 0))

        def __rand__(self, other):
            return self.__and__(other)

        def __ror__(self, other):
            return self.__or__(other)

        __hash__ = None

    return Line


def gallery_namespace(ns):
    """The namespace's every function answering a Line where it answers a pandas Series: a gallery series' operators
    meet gs-quant's answers as the language meets lines."""
    import pandas as pd
    line = line_class()

    def lined(f):
        def call(*args, **kwargs):
            got = f(*args, **kwargs)
            return line(got) if isinstance(got, pd.Series) and not isinstance(got, line) else got
        return call
    return {k: lined(v) if callable(v) and not isinstance(v, type) else v for k, v in ns.items()}


def gallery_column(frames, sources, read):
    """Show Python's column(name, pick) over the pages' samples: `alias.column` (the tile's first source's where it names
    none), the pick's rows by its source's key, along its date -- a line ordered by its dates, its missing values none
    (lite's) -- each line read kept in `read` (the dates gs-quant's one number is answered on). A split's pick, a
    period's value and any other are this tool's error (HARNESS), never gs-quant's: the gallery skips their series."""
    import pandas as pd
    by = {s["alias"]: s for s in sources}
    Line = line_class()

    def column(name, *picks):
        alias, col = name.split(".", 1) if "." in name else (sources[0]["alias"], name)
        s = by.get(alias)
        if s is None or len(picks) > 1 or any(isinstance(p, dict) for p in picks) or (s["key"] is None) != (not picks):
            raise ValueError("column(" + ", ".join(map(repr, (name,) + picks)) + ") is no line of the gallery's samples")
        df = frames[s["cube"]]
        rows = df if not picks else df[df[s["key"]].astype(str) == str(picks[0])]
        rows = rows.assign(_on=pd.to_datetime(rows[s["date"]])).sort_values("_on", kind="mergesort")
        values = pd.Series(rows[col].astype(float).values, index=pd.DatetimeIndex(rows["_on"])).dropna()
        line = Line(values.values, index=values.index, name=str(picks[0]) if picks else col)
        read.append(line)
        return line
    return column


def cashed_sharpe_ratio(ns, column):
    """gs-quant's sharpe_ratio with its risk-free curve the page's cash line: gs-quant's own path for a price line
    (econometrics._get_ratio: get_ratio_pure of excess_returns_pure over the currency's spot curve) with the curve that
    excess_returns reads from Marquee (a currency's SharpeAssets cash index, a session's) read from the page instead --
    `cash.index['<currency>']`, the builders' convention (item 13b: RiskFreeRateCurrency.USD is the page's dollar cash).
    Its excess returns' line itself (EXCESS_RETURNS) is gs-quant's as it stands."""
    from gs_quant.timeseries.econometrics import RiskFreeRateCurrency, excess_returns_pure, get_ratio_pure
    from gs_quant.timeseries.helper import CurveType, Interpolate
    line = line_class()

    def sharpe_ratio(series, currency=RiskFreeRateCurrency.USD, w=None, curve_type=CurveType.PRICES,
                     method=Interpolate.NAN):
        er = series if curve_type == CurveType.EXCESS_RETURNS else excess_returns_pure(
            series, column("cash.index", getattr(currency, "value", currency)))
        return line(get_ratio_pure(er, w, method))
    return sharpe_ratio


def run_gallery(row, field, frames, ns):
    """gs-quant's answer for a gallery series (a field of it: a band's lower, a candle's open): its points in gs-quant's
    own order, or None where gs-quant's answer has no such field (its bands no width), or raises."""
    import pandas as pd
    read = []
    column = gallery_column(frames, row["sources"], read)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        # gallery.jsonl's calls, this repository's own file (the printer's)
        got = eval(row["python"], {**ns, "column": column, "sharpe_ratio": cashed_sharpe_ratio(ns, column)}, {})   # noqa: S307
    if isinstance(got, pd.DataFrame):
        # a reference's frame names its columns; gs-quant's bands by position (0 lower, 1 upper), as run_case reads them
        if field in got.columns:
            got = got[field]
        elif field in ("lower", "upper") and got.shape[1] == 2:
            got = got.iloc[:, {"lower": 0, "upper": 1}[field]]
        else:
            return None
    elif field is not None:
        return None
    if isinstance(got, pd.Series):
        return [(pd.Timestamp(d).date().isoformat(), value_text(float(v))) for d, v in got.items()
                if not (v is None or (isinstance(v, float) and math.isnan(v)))]
    if got is None:
        return []
    v = float(got)
    on = sorted({d.date().isoformat() for line in read for d in line.index})
    return [] if math.isnan(v) else [(d, value_text(v)) for d in on]


def snap_gallery(gallery, env, data_sha, ns, version, known_map):
    """Every series of the gallery (each field of a struct its own case) run over the pages' samples: its rows -- each its
    own dates, never a spine -- its ledger cells and its meta entries. A case's sha is over its key, not its #TS: the
    answer is gs-quant's, of its call alone, and the parity test runs the series' Show #TS as it is."""
    rows, ledger, meta_cases = [], {}, {}
    ns = gallery_namespace(ns)
    for g in gallery:
        for field in (g["fields"] or [None]):
            args = g["series"] + ("" if field is None else "." + field)
            key = case_key(GALLERY_WORD, args)
            if g["skip"]:
                ledger[key] = [key, "SKIP", "SKIP", "-", "-", "-", g["skip"]]
                continue
            register = known_map.get(args, "-")
            case_sha = sha(args, g["python"], field, data_sha, version)
            try:
                points = run_gallery(g, field, env["frames"], ns)
            except Exception as e:   # noqa: BLE001 -- gs-quant's error is the case's answer; any other is this tool's
                at_line = raised_at(e)
                if at_line is None:
                    raise SystemExit("HARNESS: " + key + ": " + type(e).__name__ + ": " + str(e).split("\n")[0]
                                     + " -- raised outside gs-quant") from e
                message = (type(e).__name__ + ": " + str(e)).split("\n")[0][:300] + " (" + at_line + ")"
                meta_cases[key] = {"sha": case_sha, "status": "GS_ERROR", "error": message}
                ledger[key] = [key, "GS_ERROR", "GS_ERROR", register, "-", "-", message]
                continue
            if points is None:
                why = "gs-quant's answer has no " + field
                meta_cases[key] = {"sha": case_sha, "status": "SKIP", "error": why}
                ledger[key] = [key, "SKIP", "SKIP", "-", "-", "-", why]
                continue
            dates, values = series(points, [])
            rows.append({"word": GALLERY_WORD, "args": args, "symbol": "-", "dates": dates, "values": values})
            meta_cases[key] = {"sha": case_sha, "status": "OK", "points": len(points)}
            ledger[key] = [key, "OK", "-", register, digest(dates, values), "-", str(len(points)) + " points"]
    return rows, ledger, meta_cases


def raised_at(e):
    """Where gs-quant raised: the innermost frame of its traceback inside gs_quant, as file:line under
    gs_quant/timeseries/ (econometrics.py:497 for 'multiple data points on same date' at the pin)."""
    import traceback
    tb = traceback.extract_tb(e.__traceback__)
    frames = [f for f in tb if os.sep + "gs_quant" + os.sep in f.filename]
    if not frames:
        # a reference's own refusal (references.py and the library it calls: pandas on a line of two rows on one
        # date), its GS_ERROR as gs-quant's is -- the innermost frame of references.py
        own = [f for f in tb if os.path.basename(f.filename) == "references.py"]
        return None if not own else "references.py:" + str(own[-1].lineno)
    f = frames[-1]
    path = f.filename.split(os.sep + "gs_quant" + os.sep, 1)[1]
    return (path[len("timeseries" + os.sep):] if path.startswith("timeseries" + os.sep) else path) + ":" + str(f.lineno)


def value_counts(points, gs_text, env):
    """gs-quant's count of values on each date worth saying, in date order: every date a line the case reads holds two
    rows on (0, 1, 2 or 4 values there), and every other date gs-quant answers more than one value on (a calendar lag
    moves a duplicate's date)."""
    dates = set(repeated(points))
    for n in ("x", "y"):
        if n in reads(gs_text):
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
            datasets.append(("-" + name, data_sha, runs, where, None))
    else:
        data, data_sha = samples(args.samples)
        # the mapping's cases, then the gallery's and the markets example's series (item 14f, step 4b), over the samples
        datasets = [("", data_sha, [(None, lines(data))], None, None),
                    ("-gallery", data_sha, [(None, {"frames": gallery_frames(data)})], None, gallery_rows())]
    status = 0
    for suffix, data_sha, runs, where, gallery in datasets:
        if suffix:
            print("--", suffix[1:] + ("" if where is None else " (" + where + ")"))
        status |= one(m, args, suffix, data_sha, runs, where, version, gallery)
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
    import gs_quant.timeseries as ts
    # every gs_quant.timeseries function a case's call makes: a hand call's ts.f(, or Show Python's own names (calls.tsv,
    # each resolved by cases(); a reference's word is none of gs-quant's)
    called = set()
    refs = references().WORDS
    for _, w, args, _, gs_text, _ in cases(m):
        if args is None:
            continue
        called |= set(re.findall(r"\bts\.(\w+)\(", gs_text))
        called |= {f for f in re.findall(r"(?<![\w.])(\w+)\(", gs_text) if f not in refs and callable(getattr(ts, f, None))
                   and f not in ("Window",)}
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


def one(m, args, suffix, data_sha, runs, where, version, gallery=None):
    """One dataset's snap or check: the samples (no suffix), the gallery's series over them (-gallery: `gallery` its
    rows), or one of a fixture's price files."""
    ns = namespace()
    fixture = where is not None
    answers_file = os.path.join(HERE, "answers" + suffix + ".jsonl.gz")
    ledger_file = os.path.join(HERE, "ledger" + suffix + ".tsv")
    rows, ledger, meta_cases = [], {}, {}
    order = []
    for at, env in runs:
        r, l, mc = (snap(cases(m), env, data_sha, ns, version, at) if gallery is None
                    else snap_gallery(gallery, env, data_sha, ns, version, gallery_known(m)))
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
    live_rows = {row_key(r, fixture): r for r in rows}
    for key, entry in meta_cases.items():
        was = old_meta["cases"].get(key)
        if was is None:
            problems.append("UNSNAPPED: " + key)
        elif was["sha"] != entry["sha"]:
            problems.append("STALE: " + key)
        elif was["status"] != entry["status"] or was.get("error") != entry.get("error"):
            problems.append("DRIFT: " + key + ": " + was["status"] + " then, " + entry["status"] + " now")
        elif entry["status"] == "OK":
            r = live_rows[key]
            if (old_ledger.get(key) or [""] * 7)[4] != digest(r["dates"], r["values"]):
                # not the bytes snapped: DRIFT unless every value is the snapped one within a platform's last bits
                moved = drift(r, old_points.get(key))
                if moved:
                    problems.append("DRIFT: " + key + " (" + moved + ")")
    for key in old_meta["cases"]:
        if (not args.only or fnmatch.fnmatchcase(key, args.only)) and key not in meta_cases:
            problems.append("GONE: " + key)
    for p in problems:
        print(p)
    print(len(problems), "problems over", len(meta_cases), "cases")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
