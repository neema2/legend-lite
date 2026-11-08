# Copyright 2026 Legend Contributors
# SPDX-License-Identifier: Apache-2.0
"""THE LEDGERS' REPORTS (README.md): read from the three ledgers alone -- never typed -- and written as data and a page.

  two_rows.py --samples ledger.tsv --raw ledger-adversarial-raw.tsv --adjusted ledger-adversarial-adjusted.tsv \\
      --two-rows two-rows.tsv --summary summary.tsv --unruled unruled.tsv --md TWO_ROWS.md

- two-rows.tsv: what gs-quant and lite answer over the fixture's line with two rows on one date (BTCUSD's print and its
  correction on 2025-02-05), case by case, each case in exactly one group; a row no group describes is refused.
- summary.tsv: every count the PR and docs/GATES.md quote -- each ledger's verdicts and their dup-span share, and lite's
  split over the two-row line (cases and distinct words).
- unruled.tsv: every DIFF no register row rules, by case, with the datasets it is a DIFF in: the list the PR's Open
  findings must each answer.
- TWO_ROWS.md: the two-rows page, from the same rows.

The three .tsv files are diff-tested (//tools/gs-quant-oracle:update_generated_tests, in //:generated, the checks lane);
the page is written by the same rule and not diff-tested (prose is never a test input, G17, as docs/timeseries/WORDS.md).
Regenerate: bazel run //tools/gs-quant-oracle:update_generated and :update_two_rows_page.
"""

import argparse
import re
import sys

SYMBOL, DATE = "BTCUSD", "2025-02-05"
LITE = " | lite: "

# each group: its key and what gs-quant answers; every row at @BTCUSD falls in exactly one, or the run fails
GROUPS = (
    ("raises-497", "raises `MqValueError: multiple data points on same date` at `econometrics.py:497`"
                   " (`_get_annualization_factor`)"),
    ("raises-two-rows", "raises elsewhere, on this symbol alone (the two rows: no other trap symbol raises there)"),
    ("raises-every-symbol", "raises on every trap symbol, the two rows aside (gs-quant's calendar-window comparison)"),
    ("two-values", "answers two values on " + DATE),
    ("four-values", "answers four values on " + DATE + " (the line met with itself or its own window: the two rows"
                    " against the two, 2 x 2)"),
    ("one-value", "answers one value on " + DATE),
    ("no-value", "answers no value on " + DATE),
    ("two-values-moved", "answers two values on a later date, one or none on " + DATE + " itself (a calendar lag"
                         " moves the duplicate's date)"),
    ("two-values-twice", "answers two values on " + DATE + " and two on a later date (a calendar period back reads"
                         " the duplicate again there)"),
)
VERDICTS = ("PASS", "DIFF", "REFUSED", "GS_ERROR", "SKIP")


def read(path):
    rows = {}
    names = None
    for line in open(path, encoding="utf-8"):
        line = line.rstrip("\n")
        if line.startswith("#") or not line:
            continue
        cells = line.split("\t")
        if names is None:
            names = cells
            continue
        if len(cells) != len(names):
            raise SystemExit("two_rows.py: " + path + ": a row of " + str(len(cells)) + " cells: " + cells[0])
        rows[cells[0]] = dict(zip(names, cells))
    return rows


def gs_half(r):
    return r["why"].split(LITE)[0]


def counts(why):
    """gs-quant's count of values on each date its half of `why` names (snap.py's value_counts)."""
    return {d: int(n) for n, d in re.findall(r", (\d+) values on (\d{4}-\d\d-\d\d)", why)}


def classify(rows):
    """Each case run over the two-row line (every OK or GS_ERROR row at @BTCUSD, the ledger's order): (case, group,
    gs-quant's answer, lite's verdict). A row no group describes is refused, never filed by elimination."""
    raised_on = {}
    symbols = {k.rsplit(" @", 1)[1] for k in rows if " @" in k}
    for key, r in rows.items():
        if r["gsquant"] == "GS_ERROR" and " @" in key:
            case, symbol = key.rsplit(" @", 1)
            raised_on.setdefault(case, set()).add(symbol)
    out = []
    for key, r in rows.items():
        if not key.endswith(" @" + SYMBOL) or r["gsquant"] == "SKIP":
            continue
        case = key[: -len(" @" + SYMBOL)]
        why = gs_half(r)
        if r["gsquant"] == "GS_ERROR":
            frame = re.search(r"\(([\w/.]+\.py:\d+)\)$", why)
            if frame is None:
                raise SystemExit("two_rows.py: " + key + ": a GS_ERROR naming no frame of gs-quant: " + why)
            if frame.group(1) == "econometrics.py:497":
                group = "raises-497"
            elif raised_on[case] == {SYMBOL}:
                group = "raises-two-rows"
            elif raised_on[case] == symbols:
                group = "raises-every-symbol"
            else:
                raise SystemExit("two_rows.py: " + key + " raises on " + ", ".join(sorted(raised_on[case]))
                                 + ": no group says so")
            out.append((case, group, why.split(":")[0] + " at " + frame.group(1), r["duckdb"]))
            continue
        c = counts(why)
        if DATE not in c:
            raise SystemExit("two_rows.py: " + key + ": its row names no count of values on " + DATE)
        moved = {d: n for d, n in c.items() if d != DATE and n > 1}
        if not moved:
            group = {0: "no-value", 1: "one-value", 2: "two-values", 4: "four-values"}.get(c[DATE])
        elif set(moved.values()) == {2}:
            group = {0: "two-values-moved", 1: "two-values-moved", 2: "two-values-twice"}.get(c[DATE])
        else:
            group = None
        if group is None:
            raise SystemExit("two_rows.py: " + key + " answers " + str(c) + ": no group says so")
        out.append((case, group, why, r["duckdb"]))
    return out


def two_rows_tsv(cases):
    lines = ["# THE TWO-ROWS TABLE's data (two_rows.py, from ledger-adversarial-raw.tsv): each case run over " + SYMBOL
             + ", its group, gs-quant's answer there and lite's verdict. GENERATED -- regenerate: bazel run"
             " //tools/gs-quant-oracle:update_generated", "case\tgroup\tgs-quant\tlite (duckdb)"]
    lines += ["\t".join(c) for c in cases]
    return "\n".join(lines) + "\n"


def lite_split(cases):
    """lite over the two-row line: cases and distinct words it refuses, and answers over both rows (PASS, DIFF)."""
    def words(rows):
        return len({c[0].split(" ", 1)[0] for c in rows})
    run = [c for c in cases if c[3] != "GS_ERROR"]
    pick = lambda vs: [c for c in run if c[3] in vs]  # noqa: E731
    raised = [c for c in cases if c[3] == "GS_ERROR"]
    return {
        "run": (len(cases), words(cases)),
        "gs-quant raises": (len(raised), words(raised)),
        "gs-quant answers": (len(run), words(run)),
        "lite refuses (S149)": (len(pick({"REFUSED"})), words(pick({"REFUSED"}))),
        "lite answers over both rows": (len(pick({"PASS", "DIFF"})), words(pick({"PASS", "DIFF"}))),
        "of which PASS": (len(pick({"PASS"})), words(pick({"PASS"}))),
        "of which DIFF": (len(pick({"DIFF"})), words(pick({"DIFF"}))),
    }


def summary_tsv(ledgers, cases):
    lines = ["# THE LEDGERS' COUNTS (two_rows.py, from the three ledgers): each ledger's verdicts with their dup-span"
             " share, and lite's split over the two-row line (" + SYMBOL + ", " + DATE + "). Every count the PR and"
             " docs/GATES.md quote is a row here. GENERATED -- regenerate: bazel run //tools/gs-quant-oracle:update_generated",
             "ledger\tmeasure\tcases\tof which dup-span, or distinct words"]
    for name, rows in ledgers:
        for v in VERDICTS:
            n = sum(1 for r in rows.values() if r["duckdb"] == v)
            d = sum(1 for r in rows.values() if r["duckdb"] == v and r["note"] == "dup-span")
            lines.append("\t".join([name, v, str(n), str(d) + " dup-span"]))
        lines.append("\t".join([name, "rows", str(len(rows)), "-"]))
    for measure, (n, w) in lite_split(cases).items():
        lines.append("\t".join(["two-row line", measure, str(n), str(w) + " words"]))
    return "\n".join(lines) + "\n"


def unruled_tsv(ledgers):
    by = {}
    for name, rows in ledgers:
        for key, r in rows.items():
            if r["duckdb"] == "DIFF" and r["register"] == "-":
                case, at = key.rsplit(" @", 1) if " @" in key else (key, None)
                by.setdefault(case, []).append(name.replace(".tsv", "") + ("" if at is None else "@" + at))
    lines = ["# EVERY DIFF NO REGISTER ROW RULES (two_rows.py, from the three ledgers): each case and where it is a DIFF;"
             " the PR's Open findings answer each. GENERATED -- regenerate: bazel run //tools/gs-quant-oracle:update_generated",
             "case\twhere"]
    lines += [c + "\t" + ",".join(w) for c, w in sorted(by.items())]
    return "\n".join(lines) + "\n"


def md(cases):
    by = {g: [] for g, _ in GROUPS}
    verdicts = {g: {} for g, _ in GROUPS}
    for c, g, _, v in cases:
        by[g].append(c)
        verdicts[g][v] = verdicts[g].get(v, 0) + 1
    s = lite_split(cases)
    out = ["<!-- GENERATED by two_rows.py from ledger-adversarial-raw.tsv -- DO NOT EDIT. Regenerate: bazel run"
           " //tools/gs-quant-oracle:update_two_rows_page -->", "",
           "# On a line with two rows for one date", "",
           "The adversarial fixture's `duplicate_row` trap gives " + SYMBOL + " two rows on " + DATE + ", a print and"
           " its correction. gs-quant is handed both, in `seq` order, and counts by position. Of the mapped cases run"
           " over " + SYMBOL + " (`ledger-adversarial-raw.tsv`, rows ` @" + SYMBOL + "`; each row's `why` says gs-quant's"
           " count of values there, or the frame it raised in, then lite's account after ` | lite: `):", "",
           "| gs-quant's answer on " + SYMBOL + " " + DATE + " | cases | lite on DuckDB | words (their cases) |",
           "|---|---|---|---|"]
    for g, text in GROUPS:
        words = {}
        for c in by[g]:
            word, args = c.split(" ", 1)
            words.setdefault(word, []).append(args)
        cell = "; ".join("`" + w + "` (" + ", ".join("`" + a + "`" for a in a_) + ")" for w, a_ in words.items())
        lite = ", ".join(str(n) + " " + v for v, n in sorted(verdicts[g].items())) or "-"
        out.append("| " + text + " | " + str(len(by[g])) + " | " + lite + " | " + (cell or "-") + " |")
    out += ["", "lite over the line: of the " + str(s["gs-quant answers"][0]) + " cases gs-quant answers, lite refuses "
            + str(s["lite refuses (S149)"][0]) + " cases of " + str(s["lite refuses (S149)"][1]) + " words (register"
            " S149: a word that reads its line in order, met with two rows on one date, is refused while the query runs,"
            " naming the fix) and answers " + str(s["lite answers over both rows"][0]) + " cases of "
            + str(s["lite answers over both rows"][1]) + " words over both rows (" + str(s["of which PASS"][0])
            + " PASS, " + str(s["of which DIFF"][0]) + " DIFF). A PASS here is today's agreement over both rows, not a"
            " vouch for it.", "",
            "Duplicates do not just double-count: where a word aligns the line with itself or with its own window, the"
            " two rows meet the two and gs-quant answers four values on the date (`four-values` above); lite, where it"
            " meets two lines on that date (`if` over two lines), does the same.", ""]
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser()
    for a in ("samples", "raw", "adjusted", "two-rows", "summary", "unruled", "md"):
        ap.add_argument("--" + a, required=True)
    a = ap.parse_args()
    ledgers = [("ledger.tsv", read(a.samples)), ("ledger-adversarial-raw.tsv", read(a.raw)),
               ("ledger-adversarial-adjusted.tsv", read(a.adjusted))]
    cases = classify(ledgers[1][1])
    for path, text in ((a.two_rows, two_rows_tsv(cases)), (a.summary, summary_tsv(ledgers, cases)),
                       (a.unruled, unruled_tsv(ledgers)), (a.md, md(cases))):
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
