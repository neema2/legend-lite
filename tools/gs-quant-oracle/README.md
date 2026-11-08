# tools/gs-quant-oracle: gs-quant's own answers for every word of `#TS`

The timeseries library (`projects/timeseries/model.pure`) is gs-quant's functions written as Pure, and the `#TS{...}#`
island is its language (`docs/timeseries/WORDS.md`). This tool holds each word to `gs_quant.timeseries` itself: every
case of `mapping.yaml` is a `#TS` item and the gs-quant call that is its twin, written side by side; `snap.py` runs the
twin over the gallery's samples and snaps each series into `answers.jsonl.gz`, and `//core:ts_gsquant_parity_test` runs
the item through the Pure library on DuckDB and judges it against them.

**CI never runs gs-quant.** It is not one of the repository's locked requirements. The parity lane judges each case
against the committed answers, and fails a case with no answer (UNSNAPPED), one whose texts or samples moved since
the snap (STALE), an answer whose case is gone (GONE), and a file snapped with another gs-quant (VERSION). Only a
person runs the live check (`--check`), locally: after editing the mapping, after changing this tool, and before
landing a change to the answers.

## How this differs from `tools/ts-oracle/`

`tools/ts-oracle/` (docs/TS_GSQUANT_ORACLE_2026_11_01.md) recomputes the 210 series the gallery and markets pages
DRAW, each from its tile's formula, and compares them with what the tile answered: parity of the drawn page with
gs-quant, at 1e-6 relative, over the forms the pages happen to use. This tool takes every word, case by case, in the
forms its parameters allow (a count, a calendar period, `all`, `whole`, each option), and compares the library's own
answer with gs-quant's own function at the contract's tolerances: parity of the vocabulary. Both stay: the first
catches a tile that composes words wrongly, this one a word that departs from gs-quant in a form no page draws.

## Prerequisites

- **Python 3 on the pinned stack.** The stack is ONE set of facts, `pins.env` (KEY=VALUE, as `tools/oracle-pins.env`):
  gs-quant, and pandas, numpy, scipy and statsmodels, each exactly (gs-quant declares only ranges for them:
  `numpy>1.17.0`, `pandas>=1.4`, `scipy>=1.2.0`, `statsmodels>=0.14.5`, so an answer is reproducible only with each
  held), and Python's minor. `requirements.txt` reads each (`gs-quant==${GS_QUANT_VERSION}`, ...), `snap.sh` installs
  from `requirements.txt` into its interpreter when that holds another release, `snap.py` refuses any other installed
  release or Python minor (VERSION, no fallback) and writes the whole set into every answer's hash, every answers
  file's metadata and every ledger's header, and `//core:ts_gsquant_parity_test` reads `pins.env`. A snap made on
  another stack is STALE on `--check` and in the parity test. In the checkout: `uv venv --python 3.12 .venv`, then
  `snap.sh` (`.venv` is its interpreter, `GSQUANT_ORACLE_PYTHON` overrides it; with neither, `snap.sh` refuses -- it
  never installs into a system Python). To move a pin: edit `pins.env`, run `snap.sh --write` and `--write --fixture`,
  and rewrite the ledgers' verdicts with the parity test's update flag.
- **The samples**, built once: `bazel build //datacube:ts_replay_samples` (prices, rates and the macro series, as the
  pages make them; `bazel-bin/datacube/generated/ts-replay-samples.json`).

## Commands

```sh
tools/gs-quant-oracle/snap.sh --write                      # every case: write answers.jsonl.gz and ledger.tsv
tools/gs-quant-oracle/snap.sh --write --only 'mean *'      # one glob of case keys, merged into the files
tools/gs-quant-oracle/snap.sh --check                      # DRIFT, UNSNAPPED, STALE, GONE, VERSION, TAG, READER, WORDS
tools/gs-quant-oracle/snap.sh --write --fixture DIR        # an adversarial fixture (manifest.yaml): its prices.csv as
                                                           # adversarial-raw, its prices_adjusted.csv as
                                                           # adversarial-adjusted, each its answers and ledger; and
                                                           # traps.tsv (a missing price file is an error)
tools/gs-quant-oracle/snap.sh --release-diff OTHER_PYTHON  # release-diff.tsv: each mapped definition at another
                                                           # gs-quant (that interpreter's) against the pin's
bazel run //tools/gs-quant-oracle:snap -- --check          # the same through Bazel
# the lite side, and its ledgers' verdicts rewritten (the ratchet moves only so):
bazel test //core:ts_gsquant_parity_test
bazel test //core:ts_gsquant_parity_test --strategy=TestRunner=local \
    --test_env=JDK_JAVA_OPTIONS=-Dts.gsquant.update=$PWD/tools/gs-quant-oracle
bazel test //core:ts_gsquant_parity_test --test_env=JDK_JAVA_OPTIONS=-Dts.gsquant.only=zscores   # print, judge nothing
# the ledgers' reports, after any snap or verdict rewrite:
bazel run //tools/gs-quant-oracle:update_generated && bazel run //tools/gs-quant-oracle:update_two_rows_page
```

## The files

- **`mapping.yaml`**: each of the 87 words (and `wavg`, `wsum`: ruled by R15, not yet words, SKIP), MAPPED (its gs-quant function, `src` the file and line in
  `gs_quant/timeseries/`, its cases) or SKIP (why). A case is `[args, ts, gs]` or `[args, ts, gs, column]`: its name
  within the word, the `#TS` item (`{x}` close of NVDA, `{y}` close of SPX; a case starting `WITH` is its own
  statement), the gs-quant expression, and a struct word's field. A default that differs is passed explicitly
  (`defaults` says which): every gs-quant window defaults to `Window(None, 0)`, to date, where lite's counts 14, 20, 22,
  63 or 252. `known` names the register row that rules a case's DIFF. Each case is one line, so the parity test reads
  the file without a YAML library; `snap.py` holds its YAML reader to that line reader (READER) and each word's mapped
  function to the library's own `Words.gsQuant` tag (TAG).
- **`answers.jsonl.gz`**: one row per series, gzipped (written with mtime 0, so a re-snap of the same answers is the
  same bytes): `{word, args, symbol, dates, values}`. `values` is gs-quant's answer in date order along the case's
  SPINE -- the sorted union of the dates of the lines `symbol` names (`NVDA,SPX`, `CPI,SPX`, a fixture's `BRAVO,ALPHA`)
  -- from the answer's first date to its last, `null` where it has none, and `"Inf"` or `"-Inf"` for a value that is
  not finite. `dates` is `[first, last, count]`: the reader takes the spine's dates from `first` to `last` and checks
  that there are `count` of them (SPINE fails the parity test otherwise). A series with a date off its spine (gs-quant's
  lag of a calendar period moves each date: `lag obs=1mo` answers on 2020-02-01, a Saturday) carries its own dates as a
  list instead, so no point is lost. First, the meta line: the tool, gs-quant's release, the data's sha, and each
  case's sha (its two texts, its column, the data, the release), its status (OK or GS_ERROR) and gs-quant's error.
  Three files: `answers.jsonl.gz` (the samples, 0.8 MB), `answers-adversarial-raw.jsonl.gz` and
  `answers-adversarial-adjusted.jsonl.gz` (a fixture's `prices.csv` and `prices_adjusted.csv`, 1.1 MB each).
- **`ledger.tsv`** (and `ledger-adversarial-raw.tsv`, `ledger-adversarial-adjusted.tsv`): each case, `<word> <args>`
  (over a fixture ` @<symbol>`): `gsquant` (snap.py's: OK, GS_ERROR, SKIP), `duckdb` (the parity test's: PASS, DIFF,
  REFUSED, GS_ERROR, SKIP), `register` (the row that rules a DIFF), `digest` (the sha256 of the series' row, its
  `dates` and `values` written canonically: `--check` holds a live run to it, so drift shows from the ledger alone),
  `note` (`dup-span`: the series covers a date a line it reads holds two rows on), and `why`, in two halves: gs-quant's
  (snap.py's: its points, its count of values on each date a line holds two rows on, or the frame of gs-quant it raised
  in) and, after ` | lite: `, the parity test's account of a DIFF or a REFUSED. Each writer rewrites its own half; the
  two agree byte for byte. A ledger's header names the stack it was snapped on; its columns are read exactly, never
  filled in.
- **`traps.tsv`** (`--write --fixture`): each trap of the fixture's manifest that `mapping.yaml`'s `traps:` names a case
  for -- #90's four: `holiday_gap`, `late_print`, `split_2_for_1`, `stale_price_5d` -- gs-quant's value on the trap's
  date in each price file, against the manifest's correct answer. `--check --fixture` holds it to a fresh reading.
- **`release-diff.tsv`** (`--release-diff`): each `gs_quant.timeseries` definition a case calls, its AST's sha256 at
  the pin and at another release, and whether they are one.
- **`summary.tsv`**, **`unruled.tsv`**, **`two-rows.tsv`** and **`TWO_ROWS.md`**: the ledgers' reports (below).

**The order of the rows.** gs-quant counts observations by position in the series it is handed (`x.shift(1)`,
`x.rolling(w)`: register S149), and a fixture's `prices.csv` is in FILE order (its `late_print` trap holds a row after a
later date's). `snap.py` sorts every price frame by (symbol, date) before gs-quant sees it -- stably, so two rows of one
symbol on one date keep their file order between them (the `duplicate_row` trap, which gs-quant itself refuses in its
annualized words: "multiple data points on same date"). A price file is read by column NAME (`symbol`, `date`,
`close`): a fixture's trailing `seq` (its ingest order) is never handed to gs-quant, but where it is present the
stable sort's order within a date is held to it -- `seq` increasing, else SEQ, and nothing is snapped -- so file order
and ingest order are one, checked rather than assumed. gs-quant sees every row (a corrected print and the print it
corrects, both); "last print wins" is the trap's answer, not gs-quant's.

## On a line with two rows for one date

**[TWO_ROWS.md](TWO_ROWS.md)** says, case by case, what gs-quant and lite answer over the adversarial fixture's line
with two rows on one date (BTCUSD's print and its correction on 2025-02-05): which words raise `MqValueError` at
`econometrics.py:497`, which raise elsewhere on that symbol alone, which answer two values there, which four, and lite's
verdict for each. Every count the PR and `docs/GATES.md` quote is a row of **`summary.tsv`** (each ledger's verdicts and
their dup-span share; lite's split over the two-row line in cases and distinct words), and every DIFF no register row
rules is a row of **`unruled.tsv`**, which the PR's Open findings answer one by one. All of it is GENERATED from the
three ledgers by `two_rows.py`, never typed, and a ledger row no group of the table describes fails the run. The three
`.tsv` files are diff-tested (`//tools/gs-quant-oracle:update_generated_tests`, in `//:generated`, the checks lane);
`TWO_ROWS.md` is written from the raw ledger by the same rule and not diff-tested (prose is never a test input, G17, as
`docs/timeseries/WORDS.md`).

**One date's values compare as a bag -- deliberately, not leniently.** Where a line holds two rows on one date, both
sides answer more than one value there: gs-quant in the order it was handed the rows (the file's, `seq`'s), lite in
the order the database returns them, which is undefined among rows of equal keys (register S149: "the window over rows
is the database's, whose order among equal keys is undefined"). Compared in order, the same two numbers would DIFF by
the database's whim from run to run. So the parity test sorts each date's values on both sides and compares them one
for one, at the same tolerances: the same COUNT of values on every date, and the same values. A date with two values
on one side and one on the other is a DIFF.

## The judge

The same dates on both sides, each date the same number of values, and each value within **1e-9 of its series'
scale** (a level: 1e-9 times the largest magnitude of gs-quant's figures, never less than 1e-9 -- the replays' rule;
the user's ruling of 2026-11-08, after a sum at BTC's level differed from pandas' in the last place)
or **1e-8 relative** for a volatility, a correlation or a beta (`volatility`, `exponential_volatility`,
`exponential_spread_volatility`, `vol_swap_volatility`, `correlation`, `beta`). A date one side answers and the other
does not is a DIFF. gs-quant's one number (`percentile ... whole`, `value`) is its answer on every date of x. The
absolute tolerance is the contract's: at a level of BTC-sized sums (4.4e6, its float64 spacing 9.3e-10) it is
effectively bit-exact, so a sum taken in another order DIFFs by a few units in the last place.

A verdict is earned, never defaulted: REFUSED is lite's own refusal alone -- a `DataError` at run time (S149's among
them) or a `LegendCompileException` as the query compiles -- and any other exception fails the test as its own fault;
GS_ERROR is an error raised inside gs-quant (snap.py stops, HARNESS, on one raised by no frame of gs-quant); an OK
case with no answer row fails (NO ROW), as does a row whose points are not its metadata's count (POINTS) or whose
dates and values are not one length (SPINE). Every DIFF and REFUSED is written to the test's undeclared outputs
(`gsquant-findings.md`): its first points apart, gs-quant's call and source line, the `#TS` statement and lite's SQL
on DuckDB. **A DIFF is a finding: post it, ledger it, and write its proposed register row in the PR text, in full (a
contributor never edits `docs/SEMANTICS_REGISTER.md`) -- never change a word to make it pass.**

## What the oracle cannot say

- A word gs-quant has no function for (SKIP, 25 of 87, and `wavg`, `wsum`, ruled by R15 and not yet words): DataCube's own (`atr`, the channels, the captures, the
  regressions' residue), a Marquee service (`alpha`, `information_ratio`, `r_squared`, `tracking_error`: report reads
  in gs-quant; `cash_index`: gs-quant's cash is a currency's curve from Marquee), or no twin (`rank`, `round_down`, `round_up`, `percent_change`: its identity `x / x0 - 1` for the owner
  to rule).
- A form with no gs-quant argument: a word's `from`, a period or date form of `count`, an `across` window (each date's
  instruments: gs-quant's functions take one series).
- A case gs-quant raises on is GS_ERROR, its message and frame kept: `cov` and `beta` over a calendar window compare a
  DatetimeIndex with a `datetime.date` (`statistics.py:720`, `econometrics.py:1040`) and raise on every symbol -- as
  measured on the pinned stack (pandas' default `datetime64[us]` among it): an oracle can say what gs-quant answers on
  the stack it runs, not on every stack gs-quant's ranges admit.
