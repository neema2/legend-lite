# The adversarial markets fixture (2026-11-07)

A known-answer fixture for the timeseries language: market data with traps planted in it, each with the answer a naive
SQL expression gives and the correct one, both worked by the fixture's own generator over the numbers it writes --
never by the code under test. Lite is judged against it by `//core:ts_adversarial_test` (DuckDB); the gs-quant oracle
reads the same files (`tools/gs-quant-oracle`, the companion oracle PR).

## 1. The files

`projects/timeseries/fixtures/adversarial/`, every file but the generator written by it (`python3 generate.py`;
`--check` compares the committed files with what it would write, exit 1 on a difference, naming the first line that
differs; `//projects:timeseries_adversarial_fixture_test` runs it in the core lane). Standard library only, seed
`20261107`; 385,451 bytes in all. `--check` is sensitive to Python's float summation: `sum()` over floats compensates
since 3.12 and does not before, so with `sum()` the plain mean's last digit differed between Python 3.9.6 and 3.12.13
(measured: the fixture test failed under Bazel's 3.12.13 against files written by 3.9.6). Every float sum is
`math.fsum`, correctly rounded, which makes the files version-independent; `--check` passes on Python 3.9.6 and
3.12.13, the two versions verified (`/usr/bin/python3`; the oracle's venv and Bazel's hermetic interpreter).

| File | What it holds |
|---|---|
| `generate.py` | the generator: the walks, the planted traps, the answers, the files |
| `prices.csv` | `symbol,date,open,high,low,close,volume,seq`, 2,470 rows, in FILE order, not date order, on purpose: a late print and a corrected print are where they arrived; sorting by (symbol, date) is the consumer's job. `seq`, the last column, is each row's ingest sequence, 1, 2, ... in file order: the order the database can see (file order it cannot); a consumer that selects named columns is unaffected |
| `prices_adjusted.csv` | the same rows, columns and order with BRAVO's 2:1 split applied: its prices before 2024-06-10 divided by 2 (exact decimals), its volumes multiplied by 2 |
| `calendar.csv` | `date,market,is_business_day` for every day of 2024-2025: NYSE (its 21 full-day closures, 2025-01-09 among them), FX (five days a week), CRYPTO (seven) |
| `manifest.yaml` | the columns, the symbols, and per trap: name, symbol, date_range, the query in words, `naive_sql` (DuckDB, over a table `prices` holding prices.csv's rows, loaded in file order; every one answers one row, its date and its value), `naive_sql_answer`, `correct_answer` (each `{date, value}`, a value null being no value), why; where a trap has them, `control_sql` and `control_answer` (the naive SQL on a neighbouring date, which must answer a value), a `witness` (a date the correct query must answer on), the holiday and its market, the duplicate's date |

Five symbols over 2024-01-01 to 2025-12-31: ALPHA and BRAVO (NYSE), EURUSD (FX), BTCUSD (crypto) and NEWCO (NYSE,
listing 2025-03-03).

## 2. The traps and lite's answers

| Trap | Query | Naive SQL's answer | Correct | Faces the trap | Lite (DuckDB) |
|---|---|---|---|---|---|
| holiday_gap | `returns(CLOSE['ALPHA'], 1)` on 2025-11-28 | none (yesterday is the holiday; its control, 2025-11-26, a value) | over 2025-11-26 | yes | PASS |
| fill_prev_series_start | `CLOSE['ALPHA'] - CLOSE['NEWCO'] ... FILL(PREV)` on 2025-02-28 | NEWCO's first close backfilled | none (and a value on the witness, 2025-03-03) | yes | PASS |
| weekly_sample_holiday_end | `last(CLOSE['ALPHA']) ... SAMPLE BY 1w`, the week of 2025-06-30 | none (Friday 07-04 a holiday; its control, the week before, a value) | 07-03's close, dated 07-03 | yes | PASS |
| late_print | `returns(CLOSE['BRAVO'], 1)` on 2025-03-13 | over 03-11 (arrival, `ORDER BY seq`) | over 03-12 | yes | PASS |
| duplicate_row | `sum(VOLUME['BTCUSD']) ... SAMPLE BY 1w`, the week of 2025-02-03 (02-05's print, then its correction) | 226,756 (both rows) | 193,862 (last print wins, the greatest seq) | yes | **DIFF** -- 226,756, the naive answer (finding 2) |
| duplicate_row/last_print_source | the same over the rows deduplicated by hand in Pure by (SYMBOL, DATE), the greatest SEQ kept, handed to `#TS` | | 193,862 | no -- the fix written by hand | PASS |
| stale_price_5d | `volatility(CLOSE['BRAVO'], 10, annualization_factor => 252)` on 2024-10-21 | 31.235 (change-only rows) | 14.870 | no -- the change-only filter is a mistake lite never makes | PASS |
| split_2_for_1 | `returns(CLOSE['BRAVO'], 1)` over `PRICES_ADJUSTED` on 2024-06-10 | -48.95% (raw) | +2.10% | no -- the file undoes the split | PASS |
| tenor_3m_month_end | `returns(CLOSE['ALPHA'], 3m)` on 2024-05-31 | over 03-01 (90 days) | over 02-29 (3 months, clamped) | yes | PASS |
| vwap_zero_volume | `sum(t.PV['BRAVO']) / sum(t.VOLUME['BRAVO'])` over `(SELECT *, CLOSE * VOLUME AS PV FROM PRICES) t`, `SAMPLE BY 1w` | 192.928 (plain mean) | 189.100 | no -- the formula written by hand | PASS |

**6 of the 9 traps face the trap** -- holiday_gap, fill_prev_series_start, weekly_sample_holiday_end, late_print,
duplicate_row, tenor_3m_month_end: 5 PASS, 1 DIFF. The other cases are evidence of the word, not of the trap.

Every `naive_sql` runs in the test, as the manifest writes it, on the same DuckDB session: it must answer one row, its
date the manifest's (a missing date is a failure, never a match), its value the `naive_sql_answer`'s and not the
correct one (measured: every trap bites; none was dropped); a trap's `control_sql` must answer its `control_answer`.
The holiday trap's calendar is read too: 2025-11-27 closed on the NYSE, 2025-11-26 and 2025-11-28 open. The stale
trap's correct answer is gs-quant's definition, not ours: in gs-quant 2.2.3, the release the companion oracle PR pins,
`volatility(x, 10, annualization_factor=252)` is `returns(x)` (`x / x.shift(1) - 1`), then `rolling(10, 0).std()`
(N - 1), times `sqrt(252) * 100` (`timeseries/econometrics.py` volatility and returns, `statistics.py` std, `helper.py`
normalize_window and apply_ramp), so its window counts the four zero returns.

Each case's status, whether it faces its trap and lite's value are held by
`core/src/test/resources/ts-adversarial/ledger.tsv` (the parity lanes' ratchet's problems, and VALUE MOVED: a DIFF
whose wrong number changes is red; a case twice in the ledger is red). A DIFF is ledgered, never fixed by editing the
words. This repository has no `@Disabled` marker to use: `SkipCensusTest` pins none. Measured: a ledger status or
value moved, a case twice, a manifest naive answer's date changed, or a control answer changed, each turns the target
red, the problem named. A refusal is lite's `LegendCompileException` (a compile's or a plan's) or `DataError` (the
database's raise while the query runs); nothing else is caught.

## 3. Open findings

1. **R15 ruled, not yet a word.** The plan (`docs/TS_ONE_TREE_PLAN_2026_10_31.md` §11, R15) rules kdb+'s
   `wavg(x, weights)` and `wsum(x, weights)`: "`wavg(price, volume)` under `SAMPLE BY` is each period's VWAP,
   `wavg(price, volume, 20)` a rolling one". Neither is among the 84 words (`docs/timeseries/WORDS.md`), and a period's
   aggregate under SAMPLE BY takes one argument (`TsToPure.periodOf`), so `wavg(price, volume)` cannot be written. The
   VWAP case writes the period's ratio of sums over a source with `CLOSE * VOLUME AS PV`. The words were not edited.
2. **The last-print-wins proposal** (needs the owner's ruling; its full proposed row is in PR #90, the register file
   untouched: the owner rules and lands it).
   BTCUSD's 2025-02-05 print is re-sent corrected (another close and volume, the greater seq). Today lite's SUM runs
   over every row: `sum(VOLUME['BTCUSD'])` under `SAMPLE BY 1w` answers 226,756, both rows counted
   (`timeseries::lines::sample` groups every row); last print wins gives 193,862, measured by
   `duplicate_row/last_print_source`. Counting the date once by its first print would give 188,928 -- the generator's
   arithmetic, no query's measurement. S149 refuses the words that count observations or read a line in order over
   such a line; a period's aggregate is outside it.
   *Proposed:* over a line with two or more rows on one date, a word takes, of each date's rows, the one with the
   greatest sequence, where the source names a sequence column (how it names one is to be designed); where it names
   none, the word is refused while the query runs, naming the fix: declare a sequence column, or deduplicate in Pure
   as `duplicate_row/last_print_source` does. *The alternative the owner may prefer:* refuse always. gs-quant's
   behaviour over the corrected print: see the oracle PR's README table. The recommendation is the PR author's.
3. **Defect report: `last()` under SAMPLE BY is order-dependent on duplicate dates -- two load orders, two closes.**
   Measured by `TsAdversarialTest.sampleByLastPicksAmongADatesTiedRowsByTheOrderTheyWereLoaded`:
   `last(CLOSE['BTCUSD']) ... SAMPLE BY 1d` on 2025-02-05 answers **63009.14** over the rows in prices.csv's order (the
   original print; its correction arrived after it) and **63387.19** over the same rows in reverse order (the
   correction). The cause, read: SAMPLE BY's `last` takes a period's row by its number in time, its date's seconds
   (`timeseries::lines::sample`, `Taken.last`: `rowMapper($x.sv, $x.so)->maxBy()`), so a date's two rows tie and the
   database picks; seq is never read. S149's refusal (`timeseries::lines::once`) names "SAMPLE BY 1d with last(x)" as
   its fix; on a duplicate date that fix is not one, and over the fixture as loaded it answers the superseded print.
   The test turns red the day the pick reads an order of the data's. (It motivates the last-print-wins proposal,
   finding 2, which does not name it as a fix.)
4. **No way to de-duplicate inside `#TS`** (a defect report, not a register row). A source's subquery refuses
   `SELECT DISTINCT` with a bare parser error ("syntax error at or near DISTINCT", in FROM and in a WITH body alike;
   measured 2026-11-07), not the language's refusal naming a fix; `GROUP BY` is refused by name ("a source's subquery
   in #TS is SELECT ... FROM source [WHERE ...]: GROUP is not one of its clauses yet"); neither could keep the greatest
   seq anyway. The fix today is Pure: the rows deduplicated (`rowNumber` over the date's rows by seq, descending) and
   the relation handed to `#TS`, as `duplicate_row/last_print_source` does.
5. **What the oracle will see (not lite's divergences under the contract).** `fill_prev_series_start`'s naive answer is
   gs-quant's own step alignment (S145, ruled). `late_print` is judged after the oracle's sort by (symbol, date), which
   the contract makes the consumer's job. gs-quant's behaviour on each trap: see the oracle PR's README table.
