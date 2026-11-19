# Census B2: the corpus tests that pass on legend-engine's SQL text (2026-11-19)

The compatibility program's orchestrator asked for one read-only measurement for CP-4 ("upstream's SQL text is never
promised", ruled by the user on 2026-11-19). The question: how many relational-corpus tests pass today only because
lite reproduces legend-engine's SQL text? This count is the one owed to the user before slice 2.3 deletes anything,
and it becomes the "not promised" class's pin.

The answer: **152 distinct tests move from pass to fail when legend-engine's text is replaced by lite's own.**
- **143** are judged on text alone. They move to "not promised".
- **3** are DIVERGED appeals: the text verdict hid an answer difference (§3).
- **6** are judged on legend-engine's plan structure, not on SQL text (§4).

No answer changed beyond the three in §3.

## 1. The method

**The probe.** It is in `census-b2-sqltext-probe-2026-11-19.patch`, beside this record, and on the local branch
`claude/census-b2-sqltext` at `0968d1220`, off main `b75d6c73a`. Neither is for main.
- `PureText.engineText` and `PureText.enginePlanText` are the only places the `EngineStyleH2`, `EngineStyleDB2` and
  `EngineStyleComposite` printers are built. Both now return a proxy that writes what the product's own H2 dialect
  writes for the same MIR, so every engine-text and plan-text path is covered.
- The engine-only parts of the printer interface answer lite's plainest form: no template parameters, an alias as
  given, the select list cut from lite's text. A FreeMarker collection splice is refused, marked `SQLTEXT-PROBE`.

**The judging is unchanged.** By the user's ruling of 2026-09-21 (`docs/SQLTEXT_ROW_VERDICT_CHARTER.md` §0), a text
byte-equal to the golden is the verdict. A failed text appeals to rows: the golden is replayed on the referee H2 and
compared with lite's rows. A match holds; a divergence or fault fails; a decline keeps the text as the contract. Under
the probe, therefore:
- a test that moves to fail passed only because of legend-engine's text;
- an appeal that comes back DIVERGED exposes an answer difference the text had hidden.

Lite never executes engine text: the product renderer refuses its constructs at run time. So no moved test's rows
came from the printers.

**The runs and the baseline.**
- Ran: `//spec:judge_host_h2` and `//spec:judge_host_duckdb`, the corpus lanes' host passes.
- Baseline: the committed `spec/src/test/resources/rcorpus/{h2,duckdb}-fail-roster.txt`. The corpus lane diff-tests
  those rosters against main's measured ones, so they are main's results.
- Each moved test is classified by its own failure line in the pass's `host.log`, by the rules in §6. The full list,
  one row per test and lane, is `census-b2-sqltext-probe-2026-11-19.tsv`.

**Runtime.** Both passes together took 10.5 minutes wall-clock, run in parallel on a 4-core container (04:04:45 to
04:15:16 UTC), plus one single-test diagnosis with `//spec:corpus_one`.

## 2. The counts

| Lane | Fail roster | Moved to fail | Moved to pass | (a) text alone | (b) DIVERGED | (c) plan structure |
|---|---:|---:|---:|---:|---:|---:|
| H2 | 276 → 423 | 147 | 0 | 139 | 2 | 6 |
| DuckDB | 108 → 260 | 152 | 0 | 143 | 3 | 6 |
| Distinct tests | | 152 | 0 | 143 | 3 | 6 |

All 147 of H2's moved tests are among DuckDB's. The DuckDB-only five are:
- `testToSqlGenerationFirstDayOfWeek` (b);
- four that already fail on H2 on main: `testPlanForDateTimeVariableESTTimeZone`, `testBase64H2`, `testPaginated` and
  `testPaginatedWithVariables`.

The skipped rosters are unchanged. H2's ordering register lost 4 entries, which follow from the moves.

(a), text alone, by how the text is judged (DuckDB / H2):

| Sub-class | DuckDB | H2 | What |
|---|---:|---:|---|
| a1 | 64 | 64 | The text failed, and the rows appeal declined. 23 / 22 are DB2 or Composite goldens the H2 referee cannot replay; 19 / 20 are lite's rows underivable on the referee; 22 / 22 are goldens that do not run on the referee (a missing schema or table, a syntax error, now-relative dates). |
| a2 | 22 | 20 | A printed SQL, plan or DDL string compared or searched, with no appeal: `planToString` strings, `toDDL`, `contains('parsedatetime')`, "expected null-safe comparison in plan", the JSON's `"sql"`. |
| a3 | 53 | 52 | Lite's printer has no form for an engine-text construct the test prints. Mostly untyped plan parameters in `planToString` goldens (sampled: every one asserts the plan's text); also FreeMarker splices, a minimal-fraction date format and a mixed tagged value. |
| a4 | 4 | 3 | SQL text read from legend-engine's plan (an SQL node's `sqlQuery`, an activity's `.sql`). With lite's printer the read does not plan, and lite plans a body whole, so the body fails. Its rows asserts would hold: `testBase64H2`, diagnosed alone, fails on `$result.activities->...->cast(@RelationalActivity)`. |

## 3. (b) The DIVERGED appeals: answers the text had hidden

Each is a real finding. In each, my reading from the pinned sources is that lite answers Pure's semantics where
legend-engine's golden SQL does not.

1. **`meta::relational::tests::functions::sqlstring::testToSQLStringForTDSStringJoin`** (H2 and DuckDB).
   - legend-engine's golden H2 SQL writes `joinStrings(list, sep)` as `concat(a, ' ', b, '|')`, appending the separator;
     the `(prefix, sep, suffix)` form likewise appends all three. The source is legend-engine's `testToSQLString.pure`.
   - Pure's `joinStrings(strings, prefix, separator, suffix)` (legend-pure `joinStrings.pure`) joins with the separator.
   - Lite's rows are Pure's: `Anthony| |Allen` and `[Anthony, ,Allen]`. The golden's are `Anthony Allen|` and
     `Anthony Allen[,]`.
   - So the golden SQL is wrong against Pure, and on main the text verdict hid that lite disagrees with it.
2. **`meta::relational::tests::functions::sqlstring::testHashFunctions`** (H2 and DuckDB).
   - Only the `tds_digest` column differs: `[first, last]->joinStrings('|')->hash(MD5)`.
   - Same root cause as 1: legend-engine hashes `concat(first, last, '|')`, lite hashes `first|last`, as Pure joins.
3. **`meta::relational::tests::functions::sqlstring::testToSqlGenerationFirstDayOfWeek`** (DuckDB only).
   - The golden on the referee H2 gives 2014-11-30, a Sunday. Lite on DuckDB gives 2014-12-01, a Monday.
   - Pure's `firstDayOfWeek` is `mostRecentDayOfWeek(DayOfWeek.Monday)` (legend-engine `dateExtension.pure:202`), so
     DuckDB answers Pure.
   - On the H2 lane the test still passes under the probe, and with lite's text only a matching appeal passes it. So
     lite's H2 answer is the golden's Sunday.
   - Lite lowers `firstDayOfWeek` to a week truncation that each dialect writes as `date_trunc('week')`. That answers
     Monday on DuckDB and Sunday on H2. This is a cross-database disagreement in lite, with H2 wrong against Pure.

## 4. (c) legend-engine's plan structure, not SQL text

Six tests: `meta::pure::executionPlan::tests::testSupportStreamFlagSimple`, `testSupportStreamFlagFromSimple`,
`testSupportStreamFlagGraphFetchSimple`, `testSupportStreamFlagWithSupportedAndUnSupportedUsages`,
`testSupportStreamFlagWithTdsJoinForOneDB` and `testSupportStreamFlagithTdsJoinForTwoDB`.
- They assert a `FunctionParametersValidationNode`'s `supportsStream` flag in legend-engine's execution plan.
- Lite builds that plan only through the engine plan-text path, so without the engine printer the plan does not exist
  and the body fails.
- CP-4 as worded covers SQL text. Whether legend-engine's plan structure is promised is a separate question.

## 5. What this does not measure

- **Bodies that already fail on main.** They are not counted. Their text dependence, if any, is invisible here.
- **The SQL shape.** The probe swaps the printer, not the MIR. A shape the lowering builds to match legend-engine's
  text (`RelationalRootForm`'s root form, the union frame) still shapes the MIR. Its text effect is in the count; its
  structural effect on answers is not. None of the moved tests failed on an answer except §3.
- **Under the appeal.** "Text alone" means the text verdict failed and no appeal could rescue it. With no appeal at all,
  more tests would depend on text. That count is not taken here, because the appeal is the judging as ruled.

## 6. The classification rules

Each moved test's failure line, first match. The script is `m/sqltext_classify.py` in the session's scratchpad; its
rules are restated here.

| Class | The failure line |
|---|---|
| b | `ROW verdict ... diverged` |
| a1 | `(sql-text, DB2` or `(sql-text, Composite`; `rows underivable`; `oracle declined` |
| a2 | `AssertFailed: expected ...`, `SQL not as expected`, a plan-text dump, `Expected enum push-down`, `expected null-safe`, `expected plain equals`, `Must have`, `Milestoning column reference`, `Assert failed` |
| a3 | `DialectCapability`, `SQLTEXT-PROBE`, `a tagged value reached the renderer` |
| a4, or c by name | `class query under TypedMap is not resolvable yet`: the `testSupportStreamFlag*` tests are (c), the rest (a4) |

No pin, roster or ledger moved. The probe and its runs are measurement only.
