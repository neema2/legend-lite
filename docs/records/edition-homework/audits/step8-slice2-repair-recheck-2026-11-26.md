# Re-check of the slice 2 repair (90d2759b5..origin/claude/edition-step8: 8730b2200, 72cb4d3b9, 5e9a8879b)

**Verdict: 0 BLOCKER, 4 SHOULD-FIX (R1 to R4), plus 5 notes.**

How this was checked: read-only, from the git objects; nothing was built or run. CONFIRMED means I traced the code
path to the answer. PLAUSIBLE means the path is traced but the database's behaviour or the run was not observed.

The repair's own diff touches these files and no others: `PrintTail`, `PrintedRelations`, `PureRelationTextWriter`,
`UpstreamPrints`, `native-claims.tsv`, `PctPlanRoute`, the tests, `PeRatchets`, `ratchets.tsv` and the two records.
`VerdictQueries`, `TestPlanner`, `ClickHouse`, `MongoDb`, `NativeCallStampGuardTest` and `lean-sql/baseline.tsv` change
only in the merge `6a6f5e06d..90d2759b5`, i.e. the merged base `4d44bf54f`, not in the repair (details at the end).

## S1..S7

| Item | Status | Evidence |
|---|---|---|
| S1 two printers in one body | **DEFERRED WITH A RECORD (ruled)** | The design is register S428, ruled 2026-11-26: a last step is legend-pure's text, a value used further is lite's six decimals. The deletion slice makes the audit's two examples agree. `UpstreamPrintsTest.aPrintAtALastStepAndTheSamePrintUsedFurtherInOneBody` holds them, together with the remaining split (`1.0E-7`: 9 at a last step, 3 used further, lite's text `0.0`). The repair changes nothing here, correctly. Side note: lite's used-further text of `1.0E-7` is `0.0`. That is S428's ruled rule, but it reads as "data lost" under the 2026-11-22 amendment and is worth naming to the user once. |
| S2 a value read twice | **FIXED WRONGLY: over-broad and under-broad** (R1) | `UpstreamPrints.java:172-175, 397-414`. The new `unrepeatable` refuses far more than the audit asked for, and still misses a draw hidden in a user function. Details in R1. |
| S3 the tail applied in a writer | **FIXED in code; the guard is trivially passable** (R3) | `PureRelationTextWriter.end` (:104) now calls `receiver.accept(text)` and nothing more. The tail is applied by `PrintedRelations.Lifted.answer` / `Side.answer` (`PrintedRelations.java:63-65, 87-89`) at the two doors: `PctPlanRoute.java:191` (channel A) and `UpstreamPrints.PRINTER` (`UpstreamPrints.java:71-80`, channel B and the corpus). `noWriterAppliesAPrintsTail` (`WriterCarriageGuardTest.java:323`) is a substring check for the token `PrintTail` in a hand list of files. See R3. |
| S4 one function, two meanings | **DEFERRED WITH A RECORD (drafts, not rows)** | The record's last section drafts 5 items for the user (trim, UTF-16 length/indexOf, `replace('')`, indexOf from past an int, a Float held as an integer). Not adding unruled rows is right by AGENTS.md. **Incomplete:** MariaDB/MySQL `startsWith`/`endsWith` under PAD SPACE is missing (R4). |
| S5 engine-door tail | **FIXED (restored to the pre-slice rule)** | `PrintedRelations.java:137-139`: with `values == false`, a tail holding anything but `Replace` gives `null`, so the program is not lifted. That equals the pre-slice outcome: before slice 2, `beneath` stopped at the first non-replace call and `printed()` of that call was null. Tested at unit level, member by member (`PrintedRelationsTest.legendEnginesDoorLiftsThePrintWithItsReplacesAlone`). No end-to-end test holds the engine door's `indexOf` (S134, 1-based) answer for `$rel->toString()->indexOf(..)`. The class javadoc still claims the opposite (R2). |
| S6 a literal round-trips the DB | **FIXED** (one note) | `PrintedRelations.java:173-176`: `Lets.bound(operand, lets) instanceof TypedCFloat` means the text is `PureValueText.pureFloat(literal.value())`. Channel A runs nothing (`PctPlanRoute.java:172-176`). Channel B reads nothing (`UpstreamPrints.java:72-75`) and skips the `unrepeatable` gate (`:172`). Tested in `PrintedRelationsTest` and `UpstreamPrintsTest.aFloatLiteralsPrintIsWrittenAtTheDoorWithoutARead`, with a `Reads` that throws. Note N3 (exact digits). |
| S7 thin tests | **PARTLY FIXED** | Added: `PrintTailTest.whereADatabaseAnswersOtherwiseJavasIsLegendPures` (Java only); `PrintTailParityTest` (DB on the three embedded lanes, plus the edge's own plan); `PureRelationTextTest.aFloatsValuePrintIsLegendPuresOnTheBuildsJdk` (19 values and carriers, **Java only**); the S2/S6/S1 bodies in `UpstreamPrintsTest`. Gaps listed below. |

### What S7 still lacks

1. **The Float value cases are never run through a database.**
   - `aFloatsValuePrintIsLegendPuresOnTheBuildsJdk` feeds Java objects to `PureValueText.value`.
   - Not exercised: a DB-computed `-0.0` (`-1.0 * 0.0`); a DB-computed infinity or NaN; SQLite's NaN arriving as NULL (`PureRelationTextWriter.java:117`, "the answer is empty"); the subnormal or 1e308 transport of each lane; the BigDecimal carriers that ClickHouse, BigQuery and Snowflake really send.
   - The audit asked for Java's text against the database's on real edge cases. That was not done for values.
2. **`PrintTailParityTest.whereJavaAndLitesSqlPart_eachIsHeld` does not hold each lane's answer.**
   - It accepts any of a list (`"3"` or `"4"`; `"2"` or `"3"`), so a lane that flips between the two forms stays green.
   - `catch (RuntimeException refused) -> "REFUSED"` turns any crash into an accepted answer for every case. It should be scoped to the one case, and to the database's out-of-range error.
   - "Measured on every lane" in its javadoc means the three embedded lanes only. MariaDB, MySQL, Postgres and the cloud lanes were not run.
3. **Some whitespace and empty-text cases are not run against the DB.**
   - Java only: trim of `\r`, `\u000b`, `\u000c`, `\u0000`, U+00A0.
   - Not covered: an empty text `''` with each member; regex-special needles (`'a.b'->replace('.', 'x')`, `contains('%')`, `startsWith('_')`). Every product dialect uses a literal REPLACE/INSTR/LEFT, so these agree, but nothing holds them.
4. **The S2 test cannot tell its causes apart and has no positive case.**
   - Its third body (`$g->map(r|$r.n->toOne() * 1.5)->at(0)`) is refused for two reasons at once: the grouping, and the lambda parameter `$r` (R1).
   - No test asserts that a deterministic computed value with a lambda, or a sorted store read, IS still lifted. That is exactly the regression R1 describes.
5. **The `||` short-circuit case was not added.** It is listed for the orchestrator. It is pre-existing for relation sides, and now reaches value sides too.

## New findings

### R1 (SHOULD-FIX, CONFIRMED by code path): `unrepeatable` refuses every value whose computation holds a lambda parameter or reads a store anywhere, even sorted. A user function's draw is not seen.

`UpstreamPrints.unrepeatable` (`:397-414`) pops every node of the operand's subtree. That includes lambda bodies, because `TypedLambda.children()` is its body. It calls `OrderView.of(n, lets)` on each node.

**Too broad (a).** A lambda's parameter `$x` is an unbound `TypedVariable`. `OrderView.of` returns INCIDENTAL for it (`OrderView.java:~202`, with a null hook). So any operand that holds a lambda using its parameter is "unrepeatable".

**Too broad (b).** A `TypedGetAll` inside an `execute` lambda is INCIDENTAL by itself, even beneath a `sortBy`. The walk reaches it after the `sortBy` node (SORTED) and returns true. So every value read from a store is refused, sorted or not. The record says "with no sort", and the code does not do that.

The outcome of a refusal is "left as written", which is lite's six-decimal text (S428's used-further text) at a last step where S428 promises legend-pure's text. The bodies below were Java's (right) after slice 2 and are now wrong. No recorded test holds them, which is consistent with the record's "no recorded answer moved".

- `|assertEquals('0.30000000000000004', [0.1]->map(x | $x + 0.2)->toOne()->toString());`: legend-pure passes; lite now writes `0.3`, so it FAILS.
- `|assertEquals('0.0000001', [1.0E-7]->map(x|$x)->toOne()->toString());`: lite writes `0.0`.
- A corpus-style body `let r = execute(|T.all()->sortBy(#/T/p#)->project([t|$t.p],['p']), m, rt); assertEquals('1.23456789', $r.values->at(0).getFloat('p')->toString())` gets `1.234568`. PLAUSIBLE: the six-decimal text is from S428's wording, not run.

**Too narrow.** `TypedUserCall.children()` is its arguments only. The walk never enters a user function's body, and the same holds for a derived or qualified property's body.

- `function my::draw():Float[1] { random() }` with `|let x = my::draw(); assertEquals($x->toString(), $x->toString());`
- Each side is lifted and read separately (`print` → `readBody` inlines `my::draw()`), so there are two draws and a failing assert where legend-pure passes. This is the audit's own S2 example, one indirection away.

**Fix:**
- Take the order check only at nodes outside lambda bodies. `OrderView.of` already follows lets and descends into an `execute` frame's lambda itself.
- Do not descend past a SORTED node for the order check.
- Keep scanning the whole subtree, lambdas included, for `answersAnew` natives.
- Treat a `TypedUserCall` (and a derived property) as unrepeatable unless its body is scanned too. Alternatively, run the check after `UserCallInliner`.
- Add positive tests: a lambda, and a sorted store read, are still lifted.

### R2 (SHOULD-FIX, CONFIRMED): the class javadoc of `PrintedRelations` contradicts the S5 repair, and two ledger comments are stale.

- `PrintedRelations.java:30-34`:
  - "the tail answers that for every function of the set": S1 shows it does not.
  - "(The tail after a RELATION's print is Java's at that door too: legend-engine has no SQL for a relation's text, so the print and everything after it run in its own Java, which is legend-pure's.)": this is the very recalled claim S5 withdrew, and `lift` (`:137-139`) now does the opposite.
- `:39-40` "the tail's steps are applied to the printed text by the receiver": the opposite of S3's repair.
- `JavaEvalLedgerTest`'s row for `PrintedRelations.java` (`:2036-2042`) still says "whole calls removed, nothing planned, no value computed". The class now applies the tail (`answer` → `PrintTail.applied`) and writes a literal's text (`pureFloat`).
- `UpstreamPrints.PRINTER`'s javadoc (`:68-70`) still credits the writer with "what the tail's last step answers".

Docs only, but the javadoc is where the next reader will take the engine-door rule from.

### R3 (SHOULD-FIX, CONFIRMED): `noWriterAppliesAPrintsTail` catches only the literal token `PrintTail`.

`WriterCarriageGuardTest.java:323-339`. Each of these stays green:

1. The writer calls `new PureRelationText.Replace(a, b).applied(text)`. `Replace` is declared inside `PureRelationText`, a scanned writer file, and the guard strips `implements PrintTail` to allow exactly that file.
2. The writer computes inline with `text.trim()`, `text.length()` or `text.indexOf(..)`. As N2 noted, `theWritersCarryAndDecideNothing`'s vocabulary does not see these.
3. The writer takes a `Function<String,Object>` from the door (`lifted::answer`).
4. A new `ResultSink` outside the hand-listed six edge files and result/exec is not scanned at all.

Fix:
- Make it an ArchUnit dependency rule: no class implementing `com.legend.result.ResultSink`, and no class in result/exec, depends on `PrintTail`, `PureRelationText.Replace`, or `PrintedRelations.Lifted#answer` / `Side#answer`.
- Move `Replace` out of `PureRelationText` into `PrintTail`, so that no writer file holds tail code.

Separately, PrintTail.java's row and file left the vocabulary scan. That is a coverage loosening (see the guard table), dated and justified, but the class that computes is now held only by `PrintTailTest` and the ledger's name list.

### R4 (SHOULD-FIX, PLAUSIBLE: dialect code read, the database not run): an unrecorded Java/SQL part on MariaDB and MySQL.

- MariaDB's session is `utf8mb4_bin`, which is PAD SPACE (`MariaDb.java:42-44` says so). `STARTS_WITH` is `(LEFT(x, CHAR_LENGTH(p)) = p)` and `ENDS_WITH` is `(RIGHT(...) = p)` (`MariaDb.java:2022-2025`).
- Under PAD SPACE, `'a'->startsWith('a ')` is `'a' = 'a '`, which is TRUE. `''->startsWith(' ')` and `'a'->endsWith('a ')` are TRUE too.
- Java (legend-pure) answers false.
- This is neither among the record's five drafts nor in `PrintTailParityTest`'s list, and that lane was not run.

It is a product behaviour older than step 8, but it belongs in the S4 drafts, with a probe on the MariaDB/MySQL lane.

## Notes

- **N1: a literal written at channel A skips the body.**
  - In `PctPlanRoute.java:172-176`, nothing runs for a written literal, not even the lifted body's earlier statements or `DECLARED_SETUP`. A body like `|let a = [1,2]->at(5); 1.5->toString()` would answer `'1.5'` where legend-pure raises, if lite's run would have evaluated the unused let (PLAUSIBLE: not checked whether the product drops unused lets). PCT's bodies are single-expression lambdas, so this is latent.
  - `:184-186`'s comment "the plans compared are the ones the case ran" is false on that path: a plan is recorded for a case that ran nothing. That plan can feed the channel-A SQL-size or plan comparison.
- **N2: equal agreement is accepted as a set.** The parity test's "agreed" cases (`indexOf('', 5)` → 3, `indexOf('b', -5)` → 1, the empty needles) are single-valued and fine. But `'abcb'->indexOf('b', 2147483648)` on H2 raises "Numeric value out of range" where Pure answers 1. That is a raise where Pure answers, which draft 4 should say explicitly.
- **N3: a literal's exact digits are ignored.** `TypedCFloat.exact` (legend-pure's interpreted, BigDecimal-backed Float, `CFloat` javadoc) is not used: `pureFloat(literal.value())` prints the double. This matches compiled legend-pure (and slice 2's state, where the DB's decimal went through `doubleValue()`), but not interpreted legend-pure, for a literal whose digits do not survive a double. Unchanged by the repair; worth one line in S428.
- **N4: relation sides have no non-determinism gate.** `unrepeatable` gates VALUE sides only. A relation side that reads `now()` or `random()` (`$t->extend(~r: x|random())->toString()` printed by two asserts) is still read once per side. This dates from B3 step 2b, not the repair.
- **N5: an orphaned javadoc.** `UpstreamPrints.java:382` has `readable`'s javadoc above `answersAnew`'s, two doc comments stacked. Cosmetic, and pre-existing.
- **On ordering and thread safety: no problem found.** Each side is replaced in place during one tree walk, and `PRINTER` uses a per-call `String[1]`. There is no list of lifted values matched to positions afterwards, so nothing can be mismatched. `PrintTail.applied`'s `(String)` cast is safe by `beneath`'s root-only rule for non-String steps (`PrintTail.java` `beneath`).

## Tail functions: Java against legend-pure and against lite's SQL

Java is `java.lang.String` per member, which is legend-pure's (CompiledSupport/CoreHelper; the prior audit read it).

| Member | Java = legend-pure | Where lite's SQL parts | Recorded? |
|---|---|---|---|
| replace | `String.replace`, literal | `replace(s,'',x)` gives `abc` vs Java `xaxbxcx` (all 3 lanes). Regex-special needles agree: every dialect uses a literal REPLACE. Not tested. | draft 3 |
| length | UTF-16 units | DuckDB and SQLite count code points (3), H2 counts units (4); MariaDB CHAR_LENGTH and ClickHouse lengthUTF8 count code points | draft 2 |
| trim | `String.trim`, every char <= U+0020 | SQL strips spaces only (CSV's last `\n` kept) | draft 1 |
| startsWith / endsWith | `String.startsWith`/`endsWith` | MariaDB/MySQL PAD SPACE: a needle with trailing spaces longer than the text gives TRUE | **not recorded (R4)** |
| contains | `String.contains` | none found (INSTR/strpos/position are literal, and case-sensitive under each session's collation) | n/a |
| indexOf | UTF-16, `from.intValue()` wraps, clamped | after a character above U+FFFF (draft 2); from past an int: -1, or H2 refuses (draft 4) | drafts 2, 4 |

## Guard edits

| Guard | Edit | Kind | Dated and justified? |
|---|---|---|---|
| `WriterCarriageGuardTest` REGISTERED: `PrintTail.java` row (`intValue 1`) removed | the class left the vocabulary scan | **loosening** (one computing class is no longer scanned) | yes, 2026-11-26, the audit's S3/N2; reason sound but see R3 |
| `WriterCarriageGuardTest` scan list: `PrintTail.java` removed | same | loosening; the floor of 50 is unchanged (the remaining count is assumed above 50, not run) | yes |
| `WriterCarriageGuardTest.noWriterAppliesAPrintsTail` | new test | tightening (weak, R3) | yes |
| `JavaEvalLedgerTest` | comment text only on the `PureRelationText`/`Writer` row | like-for-like (no list change) | yes; but `PrintedRelations`' row is stale (R2) |
| `ratchets.tsv` `own_corpus.matched` 3359 → 3368, plus `PeRatchets.HISTORY` (+1 string) | a floor rising | tightening; `PeRatchets` changes a pin's history list only, no logic | yes, dated; under the user's standing word "parser bumps are always okay". "The nine not traced one by one" is a thin justification. |
| `native-claims.tsv` (3 lines) | regenerated ledger: `today`, `random` and `generateGuid` now name `UpstreamPrints` | generated, not a guard move | yes |
| (merged base) `NativeCallStampGuardTest` VerdictQueries 6 | comment only, the count is unchanged | like-for-like | yes (`17a6e9f1d`) |
| (merged base) `lean-sql/baseline.tsv`, 2 ClickHouse rows | 774/3646 → 772/3644 (abs_Float), 10411/41663 → 10409/41661 (toString_Any) | **down** (2 chars each: `toFloat64('…')` → `toFloat64(1.3421e-8)`, `e1d491e40`) | regenerated (`f839bbfb3`) |

## Merged-base files (not the repair)

- `ClickHouse.java`: an extreme-magnitude Float literal is the number, not its text parsed (`e1d491e40`).
- `MongoDb.java`: `$asin`/`$acos` in-domain arms read the operand through the row (`b4c4f63d7`).
- `VerdictQueries.java`, `TestPlanner.java`: javadoc only (`17a6e9f1d` and its kin).

None of these is in `90d2759b5..edition-step8`.

## What I could not determine without running

- That `GuardCoverage.assertFloor(..., 50)` still holds after `PrintTail.java` left the scan.
- What lite's six-decimal text actually is for R1's store-read example on each lane.
- Whether lite's run evaluates an unused earlier let (N1).
- MariaDB/MySQL PAD SPACE answers for R4.
- Each cloud and server lane's answers for the tail table.
- Whether the corpus or PCT holds any body that R1's over-refusal now moves. The record says all five recordings were unchanged; I did not re-run them.
