# Independent audit: step 8, slice 2 (d5080e3f, "a print's tail and a Float's last-step print")

Read-only audit of `git diff d5080e3f^1 d5080e3f -- core/src/main core/src/test pct` in /home/user/ll-edit, against
AGENTS.md (identical to legend-lite's), the author's records (slice2-value-lift-and-tail.md, print-tail-proposal.md,
moves-if-float-text-used-further-is-lites.md, slice1-float-shortest-text.md) and legend-pure 5.99.0's Java (the Bazel
cache's `legend_pure_src`). Nothing was built or run. CONFIRMED means I traced the code to the answer; PLAUSIBLE means
the code path is traced but a database's or upstream's behaviour was not run.

What I checked and found right: `PureValueText.pureFloat` is byte-for-byte legend-pure's `primitiveToString(double)`
(CompiledSupport 130-137, 1271-1274: `0.0` for either zero, `DecimalFormat("0.0")`, English, 340 fraction digits,
so NaN `NaN`, infinities `∞`/`-∞`, no exponent at any magnitude, `Double.MIN_VALUE` written in 324 fraction digits,
2^53+1 written as the double ...992). Each tail member is legend-pure's Java: `CompiledSupport.length/indexOf/
startsWith/replace`, `CoreHelper.contains/endsWith/trim`, the interpreter's `Length`, `Trim`, `IndexOfString`. No main
source outside `com.legend.edge` changed (plus the generated `native-claims.tsv`), and the product's road does not see
the lift, so tenet 1 for the product, G5 and tenet 6 hold. No legend-engine audience gets the value lift
(`PctPlanRoute` passes `audience == LEGEND_PURE`; channel B runs legend-pure's audience only; the corpus's
`UpstreamPrints` reads test code outside `execute()`, which is legend-pure's).

---

## Blockers

None that turns an ordinary, recorded test wrong. Two findings below (S1, S2) produce a wrong verdict on short,
plausible bodies; I rank them should-fix only because no recorded test holds them today.

## Should-fix

### S1. Two printers at one door again: a Float's text is now Pure's at an assert argument and legend-engine's cast one call further, inside the same body. CONFIRMED (code path); answers PLAUSIBLE per database

`UpstreamPrints.substituted` (UpstreamPrints.java ~158-197) reads a Float print only where it is an assert's own
argument (`side(n, assertArgument)`, with `assertArgument = isAssert(parent)`). One call further up, the same print is
left to the edge's SQL, whose pass `PurePrintForms.floatOf` writes legend-engine's in-query cast `CAST(x AS VARCHAR)`
(PurePrintForms.java ~95-104), and for a literal on H2 `CAST(CAST(<print> AS FLOAT) AS VARCHAR)` (`PRINT_AS_FLOAT`).

- Body (channel B or a corpus body, DuckDB): `let x = 1.0E-5 * 1.0; assertEquals($x->toString()->length(), $x->toString()->length() + 0);`
  - left side, an assert argument with tail [Length]: Java, `"0.00001"`, so 7;
  - right side, under `+`: the database's cast, `"1e-05"`, so 5;
  - so the assert fails. legend-pure passes (7 == 7). Before this slice both sides were the cast (5 == 5), so it passed.
- On H2 the window is wide and ordinary: H2's cast is `Double.toString`, which uses an exponent from 1e7. So with
  `x = 12345678.5 * 1.0`, `assertEquals('12345678.5', $x->toString())` now passes, but
  `assertEquals('v=12345678.5', 'v=' + $x->toString())` gets `v=1.23456785E7`.
- A literal on H2 behaves the same way: `1.3421e8->toString()` at a last step is `134210000.0`; with `+ ''` after it,
  it is `1.3421E8`.

This is the objection that held S6a on 2026-11-25 ("two printers at one door"). The record says the tail "answers
that for every function of the set". It does so only for the seven functions. `+`, `joinStrings`, `format` and a
let are the common ways a text is used further, and the proposal's own reach table says 54 corpus sides are "a
toString joined to a computed value". The fix is the next slice (a Float's text used further becomes lite's own),
which needs the user's word on the S(next) row. Until it lands, the inconsistency should be posted to the user as a
known interim state, not described as answered.

### S2. The pre-read reads a Float value twice, which order or randomness can make differ. Before, only a relation side was read, and the order guard protected it. PLAUSIBLE

`UpstreamPrints.incidental` (diff: UpstreamPrints.java ~262-267) now returns false for `Form.VALUE` ("a value's print
has no row order"). But a scalar is often taken from rows whose order the program does not set, and the pre-read is a
second execution of the lets (`readBody(side.relation(), lets)`), separate from the body's own run:

- `let r = execute(...)` with no sort, then `assertEquals('1.5', $r.values->at(0).price->toString()); assertEquals('Bob', $r.values->at(0).name);`
  The first side's row comes from the pre-read's execution. The second assert's row comes from the body's
  execution. This is exactly the hazard the `OrderView.INCIDENTAL` guard exists for on relation sides, and a value
  taken by `at(0)`, `first()` or `toOne()` of such rows now bypasses it.
- `let x = random(); assertEquals($x->toString(), $x->toString());`: each side is its own pre-read, so two draws and a
  failing assert. legend-pure passes. `random()` is a lite native (native-claims.tsv:583).
- A sum of doubles that a parallel database adds in a different order gives a different last digit, which the Java
  print now always shows.

Fix: apply `OrderView` (or a "deterministic value" check) to the value's operand as well, and refuse to lift a value
whose lets or operand call a non-deterministic native.

### S3. The tail is applied inside a result writer, against the writer charter. CONFIRMED

`PureRelationTextWriter.end()` now calls `PrintTail.applied(text, tail)` (PureRelationTextWriter.java ~103) and
hands the receiver a Long or a Boolean. `WriterCarriageGuardTest`'s javadoc states the rule: "swap the writer and
every value is unchanged... The writer that compares or computes decides output the plan never typed". Here the plan
typed a Float, or a relation, and the writer answers `Boolean true`. The class's own javadoc still says "A writer: it
spells, decides nothing".

The user ruled that the tail is Java's. The user did not rule that a result sink computes it. Fix: have the writer
hand back the text, and let the door (`PctPlanRoute`, `UpstreamPrints.PRINTER`) apply the tail. Then the writer
charter and `ArchitectureTest.writersAreCarriageOnly`'s intent hold without an exception.

### S4. One function, two meanings at one door; the product's two divergences from Pure are unledgered. CONFIRMED

The tail's `trim`, `length` and `indexOf` are Pure's (Java). The same call one step away from a print is lite's SQL:

- `trim`: lite's SQL strips spaces only.
  - At legend-pure's door, `'a\n'->trim()` is `"a\n"`, but `$r->toCSV()->trim()` strips the CSV's last `\n`.
  - `$r->toCSV()->trim() + ''` keeps the `\n` again: the print is read by `UpstreamPrints` and the trim runs in the
    database over the literal.
- `length`: the database counts code points, Java counts UTF-16 units.
  - For a relation with a cell `a😀`, `$r->toString()->length()` counts the emoji 2.
  - `$r->toString()->length() + 0` counts it 1.
  - Before this slice both counted 1, so the answer moved without a register row.
- `indexOf`: the same split, after any character above U+FFFF.

AGENTS.md says "a divergence that is not a row is a defect". The record says the orchestrator "filed" the trim
defect. I found no row for `trim` or for `length`/`indexOf` in UTF-16 units in docs/SEMANTICS_REGISTER.md. These
need rows (or a product fix) before this lands. Otherwise the edge's lift hides a product defect behind a
position-dependent answer.

### S5. At legend-engine's door, a relation print's tail moved from the door's table to Java without asking the engine runner. PLAUSIBLE

`PctPlanRoute` now lifts `lift(query, false)` with the whole tail for legend-engine's audience. Before, only
`replace` was lifted.

- `$rel->toString()->indexOf('x')` was the edge's SQL. Under `EngineTable` (EngineTable.java:117, `SQL_POSITION`,
  register S134), legend-engine's indexOf there is 1-based with 0 on a miss. It is now Java's: 0-based, -1 on a miss.
- `trim` and `length` move the same way.

The justification ("legend-engine has no SQL for a relation's text, so the print and everything after it run in its
own Java", PrintedRelations javadoc) is recalled, not read or run. The upstream-fidelity tenet requires both answers
to be READ from the pinned sources or ASKED of the engine runner. No PCT case holds this today (the proposal's
reach: one tail in all of PCT), so it is latent. It should be measured on the engine lane, or the lift kept to
`replace` at that door until it is.

### S6. A literal's print now round-trips through the database. Before, it was a constant the edge wrote. CONFIRMED (mechanism); databases PLAUSIBLE

Before this slice, a Float literal under `toString` at legend-pure's door was written by `PurePrintForms.FloatLiteral`
as legend-pure's print, an exact `DecimalLit` (`PRINT_BARE`), so no database parsed the double. Now `|1.3421e-8` is
run with the product's literal spelling and printed from the returned double, so every database's literal parse and
transport reaches the text:

- ClickHouse's `toFloat64('1.3421E-8')` was one ULP off. This is the lane finding. e1d491e4 fixed the extreme-
  magnitude spelling after this slice.
- BigQuery still writes an extreme magnitude (|x| < 1e-6 or >= 1e15) as a text parse, `CAST('<v>' AS FLOAT64)`
  (BigQuery.java ~797). This is the same class of risk and is not measured.
- SQLite parses `plainFloat` with `sqlite3AtoF`, which is not guaranteed correctly rounded on every build.

A literal is a fact of the program (the "Folds" reading permits folding constants). The lift could print a literal
operand straight from the typed literal, and send only computed values to the database. That removes the whole
class of risk for the commonest case (PCT's 14 Float last-step prints are mostly literals).

### S7. The tests hold the Java path only on easy inputs; the parity test compares against the wrong old path. CONFIRMED

- `PrintTailParityTest` compares Java with the PRODUCT's SQL (`Lean.run`, no edge style). The programs the slice
  changed used to run on the EDGE's SQL (legend-pure's door: `PurePrintForms`; legend-engine's door: `EngineTable`'s
  1-based indexOf). No test compares the lifted answer with the answer the door gave before.
- It also skips exactly the inputs where Java and SQL part:
  - `trim` on a text with a control character at either end;
  - no character above U+FFFF;
  - no empty needle;
  - no `from` below 0 or past the end;
  - lanes DuckDB, H2 and SQLite only. MariaDB/MySQL (PAD SPACE, `utf8mb4_bin`) and the cloud lanes are not run.
- Missing cases, with legend-pure's (Java's) expected answer:
  - over the text `a😀b`: `length()` -> 4 (the database says 3); `indexOf('b')` -> 3 (the database says 2);
  - `'abc'`: `indexOf('')` -> 0; `indexOf('', 5)` -> 3; `startsWith('')`, `endsWith('')`, `contains('')` -> true;
    `replace('', 'x')` -> `xaxbxcx`;
  - `trim()` of `"\u0000a\u001f"` -> `a`; of `" a"` -> unchanged (Java does not strip NBSP);
  - `'abcb'->indexOf('b', 2147483648)` -> 1 (legend-pure's `intValue()` wraps to `-2147483648`, which clamps to 0);
    lite's SQL says -1.
- `PureValueText.value` / `PctPlanRoutePrintTest` hold three values. Missing cases, with the expected text:
  - `-0.0` -> `0.0`;
  - `1.0E7 * 1.0` -> `10000000.0` (H2's cast said `1.0E7`);
  - `1.0E-4 * 1.0` -> `0.0001`;
  - `1.0E15 * 1.0` and `1.0E16 * 1.0` -> `1000000000000000.0` and `10000000000000000.0`;
  - `9007199254740993.0` -> `9007199254740992.0`;
  - `1.7976931348623157E308` -> 309 digits and `.0`;
  - `4.9E-324` -> `0.` followed by 323 zeros and `49`;
  - a Float carried as a Long (`asFloat(Long 1000000)` -> `1000000.0`) and as a BigDecimal (ClickHouse, BigQuery,
    Snowflake);
  - NaN and the infinities, each lane's carrier (SQLite's NaN arrives as NULL, so the writer raises "the answer is
    empty").
- Behaviour tests missing:
  - S1's assert at an argument against the same print one call up;
  - S2's `random()` read twice;
  - an assert under the right of `||` whose Float side raises (the pre-read raises where Pure short-circuits; the
    record's claim "which the assert always evaluates" is false for an assert that is not itself evaluated);
  - legend-engine's audience with a relation print and `indexOf` or `trim` (S5).

## Notes

- N1. The record's "No recorded answer moved" is contradicted. The two H2 pins (legend-engine's H2 failure of
  `testFloatToString...Exponent`, which legend-pure's door on H2 used to reproduce through `PRINT_AS_FLOAT`) began to
  pass. That is correct by tenet 1, but the measurement that missed it should be named, and the pins moved with a
  dated note.
- N2. `WriterCarriageGuardTest` now "scans" PrintTail.java, but its vocabulary matches `startsWith/endsWith/contains/
  indexOf` only with a literal argument (`\(""\)`), and never `length()`, `trim()` or `replace(`. So the class's
  actual computations are invisible to it, and the one registered hit (`intValue`) is the least of them. The real
  pins are `PrintTailTest.theClosedSetIsExactlyThese` and the `JavaEvalLedgerTest` file entry. Neither is a
  widening beyond what the user ruled, but the scan's coverage should not be cited as a guard on the tail.
- N3. The `JavaEvalLedgerTest` entry is an allowance added, not a reduction. It is backed by the dated ruling
  ("yes should add guard"), as AGENTS requires. `native-claims.tsv` is a regenerated ledger (`ClaimRegistryTest`),
  not a guard that moved.
- N4. The value lift's Integer carrier: `asFloat(Long)` prints `1000000.0`. In a corpus body, a Float property that a
  store holds in an INT column reaches legend-pure as a Long through legend-engine's result and prints `1000000`
  (compare the probe's row 5, which is inside a store query and not lifted). Here lite answers Pure's meaning, which
  is defensible, but it is a departure from legend-engine's answer at that door and has no row. PLAUSIBLE: no corpus
  case is known to hit it.
- N5. Item 6 of the brief. Where databases can now print different texts for one program, where their casts used to
  hide it:
  - the DECIMAL-literal lanes (ClickHouse, BigQuery, Snowflake): `(0.1 + 0.2)->toString()` is `0.3` there and
    `0.30000000000000004` on DuckDB, H2 and Postgres. Unchanged by the slice (both paths show it), but now on the
    "Pure's text" route;
  - transcendental functions (`exp`, `log`, `sin`, `pow`) whose last bit differs between glibc and `java.lang.Math`
    (H2 uses Java's, so it matches legend-pure). Every digit is now shown on every lane, including any lane whose old
    `floatToText` cast printed 15 significant digits (SQLite's `CAST AS TEXT` is `%!.15g`);
  - transport: a cloud lane whose driver sends DOUBLE as text with fewer than 17 digits (Snowflake's JSON result
    format, BigQuery's REST) gives Java a rounded double. Before, the cast's text was the answer itself. PLAUSIBLE,
    not measured.
- N6. JDK dependence: DecimalFormat's digits for a double changed in JDK 21 (JDK-8300869, the shortest-digits
  algorithm). For example, `2.0E23` prints `200000000000000010000000.0` under legend-pure's older JDK and
  `200000000000000000000000.0` under a newer one. The class comment "nothing that depends on the machine it runs on"
  is true of the tail and not quite of the print. This is pre-existing in the cell printer; the value lift widens its
  reach. PLAUSIBLE: which JDK legend-pure's reference answers were made with was not checked.
- N7. Reach that differs by how the body is written:
  - an assert inside a user helper function is not seen by `UpstreamPrints` (it walks the typed body before G½), so
    the same Float print is Java's when written inline and the cast when written in a helper;
  - a let-bound print returned or asserted unchanged (`let s = $x->toString(); assertEquals('0.00001', $s)`, or
    channel A's `|let s = ...; $s`) is treated as "used further", though nothing computes with it.

  Both are consistent between channel A and channel B, but each is a second answer for one meaning.
- N8. PrintTail javadoc line 46-47: "only a String can be read further ... (the typer already holds a program to
  that)". The guarantee is actually `PrintTail.beneath`'s own check (`!answers(List.of(step)).equals("String")`),
  not the typer. It is correct, but the cast in `applied` relies on it.

## What I did not read

- The bodies of `OrderView`, `Lets.bound`, `readBody`/`readable`, `UpstreamTestBodies`, `MinimalCorpus`'s use of the
  reads, `ChannelB` beyond its two `typed` methods, `PureText`'s twelve `floatToText` overrides, `EdgePureOnlyTexts`.
- The ArchUnit rules named above (`writersAreCarriageOnly`), `ClaimRegistryTest`, `core/edge_tests.bzl`.
- The later commits on the branch (only e1d491e4's ClickHouse fix, by its message and the current dialect code).
- No database was run. Every database-behaviour claim above (DuckDB's or H2's cast text, the parses on BigQuery and
  SQLite, the cloud transports, MariaDB's PAD SPACE) is from the code comments or general knowledge, and is marked
  PLAUSIBLE.
