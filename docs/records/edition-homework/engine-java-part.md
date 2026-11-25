# legend-engine's Java part: a value's text and the raises, asked (B2, 2026-11-25)

Asked at B3's request (the R1 relay of 2026-11-25, questions (a) to (c); (c) on H2 and DuckDB) for R4 (the texts as
edge rules) and the placement "part" the typer passes (the user's ruling of 2026-11-25: legend-engine's own part is the
code that reads no store, which legend-engine runs in Java). Documentation only; recorded, not built on.

**How:** legend-engine 4.145.0's own planner and executor, from `//tools/engine-runner:testable`'s jars (the recipe in
`docs/result-boundary-homework/README.md` §7; the probe `docs/result-boundary-homework/probes/EqualityProbe.java.txt`,
unchanged). Each query below reads no store, so legend-engine plans it as a platform node, generated Java
(`_pure.plan.root.Execute`), and runs it in Java: the "legend-engine's own part" of the ruling.

## (a) A Float's and a DateTime's `toString` in a `let` that reads no store

| Query | legend-engine answers |
|---|---|
| `\|let x = 1.3421e-8; $x->toString();` | `0.000000013421` |
| `\|let x = 1.3421e-8 * 1.0; $x->toString();` (computed) | `0.000000013421` |
| `\|let x = 0.1 + 0.2; $x->toString();` | `0.30000000000000004` |
| `\|let x = 1.0E23 * 1.0; $x->toString();` | `99999999999999990000000.0` |
| `\|let x = 2.0; $x->toString();` | `2.0` |
| `\|let x = 1000000.0 * 1.0; $x->toString();` | `1000000.0` |
| `\|let d = %2014-01-01T10:20:30.5; $d->toString();` | `2014-01-01T10:20:30.5+0000` |
| `\|let d = %2014-01-01T00:00:00.000+0000; $d->toString();` | `2014-01-01T00:00:00.000+0000` |
| `\|let d = %2014-01-01T10:20:30.5->adjust(1, DurationUnit.DAYS); $d->toString();` | `2014-01-02T10:20:30.5+0000` |
| `\|let d = %2014-01-01; $d->toString();` | `2014-01-01` |

So legend-engine's Java part prints a value exactly as compiled legend-pure does:
- a Float by `DecimalFormat("0.0")` with up to 340 fraction digits, never an exponent, legend-pure's
  `CompiledSupport` (B2's `edge/PureValueText.pureFloat` is that print);
- a DateTime at the precision it was written, `+0000` from the minute on, kept through `adjust`.

Inside SQL it is the database's cast instead (the pure/v1 oracle's `value.sizeToString`, B2's record), so the same
`toString` answers differently by part.

## (b) Each raise kind in the Java part

Each is the platform node's failure, `RuntimeException: Failed in node: root`, caused by:

| Query | Cause |
|---|---|
| `\|let x = 1; $x / 0;` | `RuntimeException: Cannot divide by zero` |
| `\|let x = 7; $x->rem(0);` | `RuntimeException: Cannot divide by zero` |
| `\|let l = [1, 2]; $l->at(5);` | `IllegalStateException: The system is trying to get an element at offset 5 where the collection is of size 2` |
| `\|[]->cast(@Integer)->toOne()` (also `\|[1, 2]->filter(x\|$x > 5)->toOne()`) | `IllegalStateException: Cannot cast a collection of size 0 to multiplicity [1]` |
| `\|let l = [1, 2]; $l->toOne();` | `IllegalStateException: Cannot cast a collection of size 2 to multiplicity [1]` |
| `\|let x = -2.0; sqrt($x);` | `RuntimeException: can't compute sqrt for input: -2.0` |
| `\|let s = 'abc'; $s->parseInteger();` | `NumberFormatException: For input string: "abc"` |
| `\|let s = 'abc'; $s->parseFloat();` | `NumberFormatException: For input string: "abc"` |
| `\|let d = %2017; $d->hour();` | **no raise: answers `-1`** |

Two notes:
- **`toOne` of an empty value bound by a `let`** (`\|let l = []->cast(@Integer); $l->toOne();`, and the `filter` form)
  does not run at all. legend-engine's generated Java fails to compile: `JavaCompileException: 2 errors compiling
  /_pure/plan/root/Execute.java` ("illegal start of type", "invalid method declaration"). Written inline, without the
  `let`, it raises the sentence above.
- **A missing date part answers `-1`** in legend-engine's Java part. Its SQL part (the pure/v1 golden
  `raise.datePartAbsent`, `%2017->hour()` in an `extend`) refuses at planning instead: `PureAssertFailException`,
  "Ensure the target system understands Year or Year-month semantic." on H2, "DuckDB doesn't support YEAR and
  YEAR-MONTH" on DuckDB (`tools/engine-runner/src/test/resources/pure_v1/engine_*_answers.tsv`). So the same expression
  answers or is refused by part.

## (c) `testBigFloatAbs`'s value, `abs(-123456789123456789.99)`

PCT's `testBigFloatAbs` asserts `assertEq(123456789123456789.99, $f->eval(|abs(-123456789123456789.99)))`. Asked of
legend-engine in both parts, on H2 (the pure/v1 goldens' trades model) and DuckDB (the shared DuckDB file the goldens
seed), each store-bound form an `extend` over one row of `trades::h2::DB.TRADES_SCHEMA.TRADES`:

| Request | H2 | DuckDB |
|---|---|---|
| SQL part: `extend(~a: r\|abs(-123456789123456789.99))` | `1.2345678912345678E17` (JSON number) | `123456789123456784.0` (a decimal) |
| SQL part: the literal alone, `extend(~l: r\|123456789123456789.99)` | `1.2345678912345678E17` | `123456789123456784.0` |
| SQL part: `abs(-123456789123456789.99) == 123456789123456789.99` | `true` | `true` |
| SQL part: `abs(-123456789123456789.99)->toString()` | `"1.2345678912345678E17"` | `"123456789123456784.0"` |
| Java part: `\|abs(-123456789123456789.99)` | `1.2345678912345678E17` | (no store: the same JVM node) |
| Java part: `== 123456789123456789.99` | `true` | (the same) |
| Java part: `->toString()` | `"123456789123456784.0"` | (the same) |

The SQL legend-engine writes:
- H2: `select abs(-cast(123456789123456784.0 as float)) as "a" ...`, a double;
- DuckDB: `select abs(-123456789123456784.0) as "a" ...`, the literal bare, which DuckDB types as a DECIMAL, so it
  computes `abs` exactly on the double's printed digits and answers that decimal. So the same `toString` answers
  `1.2345678912345678E17` on H2 (a double's cast) and `123456789123456784.0` on DuckDB (a decimal's cast).

What this says:
- **legend-engine holds the literal as a double in both parts.** Its parser reads `123456789123456789.99` as the double
  `1.2345678912345678E17`, and its SQL spells that double's legend-pure print (`123456789123456784.0`), under a FLOAT
  cast on H2 and bare on DuckDB. The assert compares the double with itself, so it holds: `true` in both parts. Only
  legend-pure's interpreter keeps the twenty digits (register S120).
- **legend-pure's Float print of this double is `123456789123456784.0`, not the shortest digits.** The Java part's
  `toString` and the SQL literal both write it so, and the JDK's `DecimalFormat("0.0")` (legend-pure's
  `CompiledSupport` print, and the edge's `PureValueText.pureFloat`) writes it so on this machine's JDK 21
  (`Double.toString` gives the shortest, `1.2345678912345678E17`). Register S120's parenthesis, "`123456789123456780.0`
  (legend-pure's own print of that double, `DecimalFormat("0.0")`)", is therefore not what `DecimalFormat` writes:
  `123456789123456780.0` is the shortest digits laid out, the text lite's lanes print. Posted to the orchestrator, who
  ruled it a factual correction: S120's parenthesis corrected the same day, with a dated note.
- PCT's relational adapter (legend-engine's `core_external_test_connection/pct_relational.pure`) reads a result's
  number through JSON as a double (json-simple; `ValueBridge.engineTDS`'s javadoc), so it sees the same double.
