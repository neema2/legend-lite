# Audit PCT-B — WRONG-ROWS / VACUOUS PASSES (range dc6086d1..HEAD, pct-native)

Scope: the six pct-native commits (A1 parse-wall burn, A1c wildcard, A2
execution, A2b native-replaces-stub, bare multi-statement lambdas, A3
assert vocabulary + `#TDS` presentation native) against
`docs/PCT_NATIVE_PLAN.md`. Method: hunk-by-hunk read of the diff + live
probes through the real pipeline (disposable JUnit probes under
`pct-corpus/`, built model + `TestBody.run` + DuckDB in-memory, deleted
after the audit). Every probe verdict below is the printed
`TestBody.Outcome` of an actual run.

## Verdict

**The 968 PASS column is not inflatable by zero-assert or advisory-only
bodies (verified==0 always classifies SHAPE), but two real vacuity
channels and one silent-wrong-value channel exist.** (1) **Every ordering
contract in the sweep is vacuous**: the order policy's `endsInSort` does
not see through the `$f->eval(|...)` adapter wrapper, so every PCT assert
falls back to multiset comparison — probe-verified that
`assertEquals([2,1], $f->eval(|[1,2]->sort()))` PASSES (list and grid
forms). A broken `sort`/`reverse`/ordered-window would still pass every
non-toString PCT assert. (2) **`assertError` counts compile-stage gaps as
the asserted runtime error** — probe-verified wrong-PASS when the
expected string equals a legend-lite resolver message. (3) **relation
`toString` outside the single K result-position arm silently scalarizes
the relation** (new catalog entries joined a name-family CAST lowering):
`'a' + $rel->toString()` yields `'a1'`, `$rel->toString(true)` yields the
cell value — silent wrong values, loud only for multi-row relations. The
parse-and-drop family is mostly honest (derived-prop reads of dropped
type variables throw; unused drops are invisible), with two overstated
claims: constraints referencing a dropped `$x` are census-walled but do
NOT gate the tests that use the class (constraints are never enforced at
instantiation anyway), and class multiplicity params silently accept any
cardinality. Counts: 0 confirmed wrong-value PASSes in today's ledger,
2 high vacuity/silent-value channels, 3 medium, rest low/info.

---

## Findings

### F1 — ORDER CONTRACTS ARE VACUOUS THROUGH THE EVAL WRAPPER (HIGH, verified by probe)

`endsInSort` recognizes order-preserving tails (`map`, `limit`, `take`,
…, `toString`) but not `eval`
(`core/src/main/java/com/legend/harness/TestBody.java:1790-1808`). Every
PCT assert's actual side is `$f->eval(|...)` (the runner's adapter
binding, `pct-corpus/src/test/java/com/legend/pctcorpus/PctCorpusRunner.java:120-128`),
whose root after let-substitution is the `eval` call — so
`sortedChain=false` for **every** PCT assert, and `compare()`
(`TestBody.java:1187-1272`) falls back to multiset after the ordered
first-try.

Probes:

- `let f = {g|$g->eval()}; assertEquals([2, 1], $f->eval(|[1, 2]->sort()))`
  → **`Ran[verified=1, failures=[]]` — PASS with a reversed expectation.**
- same shape with `#TDS` grids both sides (`gridEquals` under the order
  policy) → **PASS reversed** as well.
- control without the wrapper: `assertEquals([2, 1], [1, 2]->sort())` →
  FAIL (root `sort` → `sortedChain=true` → strict ordered). Asymmetry
  confirmed.

Consequence: all PASSes for `sort.pure`, `reverse.pure`, ordered `range`,
and the value-side of every ordered-window test verify **membership
only**. A sort that returned rows in any permutation would not change a
single scoreboard number outside the toString-pinned asserts (F9). The
order policy is a documented leniency for *unsorted* chains; here it
swallows chains that ARE sorted and whose order is the very contract
under test. Fix shape: let `endsInSort` see through `eval` into the thunk
body (the thunk's last expression is the chain).

### F2 — relation toString OUTSIDE the K arm silently scalarizes the relation (HIGH, verified by probe)

The branch adds two catalog natives
(`core/src/main/java/com/legend/builtin/Pure.java:1229-1230`,
`native-catalog.txt:509-510`). The K presentation arm fires only for a
RESULT-position root with **exactly one** argument
(`core/src/main/java/com/legend/StatementExecutor.java:152-163`,
`rts.args().size() == 1`). Everything else falls through to the
pre-existing **name-family** lowering registration `for (String f :
Pure.nativeKeysAt("toString"))`
(`core/src/main/java/com/legend/lowering/Scalars.java:1802`), which the
new relation keys silently joined: the relation argument lowers as a
scalar subquery and casts to VARCHAR.

Probes:

- nested: `assertEquals('a#TDS…', 'a' + #TDS id 1#->toString())` → got
  **`a1`** — the single cell, not the grid. Silent wrong value.
- two-arg, result position: `#TDS id 1#->toString(true)` → got **`1`**.
  (The 2-arg `typesAndMuls` form never reaches the K arm at all.)
- multi-row nested: loud
  `SQLException: More than one row returned by a subquery` → honest ERROR.

So the silent channel is exactly single-row/single-column relations —
which is also the shape most likely to *coincide* with an expected scalar
string and wrong-PASS. Today's corpus impact is loud (the
`expected a Relation, got Integer` ERROR ledger entries and the variant
quoting FAILs), and the corpus's own `testToStringWithTypesAndMuls` is
`<<test.Test>>`, not `<<PCT.test>>`, so it is not even discovered — the
2-arg overload's wrongness has **zero witnesses in the sweep**. Fix
shape: exclude `RELATION_TO_STRING` keys from the Scalars name-family
loop (or make their rule throw `NotImplementedException`), and either
K-dispatch the 2-arg form or wall it loudly.

### F3 — assertError counts compile-stage gaps as the asserted error (MEDIUM, wrong-PASS verified by probe)

`TestBody.java:653-668`: the thunk's statements run through `evalScalar`
and **any** `RuntimeException` is caught and message-compared — including
resolver/typer failures that in real pure would be compile errors of the
test itself, and in legend-lite are platform gaps.

Probes:

- `assertError(|unknownFn123(1), 'unknown function \'unknownFn123\'')` →
  **`Ran[verified=1, failures=[]]` — PASS.** A name-resolution gap
  counted as the asserted runtime error.
- mismatching expected → FAIL `expected 'boom', got 'unknown function…'`
  — a platform gap booked as a **FAIL** (wrong bucket; should be
  SHAPE/ERROR).
- thunk succeeds: `assertError(|1 + 1, 'boom')` → FAIL
  `assertError: no error was raised (expected 'boom')` — message correct.
- 4-arg positional form: line/col args are ignored entirely
  (`assertError(|…, msg, 5, 10)` passes) — matches the documented
  P3 "positional pins advisory" stance (`docs/PCT_NATIVE_PLAN.md`), OK.

The realistic wrong-PASS: legend-lite deliberately matches pure's error
texts, so a typer/resolver-stage guard that emits the same words as
pure's *runtime* raise (cast errors, multiplicity errors) will satisfy an
`assertError` even though the semantic (a runtime raise at evaluation)
never happened. The NIE rethrow at `TestBody.java:660-661` is honest but
its comment ("stays SHAPE") is wrong: `NotImplementedException extends
RuntimeException`, it propagates out of `TestBody.run` and lands in the
runner's `catch (RuntimeException…)` → **ERROR** bucket
(`PctCorpusRunner.java:139-144`) — loud either way, just mislabeled.
Fix shape: pre-compile the thunk (resolve+type) OUTSIDE the try so
compile-stage failures propagate as ERROR, then only execution failures
compare.

### F4 — assertTdsEquivalent: strict order (asymmetric with F1), TZ-skewed temporal classification, engine-vs-engine emptiness (MEDIUM, verified by probe)

`TestBody.java:1615-1653` (`tdsEquivalent`, `epochSeconds`).

- **Ordered zip with no order policy**: reversed rows FAIL
  (probe: `cell a[0]: 1 vs 2`). Correct per the engine's
  `tdsEquivalent.pure`, but the actual side of a PCT
  `assertTdsEquivalent` is a DuckDB query with no ORDER BY guarantee: a
  pass is DuckDB-order-luck (deterministic per version, unspecified by
  contract), and a wrong-FAIL on another version/machine. Note the
  asymmetry: `assertEquals` grids get the multiset policy (too lenient,
  F1), `assertTdsEquivalent` gets none (too strict).
- **`epochSeconds` mixes UTC and JVM-local epochs**:
  `LocalDate.atStartOfDay(UTC)` vs `java.sql.Timestamp/Date.getTime()`
  (wall time in the **JVM default TZ** — the runner's
  `SET TimeZone='UTC'` governs DuckDB, not JDBC materialization). Probe:
  StrictDate `2010-01-01` vs DateTime `2010-01-01T00:00:00`, timeDelta 0
  → **FAIL** (`2010-01-01 vs 2010-01-01 00:00:00.0`) on this
  (non-UTC-JVM) machine; the same test PASSES on a UTC CI box. Mixed
  date/datetime comparisons are machine-dependent, and any
  `timeDeltaInSeconds ≥ |local offset|` silently absorbs a real
  off-by-offset bug.
- **Both-empty relations PASS and count verified** (probe:
  header-only grids → `Ran[verified=1, failures=[]]`). Equality of
  empties is real semantics, but BOTH sides run through the same engine —
  a shared row-dropping defect wrong-passes. The `emptinessUnverifiable`
  guard exists but the runner always passes `false`
  (`PctCorpusRunner.java:127`).
- Sound parts, verified: computed delta →
  `Unsupported[assertTdsEquivalent/3 …]` (SHAPE, the `literalNumber`
  guard works); column-name ordered-exact compare covers column count.
  Column TYPES are not compared (names only) — a cell-level
  `Number`/`wireEquals` coincidence across differently-typed columns can
  pass; low.

### F5 — the parse-and-drop family: mostly honest, two overstated claims (MEDIUM, verified by probe)

- **Class type-variable values** (`^X(10)(…)` dropped,
  `ElementParser.java:390-399` / `SpecParser.java:1408-1425`):
  - unused → invisible (probe `^W(10)(r=5); assertEquals(5, $w.r)`
    PASSES — nothing observable, acceptable).
  - derived property reading `$x` → **loud**
    `TypeInferenceException: unbound variable '$x'` thrown out of the
    body eval → runner ERROR. Claim holds.
  - constraint reading `$x` → the claim "constraints referencing $x fail
    loud at type-check" is **overstated**: `compileAllBodies` books a
    census wall (`probe::V$constraint$unnamed … unbound variable '$x'`)
    but nothing gates the tests — probe: `^V(10)(r=50)` under constraint
    `[$x > $this.r]` PASSES. Root cause is pre-existing: class
    constraints are **never enforced at instantiation** (probe: plain
    `[$this.r > 100]` with `r=5` also passes silently) — so the drop adds
    no NEW wrongness, but a PCT test that expects a constraint violation
    to raise would FAIL loud, and one that expects `$x`-dependent
    constraint acceptance passes vacuously.
- **Class multiplicity params `<T|m>` / `<|m>` dropped**
  (`ElementParser.java:544-571`): the claim "a use fails loud at
  type-check, never silently" is **false for the property-use case** —
  probe: `Class B<|m> { vals: String[m]; }` then `^B(vals=['a','b'])`
  PASSES and `size()` reads 2. `Multiplicity.Parameter` on a property is
  silently unconstrained. Silent wrong-ACCEPT; no corpus witness asserts
  a violation, so no wrong-pass today.
- **`^New` instance name dropped** (`SpecParser.java:1408-1414`):
  probe passes; the lookahead (`fqn-segment` + `(`) cannot misfire on
  plain `^X(a=f(1))` shapes (the paren-group scanner requires a SECOND
  group after the matching close). No consumer of the name exists. Safe.
- **Measure skip** (`ElementParser.java:343-350`, `skipMeasure`):
  absence → unit references fail loud at name resolution (covered by the
  branch's own unit test). One nit: the brace-matching loop exits at EOF
  with `depth > 0` without error — a malformed Measure swallows the rest
  of the file silently. INFO.
- **`(?:?)` wildcard type slot** (`TokenStreamCursor.java:641-651`):
  flows into dedicated wildcard machinery with loud guards
  (`Typer.java:1373`, `:1421`, `InferenceKernel.UNKNOWN_COLUMN_TYPE`).
  Safe.

### F6 — runner classification: zero-assert and advisory-only bodies cannot PASS (INFO / verified good)

`PctCorpusRunner.classify` (`PctCorpusRunner.java:170-190`): `Ran` with
`verified==0 && advisory>0` → SHAPE `sql-only`; `verified==0` → SHAPE
`no verifying assertions`. Probe: a body of `1 + 1; true;` returns
`Ran[verified=0, advisory=0]` → SHAPE. `verified` increments only after a
`checkAssert` arm that actually compared (every null-return path in
`TestBody.java:485-710` compares first). The advisory downgrade is not
gameable by string content: `isSqlText` matches only calls literally
named `sql`/`sqlRemoveFormatting` (`TestBody.java:908-911`), which the
PCT suites do not use. The scoreboard's SHAPE buckets (`assertIs/2`,
`assertInstanceOf/2`, `no verifying assertions`) confirm unknown assert
forms land SHAPE, never skipped-and-passed. Residual (inherent to PCT,
not this branch): an assert whose expected side is itself engine-computed
is self-comparing; PCT's literal-expected shape keeps this rare.

### F7 — bare multi-statement lambda typing (LOW, code-verified; probes blocked upstream)

`Typer.java:198-230`: only zero-arg lambdas enter; any non-`letFunction`
statement before the last sets `statementsOk=false` → falls through to
the loud `TypeInferenceException("a bare lambda has no type outside a
call position…")`. Shadowing is safe: `Env.with` documents and implements
shadowing (`Env.java:31-36`), and the leading lets bind sequentially so
`let a = …; let a = $a+1;` re-binds cleanly. Probes could not reach the
arm through `parseCodeBlock` (a `|stmt; stmt` literal does not parse
there, and `{|…}` via `$q->eval()` is rejected earlier by
`EvalChecker.java:70`) — the arm is exercised via ElementParser-parsed
function bodies. The typed lets are kept as statements on the
`TypedLambda`, so no silent value-drop channel; consumers that read only
the last expression would hit unbound `TypedVar`s loudly.

### F8 — native-replaces-stub overload merge (INFO, reasoned)

`FunctionCompiler.java:110-136`: user definitions whose `signatureKey`
matches a catalog native are dropped from the overload set.
`signatureKey` excludes return type/multiplicity (`Function.java:51-64`),
but pure forbids return-only overloads, so no legitimate overload can be
shadow-dropped. The dropped bodies are the PCT suites' reference
implementations of the SAME functions — the intended semantics. Residual
fidelity coupling: helper functions other tests rely on (`coalesce` etc.)
now execute as legend-lite natives, so a subtly-wrong native can skew
OTHER tests' fixtures; any such skew surfaces as value FAILs, not silent
passes. Different-signature user overloads verified to still join the set
(the `stringExtension` case per the in-code comment).

### F9 — toString-strip removal did NOT demote any assert (INFO / verified good)

The `eval()` serialization-tail strip now fires for `toCSV` only
(`TestBody.java:1054-1072`); `toString` tails forward whole and compare
string-exact against the K native's `#TDS` text. No advisory/skip
demotion: a non-Tabular receiver throws the new NIE
(`StatementExecutor.relationToString`) → runner ERROR (loud). Order is
actually STRENGTHENED here — probe: an eval-wrapped `sort()->toString()`
against a reversed expected string FAILS (the only order-pinned assert
shape in the sweep; cf. F1). The new FAIL ledger entries (variant payload
quoting, over.pure windows) are honest wrong-value FAILs of the
`tdsCell` spelling and window lowering, correctly booked.

### Cross-checks with no findings

- **Lexer**: leading-dot float is dispatched after `DOT_DOT`
  (`Lexer.java:569-576`) and `.`+digit cannot be a property name;
  negative-year dates only trigger inside `%` date scanning
  (`Lexer.java:377-384`). Out-of-range years fail loud in DuckDB (ERROR
  ledger).
- **String escapes**: octal `\477` → `'\47'+"7"` and `\0` → NUL match
  commons-text `UNESCAPE_PURE` and are unit-pinned
  (`SpecParserTest.java:136-152`); incomplete `\u` stays loud. (Nit:
  `Integer.parseInt(hex,16)` accepts a sign character — `\u+004` decodes
  where commons-text's regex-free parser also would; cosmetic.)
- **`parseSingle` trailing-token fix** (`ElementParser.java:209-217`): a
  genuine bug fix — the old bare `parser.error(...)` call built the
  exception and discarded it; trailing tokens are now actually rejected.
- **Range/slice literal desugar** (`SpecParser.java:927-947`): emits the
  same `range` call real m3 emits; a `[a:b]` misread of a plain
  collection is impossible (dispatch requires a `:`).
- **`bucket()` newline-escape** (`PctCorpusRunner.java:269-273`): display
  only; 300-char cap can merge distinct long failures into one bucket
  row — the per-test FAIL/ERROR ledgers keep them distinct.
