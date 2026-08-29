# Leg 2 — assertError source positions

**Rows:** `testAtError`, `testDayOfMonthError`, `testHourError`,
`testMinuteError`, `testNewDateError`, `testSecondError`.
**Charted error:** `assertError line/column: source position is not observable from database errors`

> ## ⚠️ SNAPSHOT WARNING — THIS LEG LANDED WHILE WE RESEARCHED IT
>
> `/Users/neema/legend/legend-lite` is a LIVE tree and it moved during this
> investigation. Between **17:33:25 and 17:46:30 ADT on 2026-08-27** the
> concurrent burn session **landed leg 2 in full**. The "before" state was read
> at 17:31; the "after" state at 17:46. Every mtime below is cited.
>
> **The commissioning brief's central premise is now stale.** "`TypedSpec` nodes
> carry NO source position" was true when written; `TypedNativeCall` has carried
> a `pos` component since 17:33:25.
>
> **This dossier is therefore a REVIEW of landed work, not a design.** Its value
> is (i) independent confirmation that the landed mechanism matches the
> reference spec, (ii) five residual cleanups, and (iii) two adjudications the
> burn session should make consciously.

See `README.md` for the shared tenet quick-reference and provenance notes.

---

## a) REFERENCE SEMANTICS

### a.1 The empirical anchor: what the six rows actually assert

All eight position assertions across the six witnesses, read from
`/Users/neemsandv/legend/legend-pure/legend-pure-core/legend-pure-m3-core/src/main/resources/platform/pure/essential/`:

| test | file:line | source text (verbatim) | asserts | char at that column |
|---|---|---|---|---|
| `testAtError` | `collection/index/at.pure:52` | `    assertError(\| $f->eval(\|[1, 2]->at(2)), '…size 2', 52, 37);` | (52, **37**) | `a` of `at` |
| `testNewDateError` | `date/creation/date.pure:71` | `    assertError(\| $f->eval(\|date(2016, 13)), 'Invalid month: 13', 71, 29);` | (71, **29**) | `d` of `date` |
| `testNewDateError` | `date/creation/date.pure:72` | `…date(2016, 12, 32)), 'Invalid day: 2016-12-32', 72, 29);` | (72, **29**) | `d` of `date` |
| `testNewDateError` | `date/creation/date.pure:73` | `…date(2016, 12, 31, 24)), 'Invalid hour: 24', 73, 29);` | (73, **29**) | `d` of `date` |
| `testDayOfMonthError` | `date/extract/dayOfMonth.pure:30` | `…\|%2017->dayOfMonth()), 'Cannot get day of month for 2017', 30, 36);` | (30, **36**) | `d` of `dayOfMonth` |
| `testHourError` | `date/extract/hour.pure:29` | `…\|%2017->hour()), 'Cannot get hour for 2017', 29, 36);` | (29, **36**) | `h` of `hour` |
| `testMinuteError` | `date/extract/minute.pure:28` | `…\|%2017->minute()), 'Cannot get minute for 2017', 28, 36);` | (28, **36**) | `m` of `minute` |
| `testSecondError` | `date/extract/second.pure:27` | `…\|%2017->second()), 'Cannot get second for 2017', 27, 36);` | (27, **36**) | `s` of `second` |

**THE GOVERNING FACT:** the asserted `line` is the file line of the erroring
call; the asserted `column` is the 1-based column of the **first character of
the erroring call's function-NAME token** — not the receiver, not the `->`, not
the `assertError`, not the `eval`. Verified by direct character indexing, not
inference.

**Every** position-asserting `assertError` in both reference repos was
enumerated (4-arg forms). There are exactly **13**:

- the 8 above (all `<<PCT.test>>`, all in Channel B's scope);
- `essential/tests/assertError.pure:42` (42, `[]`), `:47` (47, 25), `:52` (`[]`, 25) — `<<test.Test>>`, **not** PCT;
- `essential/tests/assert.pure:70` (70, 19), `:71` (71, 19) — `<<test.Test>>`, **not** PCT.

Channel B filters on the PCT stereotype (`ChannelB.isPctTest`,
`ChannelB.java:181-187`), so the last five are out of scope. No other position
assertion exists anywhere in either reference tree.

### a.2 The `assertError` contract

`legend-pure/.../platform/pure/essential/tests/assertError.pure`:

- **:18** — the primitive native:
  `assertError(f:Function<{->Any[*]}>[1], errorMessageMatcher:Function<{String[1], SourceInformation[0..1]->Any[*]}>[1]):Boolean[1]`,
  `<<PCT.function, PCT.platformOnly>>`.
- **:21-28** — the /4 overload, whose body is the spec for adjudication order
  and spellings:

```
:24  assertEquals($message, $msg, 'Execution error message mismatch.\nThe actual message was "%s"\nwhere the expected message was:"%s"', …);
:25  let _1 = $line->isNotEmpty()   && assertEquals($line->toOne(),   $si.line->toOne(),   'Execution error line mismatch. Actual: %d where expected: %d', …);
:26  let _2 = $column->isNotEmpty() && assertEquals($column->toOne(), $si.column->toOne(), 'Execution error column mismatch. Actual: %d where expected: %d', …);
```

Order is **message → line → column**, each gated on non-empty. Note `$si.line` /
`$si.column` — the *main* line/column, **not** `startLine`/`startColumn`.

- **:30-33** — the /2 overload delegates with `[], []`.

### a.3 Where the position comes from, in the engine

Four citations forming a closed chain:

1. **`SourceInformation` has six components**, and `line`/`column` are distinct
   from `startLine`/`startColumn`:
   `legend-pure-runtime-java-engine-interpreted/.../natives/essentials/meta/source/SourceInformation.java:70-82`
   populates `startLine, startColumn, line, column, endLine, endColumn`.
2. **The "main" line/column is the MIDDLE token**:
   `legend-pure-core/legend-pure-m4/.../antlr/AntlrSourceInformation.java:66-80`
   — `getPureSourceInformation(beginLine, beginColumn, mainLine, mainColumn, endLine, endColumn)`;
   the single-token overload at `:82-88` sets `line` = the column-token's line,
   `startColumn = charPositionInLine + 1`.
3. **A function expression's source info IS its name token**:
   `legend-pure-m3-core/.../m3parser/antlr/AntlrContextToM3CoreInstance.java:2054-2060`

```java
private SimpleFunctionExpression functionExpression(QualifiedNameContext funcName, …) {
    SimpleFunctionExpressionInstance result = SimpleFunctionExpressionInstance.createPersistent(
        this.repository, this.sourceInformation.getPureSourceInformation(funcName.identifier().getStart()), …);
```

   Reached for arrow calls from `:785` and for prefix calls from `allOrFunction`
   `:1535-1536`. **This is exactly why column 36 = `dayOfMonth` and column 29 = `date`.**
4. **The raise attaches the *currently-evaluating* function expression's info**:
   `legend-pure-runtime-java-engine-interpreted/.../natives/essentials/date/extract/NativeDateElementFunction.java:55-58`

```java
catch (InvalidDateElementException e) {
    throw new PureExecutionException(functionExpressionCallStack.peek().getSourceInformation(), e.getMessage(), functionExpressionCallStack);
}
```

   `DayOfMonth.java:31-34`, `Hour.java:21`, `Minute.java:21`, `Second.java:21`
   all extend it. For `date(…)`, `NewDate.java` does *not* catch —
   `DateFunctions.newPureDate` throws a plain `IllegalArgumentException`, which
   `FunctionExecutionInterpreted.java:883-895` wraps with
   `functionExpressionCallStack.peek().getSourceInformation()`. Same answer either way.

**The interpreted `AssertError` native** —
`legend-pure-runtime-java-engine-interpreted/.../natives/essentials/tests/AssertError.java:59-72`:

```java
try { executeLambda(functionToApplyTo, …);
      throw new PureAssertFailException(functionExpressionCallStack.peek().getSourceInformation(), "No error was thrown", …); }
catch (PureExecutionException e) {
    matcherParams = with( wrap(e.getInfo() == null ? "" : e.getInfo()),
                          wrap(SourceInformation.createSourceInfoCoreInstanceWithoutSourceId(repo, ps, e.getSourceInformation())) );
    executeLambda(messageMatcher, matcherParams, …); }
```

Two contract details that matter downstream: (i) the matcher receives
`(message, sourceInfo)` from the **exception**, and (ii) the source id is
**deliberately dropped** (`…WithoutSourceId`, `:68`).

### a.4 The reference relational lane FAILS all six

Every relational PCT manifest lists all six as **exclusions**, e.g.
`legend-engine-xt-relationalStore-h2-PCT/.../pct-manifests/relational-h2/EssentialFunctions_manifest.json`
(215 entries):

- `testAtError` → `"->at(...) function is supported only after direct access of 1->MANY properties"`
- `testDayOfMonthError` / `testHourError` / `testMinuteError` / `testSecondError`
  → `"Ensure the target system understands Year or Year-month semantic."`
- `testNewDateError` → `"No SQL translation exists for the PURE function 'date_Integer_1__Integer_1__Date_1_'"`

Identical exclusions in the postgres, clickhouse, duckdb, sqlserver, databricks
and spanner manifests. **The reference relational executor never even reaches
the position check. Passing these in Channel B is a genuine B-FIXES-A result.**

---

## b) OUR SEAM

### b.0 The briefing's central premise is REFUTED (as of 17:33 today)

> *"Verified fact to build on: `TypedSpec` nodes carry NO source position — that is the central gap."*

**REFUTED.**
`core/src/main/java/com/legend/compiler/spec/typed/TypedNativeCall.java`
(mtime **Aug 27 17:33:25**) now reads:

```java
public record TypedNativeCall(TypedFunction callee, List<TypedSpec> args, ExprType info,
        @com.legend.Nullable com.legend.protocol.SourceInfo pos) implements TypedSpec {   // :27-29
    public TypedNativeCall(TypedFunction callee, List<TypedSpec> args, ExprType info) {   // :35-37
        this(callee, args, info, null); }
    @Override public boolean equals(Object o) {                                            // :39-45
        return o instanceof TypedNativeCall other && callee.equals(other.callee())
            && args.equals(other.args()) && info.equals(other.info()); }
    @Override public int hashCode() { return java.util.Objects.hash(callee, args, info); } // :47-50
    @Override public TypedSpec withChildren(java.util.List<TypedSpec> kids) {              // :62-65
        return new TypedNativeCall(callee, kids, info, pos); }
```

The `/Users/neemsandv` worktree copy (HEAD `f825fb9d`) has no `pos`, confirming
this is new rather than a misreading.

**Every other element of leg 2 has also landed**, in this order:

| file | mtime | what landed |
|---|---|---|
| `compiler/spec/typed/TypedNativeCall.java` | 17:33:25 | the `pos` component + hand-written equality |
| `lowering/PureSql.java` | 17:34:25 | `raise(message, pos)` |
| `lowering/DateCtorRule.java` | 17:34:42 | 3 raise sites threaded |
| `lowering/Scalars.java` | 17:35:15 | `guarded/4` + 2 raise sites threaded |
| `sql/dialect/AnsiSqlRenderer.java` | 17:35:24 | position-carrying `ERROR` arm |
| `sql/dialect/H2.java` | 17:35:30 | position-carrying `SIGNAL` arm |
| `exec/RaisedErrors.java` | 17:36:14 | `POSITION_SEP` + `Positioned` subclass |
| `pct/…/channelb/ChannelBEssentialTest.java` | 17:37:59 | pin `pass >= 299 → 305` |
| `core/src/test/…/AssertErrorNativeTest.java` | 17:46:21 | refusal test → three position tests |
| `com/legend/AssertErrorNative.java` | 17:46:30 | **the loud refusal deleted**, adjudication added |
| `lowering/Lowerer.java` | **16:39:15** | **untouched by leg 2** |

### b.1 The wall this leg was sent to remove: GONE

At 17:31, `AssertErrorNative.java:54-62` read:

```java
// SOURCE POSITION is NOT OBSERVABLE from a database error …
if (optionalInt(ae, 2) != null || optionalInt(ae, 3) != null) {
    throw new com.legend.error.NotImplementedException(
            "assertError line/column: source position is not observable from database errors"); }
```

At 17:46 that block is replaced by `AssertErrorNative.java:94-122`:

```java
if (expectedLine != null || expectedColumn != null) {
    if (!(caught instanceof com.legend.exec.RaisedErrors.Positioned p)) {              // :104-109
        throw new java.sql.SQLException("assertError line/column: the caught error carries"
            + " no source position (a native database error, or a raise emission without provenance)"); }
    if (expectedLine != null && p.line() != expectedLine) {                            // :110-115
        throw new java.sql.SQLException("Execution error line mismatch. Actual: " + p.line()
            + " where expected: " + expectedLine); }
    if (expectedColumn != null && p.column() != expectedColumn) {                      // :116-121
        throw new java.sql.SQLException("Execution error column mismatch. Actual: "
            + p.column() + " where expected: " + expectedColumn); } }
```

Adjudication order and both spellings are byte-identical to
`assertError.pure:25-26`. `optionalInt` (`:129-144`) treats an absent arg /
empty `TypedCollection` as null and refuses non-literals loudly.

### b.2 The phase boundary — it no longer dies

**Phase C — built, file-absolute.**

- `lexer/TokenStream.java:269-272` — `startColumn(i)`: *"1-based column of the
  first character of token i"*; `:265-267` `startLine(i)`.
- `parser/TokenStreamCursor.java:1029-1034` — `spanOf(fromTok,toTok)` →
  `SourceInfo(spanSourceId(), ts.startLine(from), ts.startColumn(from), ts.endLine(to), ts.endColumn(to))`.
- **Arrow call** `parser/SpecParser.java:1441-1459` — `parseArrowPostfix`
  captures `fnStart`/`fnEnd` around `parseQualifiedName()` and emits
  `spanOf(fnStart, fnEnd)`, with the in-code comment *"a call's span covers the
  function-NAME token only, not the receiver, arrow, or argument parens
  (verified via ProbeWireShapes)"*. → column 36 for `->dayOfMonth()`.
- **Prefix call** `SpecParser.java:1207-1212` — `spanOf(fqnStart, fqnEnd)`. →
  column 29 for `date(…)`.
- **Whole-file coordinates survive element splitting**: `Compiler.java:174`
  lexes the whole file; bodies re-enter via `ElementParser.java:1638`
  `SpecParser.parseCodeBlock(tokens.slice(bodyStart, pos), dialect)`;
  `TokenStream.java:322-347` `slice()` copies absolute char offsets and **shares
  the parent's `lineStarts`** (`:345`). `ElementParser.java:2519-2520` states the
  invariant in-code: *"a slice of THIS token stream, so positions stay
  file-absolute."*
- `SourceInfo.sourceId` is `""` for code blocks (`SpecParser.java:290`, `:299`)
  — which **exactly matches** the reference's
  `createSourceInfoCoreInstanceWithoutSourceId` (`AssertError.java:68`).
  Accidental agreement, but correct.

**ChannelB adapter elimination — preserved.** `ChannelB.java:405-410` splices the
inner lambda body verbatim; the generic descent at `:445-450` uses
`n.withChildren(...)`, which for `AppliedFunction` routes to
`af.withParameters(cs)` → `AppliedFunction.java:95-98`, preserving `pos` (its
javadoc `:89-94` exists *because* hand-rebuilds dropped it once before).

**Phase D — preserved.** `compiler/NameResolver.java:1550` —
`new AppliedFunction(fn, params, candidates, af.pos(), …)`.

**Phase G — preserved on the generic path.** `compiler/spec/Typer.java:1348`
`applyGeneric` → `rawGridOrSelf(emitCall(a.chosen(), a.args(), a.out(), af.pos()))`;
`Typer.java:1674-1678` `emitCall/4` → `new TypedNativeCall(chosen, args, out, pos)`.

**Phase G½ — preserved.** `UserCallInliner.java:644`
`new TypedNativeCall(nc.callee(), args, nc.info(), nc.pos())`; the generic arms
at `:462`, `:502` use `c.withChildren(args)`.

**Phase H and every generic walker — preserved.** `TypedSpec.java:116-126`
`mapChildren` reassembles through `withChildren`, which
`TypedNativeCall.java:62-65` implements preserving `pos`.

**Lowering — consumed.** `lowering/PureSql.java:27-34`:

```java
static com.legend.sql.SqlExpr raise(SqlExpr message, @Nullable SourceInfo pos) {
    return pos == null ? SqlExpr.Call.of(SqlFn.ERROR, message)
                       : SqlExpr.Call.of(SqlFn.ERROR, message,
                             new SqlExpr.StringLit(pos.startLine() + ":" + pos.startColumn())); }
```

Note `startLine`/`startColumn` — correct, because our named-call span *is* the
name token, so our `startColumn` == the engine's `column`.

**Dialect renderers** (there is no RENDER phase):

- `sql/dialect/AnsiSqlRenderer.java:532-541` → `error(chr(31) || '30:36' || chr(30) || (msg) || chr(31))`
- `sql/dialect/H2.java:157-164` → `SIGNAL('45000', CHAR(31) || '30:36' || CHAR(30) || (msg) || CHAR(31))`
- `DuckDb.java:21` `extends AnsiSqlRenderer` and does **not** override the
  `ERROR` arm (verified: `SqlFn.ERROR` over `com/legend/sql/` hits only
  `AnsiSqlRenderer`, `H2`, `Spellings`). Channel B runs DuckDB
  (`ChannelB.java:227` `jdbc:duckdb:`), so it takes the ANSI arm.
- `SqlTyping.java:599` `case ERROR -> RAISES;` — **arity-independent**, so the
  extra argument perturbs no type rule.

**Execute — the one funnel.** `exec/Executor.java:96-101`
`catch (SQLException e) { throw RaisedErrors.unwrapped(e); }`.
`exec/RaisedErrors.java:33` `SENTINEL=''`, `:40` `POSITION_SEP=''`,
`:49-62` `unwrap(String)` (strips envelope *and* position prefix — production
text stays clean), `:67-85` `Positioned extends SQLException` with
`line()`/`column()`, `:91-119` `unwrapped()` parses `line:col` off and returns
`Positioned`.

**World 1 — adjudicated.** `AssertErrorNative.java:103-122`, reached from
`StatementExecutor.java:351-361`, keyed on
`PlatformTypes.ASSERT_ERROR.equals(aec.callee().qualifiedName())` — **exact FQN**,
not `endsWith`.

### b.3 Charter hypothesis (§3), claim by claim

| charter claim | verdict | evidence |
|---|---|---|
| "B7's provenance envelope is the channel" | **CONFIRMED** | `RaisedErrors.java:33,40`; `AnsiSqlRenderer.java:532-541` |
| "the raise emission (`SqlFn.ERROR` render arm, AnsiSqlRenderer + H2) knows its node" | **PARTIALLY CONFIRMED** — the *renderer* does **not** know the node; it receives an already-built `SqlExpr.Call` whose 2nd arg is a `StringLit`. The node is known at the **lowering rule**, which is where the threading happens (`PureSql.raise`) | `AnsiSqlRenderer.java:538-539` reads `a.get(1)` as an opaque expr; `Scalars.java:1136,1414`, `DateCtorRule.java:47,53,66` hold `n.pos()` |
| separator `chr(31)\|\|'<line>:<col>\|'\|\|msg\|\|chr(31)` | **PARTIALLY CONFIRMED** — shape right, but the separator chosen was **U+001E**, not `\|` (correct: `\|` collides with pure message text) | `RaisedErrors.java:35-40` |
| "parse it off in `RaisedErrors` (SQLState or a subclass)" | **CONFIRMED** — subclass route taken | `RaisedErrors.java:67-85, 91-119` |
| "consume in `AssertErrorNative` (delete its loud line/column refusal)" | **CONFIRMED** | `AssertErrorNative.java:94-122`; refusal gone |
| "positions must be the TEST source's own — our spans ARE their coordinates" | **CONFIRMED** | §a.1 + §b.2 chain; corroborated in §b.4 |
| "production text stays clean (the funnel strips the envelope)" | **CONFIRMED** | `RaisedErrors.java:49-62`; `:101-107` |
| "should not touch Lowerer" | **CONFIRMED** | `Lowerer.java` mtime 16:39:15, before every leg-2 edit; still exactly **3500** lines |

### b.4 Independent corroboration that our coordinates are theirs

The **pre-existing Channel-A pins**
(`pct/.../Test_LegendLite_EssentialFunctions_PCT.java:96-101`, mtime **14:41:18**
— three hours *before* leg 2, so not written to fit it):

```java
one("…at::testAtError_Function_1__Boolean_1_",   "\"Execution error column mismatch. Actual: 23 where expected: 37\""),
one("…date::tests::testDayOfMonthError_…",       "\"Execution error column mismatch. Actual: 23 where expected: 36\""),
one("…testHourError_…",                          "\"… Actual: 23 where expected: 36\""),
one("…testMinuteError_…",                        "\"… Actual: 23 where expected: 36\""),
one("…testNewDateError_…",                       "\"… Actual: 23 where expected: 29\""),
one("…testSecondError_…",                        "\"… Actual: 23 where expected: 36\""),
```

Three facts fall out, and after §a.1 they are the most informative in this dossier:

1. **Column 23 is constant across all six** — and on every one of those source
   lines, column 23 is the `e` of **`eval`** (cols 19-20 `$f`, 21-22 `->`, 23
   `eval`). Channel A reports the `$f->eval(…)` call's position because that is
   the innermost *pure* frame on the reference interpreter's stack when our Java
   adapter throws — precisely `FunctionExecutionInterpreted.java:883-895`.
2. **Only the column mismatches; the line never does.** `assertError.pure:25`
   runs before `:26`, so a line failure would mask the column one. The line
   matches because `eval` sits on the same line as the raising call. This
   confirms our line semantics independently.
3. **"source position is not observable from database errors" was always a
   half-truth.** It was true of *our* K-arm; Channel A observed a position all
   along — the structurally wrong one, because the reference adapter's boundary
   is `eval`. Channel B has no adapter indirection and can therefore report the
   true inner position. **This is why Channel B can pass rows Channel A
   structurally cannot** — a real architectural difference, not vanity.

Also corroborating: `AssertErrorNativeTest.java:80-89` asserts `(1, 23)` for
`"{|assertError(|[1,2]->at(3),'…', 1, 23)}"`. Independently indexed: `{`=1,
`|`=2, `assertError`=3-13, `(`=14, `|`=15, `[`=16, `1`=17, `,`=18, `2`=19, `]`=20,
`->`=21-22, `a`=**23**. ✓

### b.5 Coverage proof: the 8 PCT assertions vs the raise sites

**Position-carrying raise sites (5, all via `PureSql.raise`):**

| site | message family | covers |
|---|---|---|
| `Scalars.java:1136` | date-extract precision guard | `testDayOfMonthError`, `testHourError`, `testMinuteError`, `testSecondError` (4 assertions) |
| `Scalars.java:1414` (`guarded(oob, …, n.pos())`) | `at` bounds | `testAtError` (1) |
| `DateCtorRule.java:47` | `"Invalid day: 2016-12-32"` (month-aware) | `date.pure:72` |
| `DateCtorRule.java:53` | `"Invalid <comp>: <lit>"` | `date.pure:71`, `date.pure:73` |
| `DateCtorRule.java:66` | fractional-seconds range | (none today) |

`Scalars.java:2770-2775` is the position-carrying `guarded/4`; `:2763-2768` is
the legacy `guarded/3`.

**Position-FREE raise sites (15)** — an `assertError` line/col over any of these
hits the loud wall at `AssertErrorNative.java:104-109`:
`Coercions.java:65`, `:93`, `:128`; `DecimalKindRules.java:53`;
`Scalars.java:361`, `:392`, `:1459`, `:1708`, `:1717`, `:1728`, `:1939`;
`CastPolicy.java:203`; `ListEncodings.java:206`; `DateCtorRule.java:81`;
`AnsiSqlRenderer.java:304` + `:313`.

**Result: none of the 8 PCT position assertions touches any of the 15.**
Coverage is exact, not sampled.

One asymmetry worth naming: `DateCtorRule.java:47/53/66` carry a position
(literal args) while `:81` does not (runtime args) — the same
`"Invalid <comp>: …"` message family, split. Harmless for the witnesses (all args
literal), but a latent inconsistency.

---

## c) MINIMUM DESIGN — the decisions (as landed, presented for ratification)

### D1 — The round trip vs host-side correlation

**Decision taken: round-trip.** It appears **forced**, and here is the argument
rather than an assumption. The position is two facts, not one:

- *Where each raise site is* — a **compile-time** fact the compiler fully owns
  (C1.6 litmus: "could this run with no database attached?" — yes).
- *Which raise site fired* — a **runtime, data-dependent** fact.
  `date(2016,12,32)` emits up to six component guards
  (`DateCtorRule.java:35-90`); `at` emits a two-sided bounds test; a single
  statement can contain many raises. Which one fires depends on values.

The database's report channel is the SQLException — message text plus SQLState.
Any host-side correlation must recover "which one fired" from that channel:

- **Correlate by message text.** Fails: messages are not unique
  (`"Invalid hour: 24"` can come from both the `date/4` literal arm and a runtime
  arm; the same guard can appear twice in one statement after inlining). A
  near-miss match would be a **fabricated position** — a direct C2.4 violation.
- **Assume a unique raise site when the lowered plan has exactly one.** A
  **fallback with a silent-degradation cliff** (invariant 4): correct until a
  second raise appears, then silently wrong. Refuse.
- **Carry an opaque raise-ID with a compile-side id→SourceInfo table.** Strictly
  the same round trip with one more indirection plus mutable compiler state. No
  tenet advantage.
- **SQLState.** Five characters; DuckDB's `error()` gives no control over it;
  H2's `SIGNAL` takes it but `'45000'` is already used. Would be a *second* wire
  protocol. Refuse.

**Tenet reading of the chosen route:** the value crossing the database is a
**compiler-authored literal**, not a data value. Java performs no computation
*on a result value* — it recovers its own compile-time constant, selected by the
database. That is C1.2 carriage plus C1.4 error routing, not C2.1 derivation.
Clause 2c is satisfied: the *verdict* stays in World 1 (`AssertErrorNative`) and
no part of the assert body is compiled into SQL — only the raise it triggers.

**Confidence: high** that the round trip is forced. **Medium** that no cleverer
scheme exists; the one thing not ruled out is a driver-level structured-error
facility (LP-4).

### D2 — Separator: **U+001E inside the U+001F envelope**, prefix position

`RaisedErrors.java:35-40`. Better than the charter's `|`: pure's own messages
contain `|` freely; U+001E cannot appear in pure text and is only ever read
*between a sentinel pair* (`:91-107`), so a native error cannot forge it. The
prefix position lets `unwrap` strip it with one `indexOf`, and message content
can never be mistaken for a position.

### D3 — Transport: **`SQLException` subclass**, not SQLState

`RaisedErrors.Positioned` (`:67-85`). Typed, cannot collide with driver states,
and `instanceof` at `AssertErrorNative.java:104` is a total check — no parsing,
no defaulting. Verified nothing between the funnel and the consumer re-wraps:
`new java.sql.SQLException(` over `StatementExecutor.java`, `exec/`,
`Compiler.java` yields **only** `RaisedErrors.java:104` and `:118`;
`Executor.java:176` rethrows `e` unchanged.

### D4 — Equality: **hand-written, excluding `pos`**

`TypedNativeCall.java:39-50`. This answers the record-equality concern, and
mirrors an existing precedent exactly: `AppliedFunction.java:136-147` already
excludes `pos`/`propertyCall`/`grouped`/`infix`, with javadoc `:131-135`
*"Position and the dot-call spelling marker are excluded from equality — see
`ValueSpecEqualityTest`."* One owner, one idiom.

Blast radius verified **exhaustively**, not sampled:

- `SourceInfo` over `com/legend/compiler/spec/typed/` → **one hit**,
  `TypedNativeCall.java:28`. It is the only typed node carrying a position.
- `public boolean equals|public int hashCode` over the same package → **two
  hits**, both `TypedNativeCall.java`. Every other typed node keeps generated
  record equality, unchanged.
- **Every** typed-node-keyed collection is identity-based:
  `SyntheticHeads.java:1031-1032`, `DateSplit.java:68,70`, `Anchors.java:39,63`,
  `InnerDemand.java:626` are all `IdentityHashMap`; the `Map<TypedSpec,…>`
  declarations in `StoreResolver.java:1842,1946,2772,2861,3364-3378` /
  `Substitution.java:81-236` are populated from those identity maps
  (`StoreResolver.java:1946-1947`). A search for `new HashMap<TypedSpec`,
  `new LinkedHashMap<TypedSpec`, `new HashSet<TypedSpec`,
  `new LinkedHashSet<TypedSpec` (and FQN spellings) returns **zero** hits.
- `UserCallInliner.sameRefs` (`:597-606`) is **reference identity**
  (`a.get(i) != b.get(i)`), not `equals`. Adding a component cannot perturb it.
  **The briefing's `sameRefs` concern is REFUTED as a hazard.**

### D5 — Absence is loud

`AssertErrorNative.java:104-109`: a line/col expectation over an error with no
captured span throws rather than passing. Satisfies C2.4. The in-code
justification (`:100-102`) notes the reference behaves the same way
(`$si.line->toOne()` on an empty `SourceInformation` raises).

### D6 — Files and caps

Nothing approaches a limit and `Lowerer.java` was not touched:
`Lowerer.java` **3500** (untouched, mtime 16:39), `Scalars.java` **3424**,
`Typer.java` **3194**, `AnsiSqlRenderer.java` **1074**, `H2.java` **497**,
`PureSql.java` **262**, `AssertErrorNative.java` **152**,
`DateCtorRule.java` **154**, `RaisedErrors.java` **120**. Longest method touched:
`AssertErrorNative.run` at 87 lines (`:39-125`).

### D7 — What remains to be done

1. **`AssertErrorNative.java:146-148` is a dangling javadoc with no method**
   (`/** Strip the backend's error-kind prefix … */` immediately followed by
   `}`) — the doc of a method deleted in the B7 burn. Delete it.
2. **`RaisedErrors.java:114-116`** —
   `catch (NumberFormatException ignored) { /* a malformed prefix falls through to the plain strip */ }`.
   Reachable **only** if our own emission produced a malformed prefix, i.e. a
   compiler bug. Silently degrading to a position-free `SQLException` hides it
   (the downstream symptom is the generic wall at `:106-108`, which names the
   wrong cause). Per invariant 4, prefer
   `throw new IllegalStateException("raise envelope carried a malformed position prefix: " + pos)`.
3. **`RaisedErrors.unwrap(String)`** (`:49-62`) now has **zero production
   callers** — `RaisedErrors.` yields `Executor.java:101` (`unwrapped`) plus
   test-only uses (`AssertErrorNativeTest.java:152-171`). Fold it into
   `unwrapped` or document it as the seam's spec surface.
4. **`guarded/3` and `guarded/4`** (`Scalars.java:2763-2775`) are two owners of
   one shape. `guarded/3` could be `guarded(cond,msg,val,null)`.
5. **`DateCtorRule.java:81`** should take `n.pos()` for symmetry with `:47/:53/:66`.

---

## d) TRAPS

**T1 — Record equality: DISARMED, but only for `TypedNativeCall`.** If a future
leg stamps `pos` on a *second* typed node, it must repeat the hand-written
equality; nothing enforces that today. (`ValueSpecEqualityTest` exists for the
protocol layer — **no** typed-layer equivalent was found. **UNVERIFIED** whether
one should be added.)

**T2 — Position-loss sites: 5 hand-rebuilds + 4 typer paths.** All **51**
`new TypedNativeCall(` sites were enumerated. Four preserve `pos`
(`ResultEnvelopeSplice.java:161`, `:259`, `UserCallInliner.java:644`,
`Typer.java:1677`) plus `withChildren`. Of the remaining 47, most are genuine
syntheses where `null` is correct. **Nine drop a position that was available:**

- *Hand-rebuilds of an existing parsed call:* `TemporalFrame.java:859`,
  `GraphEmission.java:2909`, `Substitution.java:1049`, `SyntheticHeads.java:899`,
  `DriverPkAppend.java:81`.
- *Typer paths with `af` in scope calling `emitCall/3`:* `ConcatenateChecker.java:22`,
  `MapChecker.java:37`, `DistinctChecker.java:36`, `Typer.java:1283`.

None raises today, so no live bug — but each is a place where a future raise
silently loses provenance. Worth a one-line comment at each, or fixing the four
`emitCall/3` sites (trivial, `af.pos()` is in scope).

**T3 — `ModelException` arm loses position.** `AssertErrorNative.java:63-73`
converts a deferred-body `ModelException` into `new java.sql.SQLException(e.getMessage())`
— plain, never `Positioned`. Its named witnesses (standard-suite
`testTimeBucketSeconds/Minutes/Hours`) assert no position, so this is fine today.

**T4 — Wall spelling diverges from the spec.** When `$si` is empty, pure raises
`"Cannot cast a collection of size 0 to multiplicity [1]"` (from `->toOne()` in
`assertError.pure:25`). We raise `"assertError line/column: the caught error
carries no source position (…)"`. Ours is more informative; it is also **not**
the spec's spelling, which the file's own port-the-spec discipline
(`AssertErrorNativeTest.java:20-27`) elsewhere insists on. **Adjudicate.**

**T5 — The wall arrives as `SQLException`, so Channel B classifies it FAIL, not
ERROR.** `ChannelB.java:236-247`: `SQLException` → `Status.FAIL`;
`RuntimeException` → `Status.ERROR`. A missing position is *our platform gap*,
not a spec disagreement, and the old code used `NotImplementedException` (a
`RuntimeException`) for exactly that reason. Under the new code such a row reads
as a genuine test failure and feeds the WIRE-BUG census. **Consider
`NotImplementedException` for the no-position case**, keeping `SQLException` only
for real mismatches.

**T6 — The `assertError/2` matcher overload is unimplemented.**
`AssertErrorNative.java:48-52` refuses unless `args().get(1)` is a
`TypedCString`. The reference's primitive native (`assertError.pure:18`) takes a
*lambda* matcher. No PCT row uses it; the loud refusal is correct. Named so
nobody mistakes it for support.

**T7 — Golden/text blast radius: measured ZERO.** `EngineStyleH2`/`EngineStyleDB2`/
`EngineStyleComposite` all descend from `AnsiSqlRenderer` and inherit the new
arm, so any `error(...)` in a text golden would change. Searched:
`chr(31)|CHAR(31)` across `core/src/test`, `pct/src/test` and all resources →
**no hits** outside `docs/`; `"error\(|SIGNAL\("` across the same trees → **no
hits**. No golden exercises a raise render.

**T8 — Exception-class blast radius.** `Positioned` is a `SQLException` subclass,
so every `catch (SQLException)` still fires; the only observable change is
`e.getClass()`. No site was found that prints or switches on it. **PARTIALLY
VERIFIED** — catch sites and re-wraps were grepped, not every `getClass()` use in
the PCT extension.

**T9 — `AssertErrorNativeTest.java` was red between 17:36 and 17:46.** The old
`lineColumnRefusesLoudly` (`assertThrows(NotImplementedException…)` on
`(…, 1, 23)`) stopped throwing once the refusal was deleted — and `1:23` is the
correct position, so the call now succeeds. Replaced at 17:46:21 by
`lineColumnMatches` / `lineMismatchSpelling` / `columnMismatchSpelling`
(`:80-112`). Mentioned only so the burn session does not rediscover a transient red.

**T10 — Not touched, confirmed irrelevant.** `StructLit.Field.declared` →
`DuckDb.java:327-330` `CAST(NULL AS …)` is leg 1's mechanism
(`ChannelBEssentialTest.java:71-73`); no witness raise builds a struct.
`SqlRewriter` passes contain **no** `SqlFn.ERROR` reference and rebuild `Call`
generically. `docs/SPAN_ORIGIN_CONSOLIDATION.md` catalogs seven *parse-time* span
mechanisms; the witnesses use only mechanism #1 (`spanOf`) — none of the quirk
origins apply. `docs/STAMP_DISCIPLINE_PROGRAM.md` contains **no** occurrence of
`SourceInfo` or "source position"; its "provenance" usages (`:78-79`, `:167-189`,
`:890-893`) concern FQN-based lowering provenance, a different concept. **It rules
on nothing here.**

---

## e) CONFIDENCE + LIVE PROBES

**Effort remaining: XS.** The build is done; what is left is D7.1–D7.5 plus the
adjudications in T4/T5. **Effort had it not been done: S** — the mechanism is
~60 lines across 7 files.

**Confidence: HIGH** on (a) reference semantics — the chain from `.pure`
assertion → `SourceInformation.line/column` → `functionExpression(name-token)` →
`functionExpressionCallStack.peek()` is closed with four citations and
independently corroborated by the constant `Actual: 23` in Channel A's
pre-existing pins. **HIGH** on (b) our seam — every hop cited; all 51
construction sites, all 17 `SqlFn.ERROR` references, all 13 reference position
assertions, and every typed-node-keyed collection were enumerated. **MEDIUM** on
runtime behaviour: no JVM was run, so no claim here rests on observation.

### Live probes — the burn session must run these

- **LP-1 (the one witness to prove it on):**
  `mvn -pl pct -Dtest=ChannelBEssentialTest -Dchb.only=testDayOfMonthError test`
  (hook at `ChannelB.java:160-165`). One witness, one arrow call, one raise site,
  expected `(30, 36)`. Prove that before trusting all six.
- **LP-2 (the full leg):** `mvn -pl pct -Dtest=ChannelBEssentialTest test` — must
  reach `PASS >= 305` (`ChannelBEssentialTest.java:78-80`) with `agreePass >= 293`
  and `wireBug <= 9` unmoved (`:164-165`). The six rows should reclassify
  **AGREE-FAIL → B-FIXES-A**, so `bFixesA` rises 4 → 10 and `agreeFail` falls
  21 → 15. Those two are *printed*, not asserted (`:115-117`) — read them.
- **LP-3 (the H2 arm is UNVERIFIED end-to-end):** Channel B runs DuckDB only.
  `H2.java:157-164` emits `SIGNAL('45000', CHAR(31) || … || CHAR(30) || (…) || CHAR(31))`
  and **no test exercises an H2 raise carrying a position**. Probe that H2 2.x
  transports U+001E through `SIGNAL` intact and that the driver's
  `"; SQL statement: … [45000-232]"` suffix still lands *outside* the sentinel
  pair (`AssertErrorNativeTest.java:157` pins the suffix case for the
  position-free shape only).
- **LP-4 (the one thing that could overturn D1):** does DuckDB or H2 expose any
  structured error payload (vendor code, custom SQLSTATE, chained exception) that
  would let the position ride *outside* the message text? None found in our code;
  driver sources were not read. If one exists, D2/D3 could shed the in-message
  channel entirely.
- **LP-5:** `mvn -pl core -Dtest=AssertErrorNativeTest test` and
  `-Dtest=JdbcSurfaceCensusTest+ArchitectureTest+JavaEvalLedgerTest test` —
  `AssertErrorNative.java` is registered in `JdbcSurfaceCensusTest.java:83` and
  named in `ArchitectureTest.java:708,713,803`; `RaisedErrors.java` in
  `JdbcSurfaceCensusTest.java:107` and `JavaEvalLedgerTest.java:455`. No new class
  was added, so no register should move — confirm.

---

## OPEN QUESTIONS

1. **Should the no-position case be `NotImplementedException` (platform wall →
   Channel-B `ERROR`) rather than `SQLException` (→ `FAIL`)?** T5. It changes
   which census the row feeds.
2. **Should the empty-`$si` wall carry pure's own spelling**
   (`"Cannot cast a collection of size 0 to multiplicity [1]"`) instead of our
   explanatory one? T4 — port-the-spec discipline vs diagnosability.
3. **Should `RaisedErrors.java:114-116` throw instead of swallowing
   `NumberFormatException`?** D7.2 — the branch is only reachable via a compiler bug.
4. **Should the four `Typer.emitCall/3` sites with `af` in scope pass
   `af.pos()`?** T2 — free today, prevents a silent future loss.
5. **Should a typed-layer `ValueSpecEqualityTest` analogue exist**, pinning that
   `pos` is excluded from `TypedNativeCall` equality? T1 — nothing enforces the
   idiom for the next node that gets a span.
6. **Are the Channel-A pins at `Test_LegendLite_EssentialFunctions_PCT.java:96-101`
   still accurate after leg 2?** They record `Actual: 23` from the reference
   interpreter's own `assertError`, which leg 2 does not touch — so they *should*
   be unmoved. But the exception class our compiler throws into the reference
   adapter changed (`SQLException` → `Positioned`). LP-2/T8 settle it.
7. **`DateCtorRule.java:81`** — thread `n.pos()` for symmetry with `:47/:53/:66`? D7.5.
