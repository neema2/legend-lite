# RaiseText operand census (static)

Repo `/home/user/legend-lite`, branch `claude/compat-error-records-spike`, HEAD `5c34feb2`.
Scope: every `SqlExpr.RaiseText` construction under `core/src/main/java` (no other main source root references `RaiseText`;
outside it only `core/src/test` does). Read-only: nothing was edited, built or run.

**The working tree moved while this was read.** `git status` was clean at the start; partway through, uncommitted edits
appeared in `edge/PureMessages.java`, `edge/PureText.java`, `edge/EdgeEngineTestText.java`, `edge/EdgeCodec.java`,
`edge/RaisedWords.java`, `executionplan/ExecutionPlan.java`, `executionplan/PlanJson.java`, `planner/PlanMaker.java`,
`testplan/PlanStyleFacets.java`, plus new `executionplan/Operand.java`, `edge/H13Log.java`, and deleted
`edge/SiteWords.java`, `planner/SiteFacts.java`. **No file under `lowering/`, `sql/`, `sql/dialect/`, `resolver/`,
`compiler/` changed**, so every construction-site line number below is HEAD's and the working tree's alike. Section 6 says
which `PureMessages` text is HEAD's and which is the working tree's.

---

## 0. Counts first

| | construction expressions | (kind, site) pairs | operand positions | LITERAL-String | LITERAL-Integer | LITERAL-Float/Decimal | COMPUTED (always) | EITHER | no operand |
|---|---|---|---|---|---|---|---|---|---|
| Lowering (`lowering/`) | **26** | 28 | 37 | 18 | 4 | 1 | 6 | 8 | 4 sites |
| Dialect, at render (`sql/dialect/`) | **25** | 32 | 23 | 12 | 0 | 0 | 2 | 9 | 3 sites |
| Total | **51** | 60 | 60 | 30 | 4 | 1 | 8 | 17 | 7 sites |

- `resolver/`, `compiler/`, `planner/`, `setup/`, `testplan/`, `edge/`, `executionplan/` construct **no** `RaiseText` of
  their own. The resolver and the typer build a typed *call* (`typeRaise`) that the lowering turns into one (L5); the
  edge only reads them; `executionplan/MirJson` only reads one back (a copy).
- 27 `RaiseKind`s. **12** have a position legend-pure's sentence prints that can be COMPUTED or EITHER
  (11 without `USER_FAIL`, which is never a site): `USER_FAIL`, `DIVIDE_BY_ZERO`, `DATE_PART_ABSENT`,
  `INDEX_OUT_OF_BOUNDS`, `SQRT_OF_NEGATIVE`, `SIZE_NOT_MULTIPLICITY`, `SLICE_BOUNDS_INVERTED`,
  `DATE_COMPONENT_INVALID`, `TEXT_NOT_INTEGER`, `TEXT_NOT_FLOAT`, `TEXT_NOT_DATE`, `FRACTION_NOT_KEPT`.
  **15** have none (no operand, or planner-known text only).
- Printed positions that are **always** COMPUTED, whatever the query wrote: `INDEX_OUT_OF_BOUNDS` p1 (the size),
  `SIZE_NOT_MULTIPLICITY` p1 (the size), `SQRT_OF_NEGATIVE` p0 (a `Cast ... DOUBLE` around the argument, literal or
  not), `DATE_COMPONENT_INVALID` p1 at `DateCtorRule:83` only, `FRACTION_NOT_KEPT` p1 at `DateCtorRule:165` only,
  `TEXT_NOT_FLOAT` p0 on SQLite only (a derived column).
- No site builds a `BoolLit`, `DateLit` or `TimestampLit` operand directly. `DateLit`/`TimestampLit` can only arrive
  through one EITHER position (`DATE_PART_ABSENT` p1). **No position anywhere is Boolean-typed**, so LITERAL-Boolean
  does not occur today.
- None of `NullLit`, `TimeLit`, `ArrayLit`, `StructLit`, `DatePartLit`, `FormatLit`, `TypeNameLit`, `JsonKindLit` is
  built as an operand at any site (section 5).
- Only one site is `asData` (`CheckedEnvelope:87`). 15 sites use `RaiseText.placed` (4 lowering, 11 dialect). 7 more set
  `where` through the 4-argument constructor with separate operands.

---

## 1. How to read the classification

`AnsiSqlRenderer.site` (`sql/dialect/AnsiSqlRenderer.java:1241-1264`) turns a `RaiseText` into a
`RaiseSite(n, kind, where, operands)` with **the same operand list, untouched** (line 1253:
`new RaiseSite(raiseSites.size() + 1, r.kind(), r.where(), r.operands())`), except:
`asData != null` writes `StringLit(asData)` and makes no site (1242-1245); `USER_FAIL` writes its operand 0 and makes no
site (1246-1248; `RaiseSite`'s constructor refuses `USER_FAIL`, `RaiseSite.java:30`). Two raises of one statement equal
in kind, where and operands share one site (1254-1260).

What a lowered **value** argument looks like (`lowering/Lowerer.java:3677-3704`, `lowering/MatchFold.java:84-117`), i.e.
what an EITHER position holds when the query wrote a literal there:

| Pure literal | lowered | note |
|---|---|---|
| Integer | `IntLit` | beyond a long: `DecimalLit` (`Lowerer:3677-3679`) |
| Float | `FloatLit` | **`DecimalLit` when `TypedCFloat.exact() != null`** (`Lowerer:3687-3691`): a Float literal is not always a `FloatLit` |
| Decimal | `DecimalLit` | |
| String | `StringLit` | |
| Boolean | `BoolLit` | |
| StrictDate `%2017-01-01` | `DateLit` | |
| DateTime with hour/minute/second/subsecond | `TimestampLit` | hour and minute forms are PADDED (`+ ":00:00"`, `+ ":00"`, `MatchFold:108-111`); spelled in `dbTimeZone` when that is not GMT/UTC (`MatchFold:85-90`) |
| partial date `%2017`, `%2017-04` | **`Cast(StringLit, TEMPORAL_TEXT, USER)`** | a literal wrapped in a marker cast (`MatchFold:97-102`): COMPUTED under a strict rule |
| `%latest` | `Cast(StringLit("9999-12-31T00:00:00.0000"), TEMPORAL_TEXT, DECLARED)` | `Lowerer:3701-3704` |
| StrictTime | `TimeLit` | `Lowerer:3698` |
| `[]` | `NullLit` (or `Cast(NullLit, JSON)`) | `Lowerer:3711-3713` |
| `-5`, `-5.0` | folded to a negated literal | `Scalars:412-416` |
| `[x]` | its element | the C1 collapse, `Lowerer:3802-3804` |

A rule receives its arguments already lowered, with no wrapping added at dispatch (`Scalars.lower`, `Scalars.java:2744-2781`;
the callers `Lowerer:2288, 2312, 3532, 4340, 4347, 4495`). So **EITHER = that lowered argument**: a literal of the table
above when the query wrote one, anything else (a column, a `PlanParam`, a call, a cast, even another raise) otherwise.

The prototype in the working tree, `executionplan/Operand.java` (untracked), `Operand.of`: `StringLit -> Text`,
`IntLit -> Int`, `FloatLit -> Flt`, `DecimalLit -> Dec`, `BoolLit -> Bool`, `DateLit -> Day`, `TimestampLit -> Instant`,
`default -> COMPUTED` ("a cast of a literal is computed"). `planner/PlanMaker.java:1050-1056` applies it to every
`RaiseSite` operand.

---

## 2. Lowering-side construction sites (26)

Helpers that only **carry** a `RaiseText` built by their caller (they build none):
`PureSql.raise(RaiseText, SourceInfo)` (`lowering/PureSql.java:27-34`; a non-null position rides the `ERROR` call as a
second argument `StringLit("line:col")`, it is **not** an operand); `Scalars.guarded(cond, RaiseText, value)`,
`guarded(..., Styled)`, `guarded(..., SourceInfo)` (`lowering/Scalars.java:3023-3038`);
`LoweringForms.raise(RaiseKind, Supplier<SqlExpr> checked, SqlExpr plain)` (`lowering/LoweringForms.java:46`; the
product's returns `checked.get()`, `ProductLoweringForms.java:26-28`; the edge's returns the plain operation, no raise at
all, for a kind in `PureText.ENGINE_UNCHECKED` under the pure/v1 style, `edge/EdgeLoweringForms.java:39-41`,
`edge/PureText.java:195-200`); `CastPolicy.impossible` (`CastPolicy.java:182-184`). Every caller of these is one of the
sites below. There is no helper named `refuse(...)` or `guard(...)` that builds one: every occurrence of the identifier
`RaiseText` under `core/src/main/java` was read.

"Style-gated" below = the raise goes through `LoweringForms.raise`, so the pure/v1 style drops it.

| # | file:line | kind | form | operands (position: Java expression -> class) |
|---|---|---|---|---|
| L1 | `lowering/CastPolicy.java:235` | CAST_IMPOSSIBLE | where set (4-arg ctor), not `placed`; style-gated | p0 `new SqlExpr.StringLit(src.typeName())` -> LITERAL-String; p1 `new SqlExpr.StringLit(target.typeName())` -> LITERAL-String |
| L2 | `lowering/CheckedEnvelope.java:87` | CONSTRAINT_UNEVALUABLE | **asData** | p0 `new SqlExpr.StringLit(cc.id())` -> LITERAL-String |
| L3 | `lowering/Coercions.java:65` | SIZE_NOT_MULTIPLICITY | placed `"[1]"` | p0 `StringLit("[1]")` -> LITERAL-String |
| L4 | `lowering/Coercions.java:103` | SIZE_NOT_MULTIPLICITY | placed `"[1]"`; style-gated | p0 `StringLit("[1]")` -> LITERAL-String |
| L5 | `lowering/Coercions.java:138` | **variable**: INSTANCE_NOT_OF, CAST_IMPOSSIBLE, MATCH_FAILURE | where set (4-arg ctor) | p0 `from` -> LITERAL-String; p1 `to` -> LITERAL-String (both checked `instanceof StringLit`, else `IllegalStateException`) |
| L6 | `lowering/Coercions.java:162` | SIZE_NOT_MULTIPLICITY | placed `"[1..*]"` | p0 `StringLit("[1..*]")` -> LITERAL-String |
| L7 | `lowering/DateCtorRule.java:48` | DATE_COMPONENT_INVALID | plain | p0 `new SqlExpr.StringLit("day")` -> LITERAL-String; p1 `yl`, p2 `ml`, p3 `lit` -> LITERAL-Integer x3 (each pattern-matched `instanceof SqlExpr.IntLit`) |
| L8 | `lowering/DateCtorRule.java:52` | DATE_COMPONENT_INVALID | plain | p0 `new SqlExpr.StringLit(comps[i])` -> LITERAL-String; p1 `lit` -> LITERAL-Integer |
| L9 | `lowering/DateCtorRule.java:64` | DATE_COMPONENT_INVALID | plain | p0 `new SqlExpr.StringLit(comps[i])` -> LITERAL-String; p1 `args.get(i)` -> LITERAL-Float or LITERAL-Decimal (the branch is `instanceof FloatLit \|\| instanceof DecimalLit`) |
| L10 | `lowering/DateCtorRule.java:83` | DATE_COMPONENT_INVALID | plain | p0 `new SqlExpr.StringLit(comps[i])` -> LITERAL-String; p1 `args.get(i)` -> **COMPUTED** (the else branch: never `IntLit`/`FloatLit`/`DecimalLit`) |
| L11 | `lowering/DateCtorRule.java:165` | FRACTION_NOT_KEPT | where set (4-arg ctor) | p0 `new SqlExpr.StringLit("the second ")` -> LITERAL-String; p1 `second` -> **COMPUTED**; p2 `new SqlExpr.StringLit(" has more than " + ...)` -> LITERAL-String |
| L12 | `lowering/DecimalKindRules.java:61` | DIVIDE_BY_ZERO | plain; style-gated | none (`List.of()`) |
| L13 | `lowering/ListEncodings.java:267` | SLICE_BOUNDS_INVERTED | plain; style-gated | p0 `lo`, p1 `hi` -> **EITHER**, wrapped by `clamp0` |
| L14 | `lowering/ListRules.java:28` | RANGE_STEP_ZERO | plain; style-gated | none |
| L15 | `lowering/MixedEncoding.java:1041` | VALUE_NOT_HELD | plain (where null, **not** `placed`) | p0 `new SqlExpr.StringLit("an Integer beyond 2^53 ...")` -> LITERAL-String |
| L16 | `lowering/Scalars.java:270` | USER_FAIL | plain | p0 `args.isEmpty() ? new SqlExpr.StringLit("fail") : args.get(0)` -> LITERAL-String `"fail"` for `fail()`, else **EITHER** (the user's message) |
| L17 | `lowering/Scalars.java:437` | SHIFT_OUT_OF_RANGE | plain; style-gated | none |
| L18 | `lowering/Scalars.java:477` | DIVIDE_BY_ZERO | plain; style-gated | p0 `args.get(0)` -> **EITHER** (rem's dividend, after `decimalJoin`) |
| L19 | `lowering/Scalars.java:1249` | DATE_PART_ABSENT | plain | p0 `new SqlExpr.StringLit(label)` -> LITERAL-String; p1 `args.get(0)` -> **EITHER** (the date) |
| L20 | `lowering/Scalars.java:1588` | INDEX_OUT_OF_BOUNDS | plain | p0 `args.get(1)` -> **EITHER** (the index); p1 `size` -> **COMPUTED** |
| L21 | `lowering/Scalars.java:1629` | INSERT_OUT_OF_BOUNDS | plain | none |
| L22 | `lowering/Scalars.java:1927` | SQRT_OF_NEGATIVE | plain; style-gated | p0 `x` -> **COMPUTED** (always a `Cast` to DOUBLE around the lowered argument) |
| L23 | `lowering/Scalars.java:2146` | LIST_LENGTHS_DIFFER | placed | p0 `StringLit(String.valueOf(e.getKey()))` -> LITERAL-String |
| L24 | `lowering/Scalars.java:2533` | DIVIDE_BY_ZERO | plain; style-gated | p0 `args.get(0)` -> **EITHER** (quotient's dividend) |
| L25 | `lowering/TaggedLists.java:201` | INDEX_OUT_OF_BOUNDS | plain | p0 `index` -> **EITHER**; p1 `size` -> **COMPUTED** |
| L26 | `lowering/ToOneGuard.java:69` | SIZE_NOT_MULTIPLICITY | where set (4-arg ctor) `"[1]"` | p0 `new SqlExpr.StringLit("[1]")` -> LITERAL-String; p1 `n` -> **COMPUTED** |

### The quoted code and the notes, site by site

**L1** `lowering/CastPolicy.java:235-237` (in `crossKindRaise(Type src, Type target)`, line 211):
```java
return SqlExpr.Call.of(com.legend.sql.SqlFn.ERROR, new SqlExpr.RaiseText(
        com.legend.sql.RaiseKind.CAST_IMPOSSIBLE, java.util.List.of(new SqlExpr.StringLit(src.typeName()),
                new SqlExpr.StringLit(target.typeName())), null, src.typeName() + " to " + target.typeName()));
```
Callers of `crossKindRaise`: `CastPolicy.java:68` (a user's cast, `written`) and `:191` (`castByPolicy`), both through
`impossible(raise, value, forms)` -> `forms.raise(com.legend.sql.RaiseKind.CAST_IMPOSSIBLE, () -> raise, value)` (`:183`).
The raise is unconditional (no guard CASE). Both operands are Pure primitive type names.

**L2** `lowering/CheckedEnvelope.java:87-90`:
```java
defect(cc, new SqlExpr.RaiseText(com.legend.sql.RaiseKind.CONSTRAINT_UNEVALUABLE,
        List.of(new SqlExpr.StringLit(cc.id())),
        com.legend.error.ErrorKind.CONSTRAINT_UNEVALUABLE.sentence() + " (constraint "
                + cc.id() + ")"))),
```
The only `asData` site: the `RaiseText` is the `message` value of a JSON defect object, never an argument of `ERROR`.
`site()` writes `StringLit(asData)`; no `RaiseSite`, so no `Operand` is ever made for it.

**L3** `lowering/Coercions.java:65-66` (toOne over a statically empty operand; unconditional):
```java
return SqlExpr.Call.of(SqlFn.ERROR, SqlExpr.RaiseText.placed(
        com.legend.sql.RaiseKind.SIZE_NOT_MULTIPLICITY, "[1]"));
```

**L4** `lowering/Coercions.java:100-104` (toOne over a value-lane `[0..1]`):
```java
return Scalars.guarded(
        new SqlExpr.Call(SqlFn.IS_NULL,
                List.of(args.get(0))),
        SqlExpr.RaiseText.placed(com.legend.sql.RaiseKind.SIZE_NOT_MULTIPLICITY, "[1]"),
        args.get(0), styled);
```

**L5** `lowering/Coercions.java:131-140` (the rule of the compiler's own native `meta::legend::lite::typeRaise(kind, from, to)`):
```java
if (!(args.get(0) instanceof com.legend.sql.SqlExpr.StringLit kind)
        || !(args.get(1) instanceof com.legend.sql.SqlExpr.StringLit from)
        || !(args.get(2) instanceof com.legend.sql.SqlExpr.StringLit to)) {
    throw new IllegalStateException("typeRaise's arguments are the resolver's literals: " + args);
}
com.legend.sql.RaiseKind k = com.legend.sql.RaiseKind.valueOf(kind.value());
// its where: the two types, or a match's value's type alone
com.legend.sql.SqlExpr raised = PureSql.raise(new com.legend.sql.SqlExpr.RaiseText(k,
        List.of(from, to), null, k == com.legend.sql.RaiseKind.MATCH_FAILURE ? from.value()
                : from.value() + " to " + to.value()), n.pos());
```
The kind is a variable (`RaiseKind.valueOf` of a string). The calls are built in Java only, each argument a
`TypedCString` (lowered to `StringLit`, `Lowerer:3680`); a user's call is refused at name resolution
(`builtin/Pure.java:630-635`, `COMPILER_OWN`). The four origins:

| origin | kind | from (p0) | to (p1) |
|---|---|---|---|
| `resolver/ChainDispatch.java:206` `typeRaise(com.legend.sql.RaiseKind.INSTANCE_NOT_OF, from.fqn(), to.fqn(), boolOne)` | INSTANCE_NOT_OF | a class path | a class path |
| `resolver/Substitution.java:2318-2319` `ChainDispatch.typeRaise(com.legend.sql.RaiseKind.CAST_IMPOSSIBLE, target.classFqn(), fqn, ...)` | CAST_IMPOSSIBLE | a class path | a class path |
| `resolver/Substitution.java:2360-2362` `ChainDispatch.typeRaise(com.legend.sql.RaiseKind.MATCH_FAILURE, "row", mr.arms().stream().map(a -> a.typeFqn()).collect(joining(", ")), mr.info())` | MATCH_FAILURE | `"row"` | the arms' paths, `", "`-joined |
| `compiler/spec/MatchChecker.java:141-145` `new TypedCString(RaiseKind.MATCH_FAILURE.name(), str), new TypedCString(input.info().type().typeName() + input.info().multiplicity().text(), str), new TypedCString("", str)` | MATCH_FAILURE | e.g. `Integer[1]` | `""` |

`ChainDispatch.typeRaise` itself is `resolver/ChainDispatch.java:221-230`. So the set of kinds that can flow is exactly
those three. Operands are always LITERAL-String.

**L6** `lowering/Coercions.java:162-163` (toOneMany over a statically empty operand; unconditional):
```java
return SqlExpr.Call.of(SqlFn.ERROR, SqlExpr.RaiseText.placed(
        com.legend.sql.RaiseKind.SIZE_NOT_MULTIPLICITY, "[1..*]"));
```

**L7** `lowering/DateCtorRule.java:48-49` (literal day past its literal month's length; unconditional):
```java
return PureSql.raise(new SqlExpr.RaiseText(com.legend.sql.RaiseKind.DATE_COMPONENT_INVALID,
        List.of(new SqlExpr.StringLit("day"), yl, ml, lit)), n.pos());
```
`yl`, `ml`, `lit` are bound by `instanceof SqlExpr.IntLit` at lines 37, 43, 44.

**L8** `lowering/DateCtorRule.java:52-53` (a literal Integer component out of range; unconditional):
```java
return PureSql.raise(new SqlExpr.RaiseText(com.legend.sql.RaiseKind.DATE_COMPONENT_INVALID,
        List.of(new SqlExpr.StringLit(comps[i]), lit)), n.pos());
```
`comps = {null, "month", "day", "hour", "minute", "second"}` (line 33).

**L9** `lowering/DateCtorRule.java:64-65` (a literal fractional component out of `[0, 60)`; unconditional):
```java
return PureSql.raise(new SqlExpr.RaiseText(com.legend.sql.RaiseKind.DATE_COMPONENT_INVALID,
        List.of(new SqlExpr.StringLit(comps[i]), args.get(i))), n.pos());
```
Reached only inside `else if (args.get(i) instanceof SqlExpr.FloatLit || args.get(i) instanceof SqlExpr.DecimalLit)`
(lines 55-56): p1 is a `FloatLit` or a `DecimalLit`.

**L10** `lowering/DateCtorRule.java:78-85` (a computed component, guarded at run time):
```java
guarded.set(i, Scalars.guarded(
        SqlExpr.Call.of(SqlFn.OR,
                SqlExpr.Call.of(SqlFn.LESS, args.get(i),
                        new SqlExpr.IntLit(ranges[i][0])),
                tooHigh),
        new SqlExpr.RaiseText(com.legend.sql.RaiseKind.DATE_COMPONENT_INVALID,
                List.of(new SqlExpr.StringLit(comps[i]), args.get(i))),
        args.get(i)));
```
The final `else` of the chain: `args.get(i)` is none of `IntLit`, `FloatLit`, `DecimalLit`, so p1 is always COMPUTED.

**L11** `lowering/DateCtorRule.java:162-170` (`keptSecond`, a computed fractional second):
```java
return Scalars.guarded(SqlExpr.Call.of(SqlFn.NOT_EQUAL, exact,
                SqlExpr.Call.of(SqlFn.ROUND, exact, new SqlExpr.IntLit(CARRIED_DIGITS))),
        // FRACTION_NOT_KEPT: lite's own sentence, its parts the operands, written by the edge alone (D1.1)
        new SqlExpr.RaiseText(com.legend.sql.RaiseKind.FRACTION_NOT_KEPT, List.of(
                new SqlExpr.StringLit("the second "), second, new SqlExpr.StringLit(" has more than "
                        + CARRIED_DIGITS + " fraction digits, which a date()'s fractional second keeps: refused,"
                        + " never cut (S181)")), null, "a date()'s second keeps " + CARRIED_DIGITS
                + " fraction digits"),
        second, n.pos());
```
Reached only when `known == null` (line 150-152: `second` is neither `FloatLit` nor `DecimalLit`), so p1 is always
COMPUTED. Note: `second` is `args.get(5)` **after** the loop above replaced it (line 88 `args = guarded;`), so p1 is the
`CASE WHEN ... THEN ERROR(RaiseText DATE_COMPONENT_INVALID) ELSE s END` of L10: **an operand that contains another
raise**.

**L12** `lowering/DecimalKindRules.java:57-62` (Integer / Integer):
```java
return Scalars.guarded(
        SqlExpr.Call.of(SqlFn.EQUAL, args.get(1),
                new SqlExpr.IntLit(0)),
        // DIVIDE_BY_ZERO, no operand (the interpreter's 'Division by zero', the edge's: D1.1)
        new SqlExpr.RaiseText(com.legend.sql.RaiseKind.DIVIDE_BY_ZERO, List.of()),
        new SqlExpr.Call(SqlFn.DIVIDE, args), styled);
```

**L13** `lowering/ListEncodings.java:258-269` (`slice(src, start, stop, forms)`; the one caller `Lowerer.java:4175-4179`
passes `scalar(s.start(), columns)` and `scalar(s.stop(), columns)`):
```java
SqlExpr lo = clamp0(start);
SqlExpr hi = clamp0(stop);
...
return forms.raise(com.legend.sql.RaiseKind.SLICE_BOUNDS_INVERTED, () -> new SqlExpr.Case(List.of(
        new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.GREATER, lo, hi),
        SqlExpr.Call.of(SqlFn.ERROR, new SqlExpr.RaiseText(
                com.legend.sql.RaiseKind.SLICE_BOUNDS_INVERTED, List.of(lo, hi))))),
        sliced, SqlExpr.Reason.CARRIER), sliced);
```
`clamp0` (`ListEncodings.java:349-352`):
```java
return e instanceof SqlExpr.IntLit i
        ? new SqlExpr.IntLit(Math.max(0, i.value()))
        : SqlExpr.Call.of(SqlFn.GREATEST, e, new SqlExpr.IntLit(0));
```
EITHER: a literal bound is a fresh `IntLit` **clamped at 0** (so `slice(3, -1)` holds `IntLit(0)`, not `-1`); any other
bound is `GREATEST(<lowered arg>, 0)`, a `Call` -> COMPUTED.

**L14** `lowering/ListRules.java:27-29`:
```java
: Scalars.guarded(SqlExpr.Call.of(SqlFn.EQUAL, args.get(2), new SqlExpr.IntLit(0)),
        new SqlExpr.RaiseText(com.legend.sql.RaiseKind.RANGE_STEP_ZERO, java.util.List.of()),
        new SqlExpr.Call(SqlFn.RANGE_FN, args), styled));
```

**L15** `lowering/MixedEncoding.java:1041-1044` (`guardedRanks`; callers `MixedEncoding.java:41` and `:235`):
```java
return Scalars.guarded(beyond, new SqlExpr.RaiseText(com.legend.sql.RaiseKind.VALUE_NOT_HELD,
        List.of(new SqlExpr.StringLit("an Integer beyond 2^53 in a mixed list's order: its DOUBLE rank does"
                + " not hold it exactly (refused until the kinds-kept carrier ranks numbers exactly)"))),
        ranks);
```
The one fixed-text raise that is **not** `placed`: its `where` is null, so the product's sentence of this site carries no
where, and its text exists only as operand 0.

**L16** `lowering/Scalars.java:270-271` (`fail()` / `fail(message)`):
```java
SqlExpr raised = PureSql.raise(new SqlExpr.RaiseText(com.legend.sql.RaiseKind.USER_FAIL,
        List.of(args.isEmpty() ? new SqlExpr.StringLit("fail") : args.get(0))), n.pos());
```
p0 is `StringLit("fail")` for the no-argument overload; otherwise the lowered message: a `StringLit` when the message is
a string literal (also an in-body assert's default, which lowers to `new StringLit(styled.forms().assertFailed())`,
`Coercions.java:146-149`, or `TypedCString("Assert failed")`, `compiler/spec/LambdaBodies.java:142`), COMPUTED when it is
a `format(...)`, a concatenation, a column. Never a site.

**L17** `lowering/Scalars.java:435-437`:
```java
return styled.forms().raise(com.legend.sql.RaiseKind.SHIFT_OUT_OF_RANGE, () -> {
    SqlExpr boundError = SqlExpr.Call.of(SqlFn.ERROR,
            new SqlExpr.RaiseText(com.legend.sql.RaiseKind.SHIFT_OUT_OF_RANGE, List.of()));
```
(unconditional for a literal shift outside 0..62, line 438-439; a CASE's else for a computed shift, 443-447.)

**L18** `lowering/Scalars.java:465-478` (`rem`):
```java
RULES.put(f, (StyledRule) (n, rawArgs, styled) -> {
    var args = decimalJoin(rawArgs);
    ...
    return guarded(
            SqlExpr.Call.of(SqlFn.EQUAL, args.get(1), new SqlExpr.IntLit(0)),
            new SqlExpr.RaiseText(com.legend.sql.RaiseKind.DIVIDE_BY_ZERO, List.of(args.get(0))),
            new SqlExpr.Call(SqlFn.REM, args), styled);
```
EITHER, no wrapper. `rem(dividend:Number[1], ...)`, so a literal dividend is `IntLit`, `FloatLit` or `DecimalLit`.
`decimalJoin` (`Scalars.java:2987-3018`) rewrites the arguments when any is of decimal kind: `FloatLit -> DecimalLit` and
`Cast(FloatLit, DOUBLE) -> DecimalLit` (`undoubled`); a literal stays a literal, its kind can change.

**L19** `lowering/Scalars.java:1244-1250` (year / monthNumber / dayOfMonth / hour / minute / second over a date whose
precision is statically too short):
```java
PureDateLiteral.Precision prec =
        datePrecisionOrUnknown(n.args().get(0));
if (prec != null && !prec.atLeast(needed)) {
    // the part's label and the date, the edge's sentence's operands (D1.1)
    return PureSql.raise(new SqlExpr.RaiseText(com.legend.sql.RaiseKind.DATE_PART_ABSENT,
            List.of(new SqlExpr.StringLit(label), args.get(0))), n.pos());
}
```
`label` is `"day of month"` or the part's lower-cased name (lines 1239-1242). Unconditional raise. p1 is EITHER, and
`datePrecision` (`Scalars.java:3849-3882`) says when the site is reached:
- a date **literal** of short precision -> p1 is `DateLit` (a StrictDate asked its hour/minute/second: LITERAL-Date),
  `TimestampLit` (an hour- or minute-precision literal asked a finer part: LITERAL-Timestamp, **padded** text), or
  `Cast(StringLit, TEMPORAL_TEXT, USER)` (a year or year-month literal: a literal inside a marker cast -> COMPUTED under
  a strict rule; this is the `'Cannot get day of month for 2017'` shape);
- a `date(...)` constructor call of short arity -> COMPUTED;
- **any expression typed StrictDate** asked its hour/minute/second (a column, a parameter) -> COMPUTED.

**L20** `lowering/Scalars.java:1580-1592` (`at`):
```java
SqlExpr op = CollectionLanes.compactIfValueLane(
        n.args().get(0), args.get(0));
SqlExpr size = SqlExpr.Call.of(SqlFn.LIST_LENGTH, op);
...
return guarded(oob,
        new SqlExpr.RaiseText(com.legend.sql.RaiseKind.INDEX_OUT_OF_BOUNDS,
                List.of(args.get(1), size)),
        new SqlExpr.Call(SqlFn.LIST_GET,
                List.of(op, plusOne(args.get(1)))),
        n.pos());
```
p0 EITHER, no wrapper (`IntLit` for a literal index). p1 is always a `Call(LIST_LENGTH, ...)`, COMPUTED even over a
literal list.

**L21** `lowering/Scalars.java:1626-1631` (`add(set, index, value)`):
```java
return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
        SqlExpr.Call.of(SqlFn.GREATER, idx,
                SqlExpr.Call.of(SqlFn.LIST_LENGTH, l)),
        SqlExpr.Call.of(SqlFn.ERROR, new SqlExpr.RaiseText(
                com.legend.sql.RaiseKind.INSERT_OUT_OF_BOUNDS, List.of())))),
        inserted, SqlExpr.Reason.RAISE);
```

**L22** `lowering/Scalars.java:1923-1928` (`sqrt`):
```java
SqlExpr x = new SqlExpr.Cast(args.get(0), SqlType.Scalar.DOUBLE, SqlExpr.Reason.OPERATOR);
return guarded(
        SqlExpr.Call.of(SqlFn.LESS, x, new SqlExpr.IntLit(0)),
        // the value, the edge's sentence's operand, printed there as Pure prints a Float (D1.1)
        new SqlExpr.RaiseText(com.legend.sql.RaiseKind.SQRT_OF_NEGATIVE, List.of(x)),
        SqlExpr.Call.of(SqlFn.SQRT, args.get(0)), styled);
```
**Always a `Cast` to DOUBLE (reason OPERATOR) around the lowered argument**: COMPUTED under a strict rule even for
`sqrt(-4.0)`. `SqlExpr.Cast`'s constructor folds nothing (`sql/SqlExpr.java:1762-1772`). A rule that looks through this
one cast would find `IntLit`/`FloatLit`/`DecimalLit` whenever the query wrote a literal.

**L23** `lowering/Scalars.java:2142-2148` (corr / covarPopulation / covarSample over two lists):
```java
return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
        SqlExpr.Call.of(SqlFn.NOT_EQUAL,
                SqlExpr.Call.of(SqlFn.LIST_LENGTH, xs),
                SqlExpr.Call.of(SqlFn.LIST_LENGTH, ys)),
        SqlExpr.Call.of(SqlFn.ERROR, SqlExpr.RaiseText.placed(
                com.legend.sql.RaiseKind.LIST_LENGTHS_DIFFER, String.valueOf(e.getKey()))))),
        new SqlExpr.ScalarSubquery(outer), SqlExpr.Reason.RAISE);
```
`e.getKey()` is a `List<FunctionId>` (the map's key at line 2094, e.g. `Pure.AT_MATH_CORR`), so the text is that Java
list's `toString()` (the overloads' qualified ids in brackets), not a bare native name.

**L24** `lowering/Scalars.java:2531-2534` (`meta::legend::lite::quotient(Integer[1], Integer[1])`):
```java
RULES.put(f, (StyledRule) (n, args, styled) -> guarded(
        SqlExpr.Call.of(SqlFn.EQUAL, args.get(1), new SqlExpr.IntLit(0)),
        new SqlExpr.RaiseText(com.legend.sql.RaiseKind.DIVIDE_BY_ZERO, List.of(args.get(0))),
        SqlExpr.Call.of(SqlFn.INT_DIVIDE, args.get(0), args.get(1)), styled));
```
EITHER, no wrapper (`IntLit` for a literal dividend).

**L25** `lowering/TaggedLists.java:193-203` (`at` over a mixed list; the one caller `Scalars.java:1576`
`TaggedLists.at(mixedAt, args.get(1), n.pos())`):
```java
SqlExpr size = size(tl);
...
return Scalars.guarded(oob,
        new SqlExpr.RaiseText(com.legend.sql.RaiseKind.INDEX_OUT_OF_BOUNDS, List.of(index, size)),
        tl.element(position), pos);
```
p0 EITHER. p1 `size(tl)` (`TaggedLists.java:41-52`) is a `ScalarSubquery` or `COALESCE(LIST_LENGTH(...), 0)`: COMPUTED.

**L26** `lowering/ToOneGuard.java:66-70` (a relation's `toOne()` at a statement's root; the one caller
`Lowerer.java:847`):
```java
SqlExpr n = SqlExpr.Call.of(SqlFn.COALESCE, SqlExpr.Column.of(ra, rows), new SqlExpr.IntLit(0));
...
SqlExpr raise = SqlExpr.Call.of(SqlFn.ERROR, new SqlExpr.RaiseText(
        com.legend.sql.RaiseKind.SIZE_NOT_MULTIPLICITY, List.of(new SqlExpr.StringLit("[1]"), n), null, "[1]"));
```

---

## 3. Render-time construction sites in `sql/dialect/` (25)

These are built by a dialect while it writes a statement, from a node the lowering emitted (named under "from").
`MySql` extends `MariaDb` and inherits its six. Seven dialects reach D3 and eight reach D4 through the base helpers (the
callers are listed under each); `DuckDb`, `MariaDb`, `SQLite` and `MongoDb` build their own parse raises (D9, D12, D15,
D17, D21, D24).

| # | file:line | kind | form | operands | from |
|---|---|---|---|---|---|
| D1 | `sql/dialect/AnsiSqlRenderer.java:817` | SIZE_NOT_MULTIPLICITY | placed `bound` (`"[1]"` or `"[1..*]"`) | p0 `StringLit(bound)` -> LITERAL-String | `SqlExpr.CheckedOne` with `scalarCarrier` |
| D2 | `sql/dialect/AnsiSqlRenderer.java:825` | SIZE_NOT_MULTIPLICITY | where set (4-arg ctor) `bound` | p0 `new SqlExpr.StringLit(bound)` -> LITERAL-String; p1 `SqlExpr.Call.of(COALESCE, len, new SqlExpr.IntLit(0))` -> **COMPUTED** | `SqlExpr.CheckedOne` |
| D3 | `sql/dialect/AnsiSqlRenderer.java:898` | TEXT_NOT_FLOAT | plain | p0 `s` -> **EITHER** | a user's `Cast(text -> DOUBLE, USER)` |
| D4 | `sql/dialect/AnsiSqlRenderer.java:944` | TEXT_NOT_INTEGER | plain | p0 `s` -> **EITHER** | `Call(PARSE_INT, text)` |
| D5 | `sql/dialect/AnsiSqlRenderer.java:2095` | FRACTION_NOT_KEPT | placed | p0 `StringLit("the " + kind + " parameter '" + p.name() + "' has more fraction digits than " + ...)` -> LITERAL-String | a `PlanParam` declared Date/DateTime on a database keeping < 9 digits |
| D6 | `sql/dialect/AnsiSqlRenderer.java:2491` | CSV_TOO_WIDE | placed | p0 `StringLit("a record of the CSV file has more cells than its table's " + width + " column(s)")` -> LITERAL-String | `SqlDml.LoadCsv` (`csvRecordCheck`, called at `H2.java:322`) |
| D7 | `sql/dialect/BigQuery.java:2390` | **variable**: MAP_KEYS_MISALIGNED (`:2344`), MAP_KEY_NULL (`:2379`), MAP_KEYS_NOT_UNIQUE (`:2382`) | where set (4-arg ctor), **no operand** | none (`List.of()`) | `MAP_FROM_LISTS` and its key check |
| D8 | `sql/dialect/ClickHouse.java:2105` | **variable**: VALUE_NOT_HELD (`:839`, `:2367`, `:2368`), FRACTION_NOT_KEPT (`:841`) | placed | p0 `StringLit(message)` (`DATE_RANGE` or `FRACTION`, `ClickHouse.java:2096-2101`) -> LITERAL-String | a Date/DateTime `PlanParam`; `ADD_INTERVAL` |
| D9 | `sql/dialect/DuckDb.java:1276` | TEXT_NOT_FLOAT | plain | p0 `v` -> **EITHER** | a user's `Cast(text -> DOUBLE, USER)` |
| D10 | `sql/dialect/FrameGuard.java:83` | **variable**: WINDOW_FRAME_INVERTED (`:37`), WINDOW_FRAME_OFFSET_INVALID (`:46`) | plain | none | a `WindowCall` whose frame has a computed offset (`AnsiSqlRenderer.java:1065`) |
| D11 | `sql/dialect/MariaDb.java:1728` | VALUE_NOT_HELD | placed `"NaN or an infinity"` | LITERAL-String | a user's `Cast(text -> DOUBLE, USER)` |
| D12 | `sql/dialect/MariaDb.java:1731` | TEXT_NOT_FLOAT | plain | p0 `v` -> **EITHER** | same cast |
| D13 | `sql/dialect/MariaDb.java:2066` | VALUE_NOT_HELD | placed `"a date past the year 9999 has no " + product() + " value"` | LITERAL-String | `ADD_INTERVAL`, `ADD_INTERVAL_TEMPORAL` |
| D14 | `sql/dialect/MariaDb.java:2116` | VALUE_NOT_HELD | placed `<acos\|asin> + " of a value outside [-1, 1] is NaN, which this database cannot hold"` | LITERAL-String | `ACOS`, `ASIN` (`:1966`, `:1967`) |
| D15 | `sql/dialect/MariaDb.java:2285` | TEXT_NOT_INTEGER | plain | p0 `s` -> **EITHER** | `Call(PARSE_INT, text)` (`:1984`) |
| D16 | `sql/dialect/MariaDb.java:2795` | TEXT_NOT_DATE | plain | p0 `x` -> **EITHER** | `Call(PARSE_DATE, text)` (`:2058`) |
| D17 | `sql/dialect/MongoDb.java:2173` | **variable**: TEXT_NOT_FLOAT (`:643-645`), TEXT_NOT_INTEGER (`:1062-1063`) | plain | p0 `text` -> **EITHER** | the cast / `PARSE_INT` |
| D18 | `sql/dialect/Postgres.java:452` | **variable**: MAP_KEYS_MISALIGNED (`:395`), MAP_KEY_NULL (`:443`), MAP_KEYS_NOT_UNIQUE (`:445`) | where set (4-arg ctor), **no operand** | none | `MAP_FROM_LISTS` and its key check |
| D19 | `sql/dialect/SQLite.java:458` | VALUE_NOT_HELD | placed `"an integer past 64 bits has no SQLite value"` | LITERAL-String | a HUGEINT-labelled delivered value |
| D20 | `sql/dialect/SQLite.java:1334` | VALUE_NOT_HELD | placed `"parseFloat of NaN is NaN, which this database cannot hold"` | LITERAL-String | a user's `Cast(text -> DOUBLE, USER)` |
| D21 | `sql/dialect/SQLite.java:1341` | TEXT_NOT_FLOAT | plain | p0 `t` -> **COMPUTED** (always: `SqlExpr.Column.derived(null, "__pf_t")`) | same cast |
| D22 | `sql/dialect/SQLite.java:1773` | VALUE_NOT_HELD | placed `"a date past the years 0000-9999 has no SQLite value"` | LITERAL-String | `ADD_INTERVAL`, `ADD_INTERVAL_TEMPORAL` |
| D23 | `sql/dialect/SQLite.java:1823` | VALUE_NOT_HELD | placed `<acos\|asin> + " of a value outside [-1, 1] is NaN, which this database cannot hold"` | LITERAL-String | `ACOS`, `ASIN` (`:1687`, `:1688`) |
| D24 | `sql/dialect/SQLite.java:1946` | TEXT_NOT_INTEGER | plain | p0 `s` -> **EITHER** | `Call(PARSE_INT, text)` (`:1705`) |
| D25 | `sql/dialect/SQLite.java:2445` | TEXT_NOT_DATE | plain | p0 `x` -> **EITHER** | `Call(PARSE_DATE, text)` (`:1765`) |

### Quotes and notes

**D1 / D2** `AnsiSqlRenderer.java:807-827` (`checkedOne`):
```java
String bound = co.atLeastOnly() ? "[1..*]" : "[1]";
if (co.scalarCarrier()) {
    ...
            SqlExpr.Call.of(com.legend.sql.SqlFn.ERROR, SqlExpr.RaiseText.placed(
                    com.legend.sql.RaiseKind.SIZE_NOT_MULTIPLICITY, bound)))),
...
SqlExpr len = SqlExpr.Call.of(com.legend.sql.SqlFn.LIST_LENGTH,
        co.list());
...
SqlExpr sizeErr = SqlExpr.Call.of(com.legend.sql.SqlFn.ERROR, new SqlExpr.RaiseText(
        com.legend.sql.RaiseKind.SIZE_NOT_MULTIPLICITY, java.util.List.of(new SqlExpr.StringLit(bound),
                SqlExpr.Call.of(com.legend.sql.SqlFn.COALESCE, len, new SqlExpr.IntLit(0))), null, bound));
```
The `CheckedOne` nodes come from `lowering/Coercions.java:75`, `:83`, `:169` and `lowering/Lowerer.java:1094`. No product
code constructs one with `scalarCarrier == true` (the 1-argument constructor and `Coercions:169`'s
`(list, false, true)` are the only ones; `MirJson.java:1033` can read one back), so D1 is reachable only from a
deserialized tree. `edge/EngineStyleH2.java:253` overrides `checkedOne` to the bare value (no raise).

**D3** `AnsiSqlRenderer.java:894-901` (`guardedParseFloat(writer, s, matches, cast)`):
```java
.piece(w -> raiseAs(w, SqlExpr.Call.of(com.legend.sql.SqlFn.ERROR,
        new SqlExpr.RaiseText(com.legend.sql.RaiseKind.TEXT_NOT_FLOAT, java.util.List.of(s))),
        com.legend.sql.SqlType.Scalar.DOUBLE))
```
Callers, each passing `SqlExpr x = c.value();` of a cast that `isParseFloat(c)` (`AnsiSqlRenderer.java:930-934`: target
DOUBLE, reason USER, value typed VARCHAR): `BigQuery.java:1373`, `ClickHouse.java:1719`, `Databricks.java:996`,
`H2.java:1328`, `Postgres.java:1481`, `Snowflake.java:1316`, `Trino.java:840`. The cast is built by
`castFamily(Type.Primitive.FLOAT, ...)` for parseFloat and toFloat (`lowering/Scalars.java:2406-2407`, `:2683-2687`:
`new SqlExpr.Cast(args.get(0), PureSql.type(target), SqlExpr.Reason.USER)`) and by a user's cast of a String
(`lowering/CastPolicy.java:172`, `:197`). So `s` is the lowered text argument, unwrapped: `StringLit` for
`parseFloat('abc')`, COMPUTED for a column or parameter.

**D4** `AnsiSqlRenderer.java:940-947` (`guardedParseInt(writer, s, matches, cast)`):
```java
.piece(w -> raiseAs(w, SqlExpr.Call.of(com.legend.sql.SqlFn.ERROR,
        new SqlExpr.RaiseText(com.legend.sql.RaiseKind.TEXT_NOT_INTEGER, java.util.List.of(s))),
        com.legend.sql.SqlType.Scalar.BIGINT))
```
Callers, each passing the `PARSE_INT` call's `args().get(0)`: `BigQuery.java:1545` (from `:1671`), `ClickHouse.java:1705`
(from `:1835`), `Databricks.java:1078`, `DuckDb.java:566`, `H2.java:1301` (from `:658`), `Postgres.java:751` (from `:793`),
`Snowflake.java:1521`, `Trino.java:936`. The only builder of `PARSE_INT` is `lowering/Scalars.java:2404`
(`SqlExpr.Call.of(SqlFn.PARSE_INT, args.get(0))`): the lowered text argument, unwrapped.

**D5** `AnsiSqlRenderer.java:2095-2098` (`placeholder`):
```java
SqlExpr raise = SqlExpr.Call.of(com.legend.sql.SqlFn.ERROR, SqlExpr.RaiseText.placed(
        com.legend.sql.RaiseKind.FRACTION_NOT_KEPT, "the " + kind
        + " parameter '" + p.name() + "' has more fraction digits than " + getClass().getSimpleName()
        + " keeps (" + kept + "): refused, never cut (S181)"));
```

**D6** `AnsiSqlRenderer.java:2489-2493` (`csvRecordCheck(int width)`):
```java
SqlExpr.Column.of(null, new com.legend.sql.OutputCol("C" + width, com.legend.sql.SqlType.Scalar.VARCHAR,
        true))), SqlExpr.Call.of(com.legend.sql.SqlFn.ERROR, SqlExpr.RaiseText.placed(
                com.legend.sql.RaiseKind.CSV_TOO_WIDE, "a record of the CSV"
+ " file has more cells than its table's " + width + " column(s)")))), new SqlExpr.BoolLit(true), SqlExpr.Reason.RAISE);
```

**D7** `BigQuery.java:2388-2392` (`mapRaise(kind)`):
```java
return w -> raiseEnvelope(w, raiseArgs(new SqlExpr.RaiseText(kind, List.of(), null,
        "a map built from its key and value lists")), "ERROR");
```

**D8** `ClickHouse.java:2104-2106` (`raised(kind, message)`):
```java
private static SqlExpr.Call raised(com.legend.sql.RaiseKind kind, String message) {
    return SqlExpr.Call.of(SqlFn.ERROR, SqlExpr.RaiseText.placed(kind, message));
}
```
Callers: `:839` `raised(com.legend.sql.RaiseKind.VALUE_NOT_HELD, DATE_RANGE)`, `:841`
`raised(com.legend.sql.RaiseKind.FRACTION_NOT_KEPT, FRACTION)`, `:2367` and `:2368`
`raised(com.legend.sql.RaiseKind.VALUE_NOT_HELD, DATE_RANGE)`.

**D9** `DuckDb.java:1272-1277`:
```java
SqlExpr v = c.value();
...
        .expr(SqlExpr.Call.of(SqlFn.ERROR, new SqlExpr.RaiseText(com.legend.sql.RaiseKind.TEXT_NOT_FLOAT,
                List.of(v))), 0)
```

**D10** `FrameGuard.java:82-84`:
```java
private static SqlExpr raise(com.legend.sql.RaiseKind kind) {
    return SqlExpr.Call.of(SqlFn.ERROR, new SqlExpr.RaiseText(kind, List.of()));
}
```

**D11 / D12** `MariaDb.java:1728-1732` (`SqlExpr v = c.value();` at `:1690`):
```java
.piece(raise(raiseArgs(SqlExpr.RaiseText.placed(com.legend.sql.RaiseKind.VALUE_NOT_HELD,
        "NaN or an infinity")), SqlType.Scalar.DOUBLE))
.append(" WHEN ").expr(v, 4).append(" IS NOT NULL THEN ")
.piece(raise(raiseArgs(new SqlExpr.RaiseText(com.legend.sql.RaiseKind.TEXT_NOT_FLOAT, List.of(v))),
        SqlType.Scalar.DOUBLE))
```

**D13** `MariaDb.java:2066-2068`:
```java
.piece(addInterval(a, c)).append(", ").piece(raise(raiseArgs(SqlExpr.RaiseText.placed(
        com.legend.sql.RaiseKind.VALUE_NOT_HELD,
        "a date past the year 9999 has no " + product() + " value")), typeOf(c))).append(") END)");
```

**D14** `MariaDb.java:2116-2119`:
```java
.piece(raise(raiseArgs(SqlExpr.RaiseText.placed(com.legend.sql.RaiseKind.VALUE_NOT_HELD,
        c.fn().name().toLowerCase(java.util.Locale.ROOT)
        + " of a value outside [-1, 1] is NaN, which this database cannot hold")),
        SqlType.Scalar.DOUBLE))
```

**D15** `MariaDb.java:2285-2286`:
```java
.piece(raise(raiseArgs(new SqlExpr.RaiseText(com.legend.sql.RaiseKind.TEXT_NOT_INTEGER, List.of(s))),
        SqlType.Scalar.BIGINT)).append(" END)");
```

**D16** `MariaDb.java:2795-2796`:
```java
.piece(utcOf(v -> v.expr(x, 0))).append(", ").piece(raise(raiseArgs(new SqlExpr.RaiseText(
        com.legend.sql.RaiseKind.TEXT_NOT_DATE, List.of(x))), SqlType.Scalar.TIMESTAMP))
```
`x` is the `PARSE_DATE` call's argument, built only at `lowering/Scalars.java:2448-2478`: a `StringLit` **rewritten at
planning** when the query wrote a literal without a zone (`'T'` replaced by a space, an hour or minute form padded,
lines 2463-2469), else the lowered text. A zone-carrying literal never reaches `PARSE_DATE` (2457-2462).

**D17** `MongoDb.java:2169-2174` (`guardedParse(writer, text, grammar, conversion, kind)`):
```java
.expr(SqlExpr.Call.of(SqlFn.ERROR, new SqlExpr.RaiseText(kind, List.of(text))), 0).append("]}"));
```
Callers `:643-645` (`c.value()`, `RaiseKind.TEXT_NOT_FLOAT`) and `:1062-1063` (`a.get(0)`, `RaiseKind.TEXT_NOT_INTEGER`).

**D18** `Postgres.java:450-455` (`mapRaise(kind)`):
```java
SqlWriter.Piece text = raise(raiseArgs(new SqlExpr.RaiseText(kind, List.of(), null,
        "a map built from its key and value lists")), null, true);
```

**D19** `SQLite.java:458-459`:
```java
SqlExpr.Call.of(SqlFn.ERROR, SqlExpr.RaiseText.placed(com.legend.sql.RaiseKind.VALUE_NOT_HELD,
        "an integer past 64 bits has no SQLite value")))),
```

**D20 / D21** `SQLite.java:1332-1341`:
```java
SqlExpr t = SqlExpr.Column.derived(null, "__pf_t");
return writer.append("(SELECT CASE WHEN __pf_t IS NULL THEN NULL WHEN __pf_t IN ('NaN', '+NaN', '-NaN') THEN ")
        .piece(raise(raiseArgs(SqlExpr.RaiseText.placed(com.legend.sql.RaiseKind.VALUE_NOT_HELD,
                "parseFloat of NaN is NaN, which this database cannot hold"))))
        ...
        .piece(raise(raiseArgs(new SqlExpr.RaiseText(com.legend.sql.RaiseKind.TEXT_NOT_FLOAT, List.of(t)))))
```
`__pf_t` is the dialect's own derived column: the text trimmed and its trailing `f` stripped (`:1348-1350`). **COMPUTED
even for `parseFloat('abc')`**, the one dialect where TEXT_NOT_FLOAT's operand is never a literal.

**D22** `SQLite.java:1773-1775`:
```java
.piece(addInterval(a, c)).append(", ").piece(raise(raiseArgs(SqlExpr.RaiseText.placed(
        com.legend.sql.RaiseKind.VALUE_NOT_HELD,
        "a date past the years 0000-9999 has no SQLite value")))).append(") END)");
```

**D23** `SQLite.java:1823-1825`:
```java
.piece(raise(raiseArgs(SqlExpr.RaiseText.placed(com.legend.sql.RaiseKind.VALUE_NOT_HELD,
        c.fn().name().toLowerCase(java.util.Locale.ROOT)
        + " of a value outside [-1, 1] is NaN, which this database cannot hold"))))
```

**D24** `SQLite.java:1946-1947`:
```java
.append(" AS INTEGER) ELSE ").piece(raise(raiseArgs(new SqlExpr.RaiseText(
        com.legend.sql.RaiseKind.TEXT_NOT_INTEGER, List.of(s))))).append(" END)");
```

**D25** `SQLite.java:2445-2446`:
```java
+ " = 10 OR substr(__pp.t, 11, 1) = ' ') THEN __pp.t ELSE ").piece(raise(raiseArgs(new SqlExpr.RaiseText(
        com.legend.sql.RaiseKind.TEXT_NOT_DATE, List.of(x))))).append(" END FROM (SELECT ")
```

---

## 4. Copies (not construction sites) and raises that are not a RaiseText

### Copies: a RaiseText rebuilt from an existing one

| file:line | what |
|---|---|
| `sql/SqlExpr.java:1250` | `placed`'s own body: `return new RaiseText(kind, List.of(new StringLit(where)), null, where);` (and the two convenience constructors at `:1237`, `:1241`) |
| `sql/SqlExpr.java:271` | `withChildren`: `case RaiseText r -> new RaiseText(r.kind(), cs, r.asData(), r.where());` -- the generic rebuild behind `mapChildren` |
| `sql/SqlRewriter.java:291` | `yield same ? r : new SqlExpr.RaiseText(r.kind(), cs, r.asData(), r.where());` |
| `lowering/Windows.java:187-188` | `case SqlExpr.RaiseText rt -> new SqlExpr.RaiseText(rt.kind(), rt.operands().stream().map(x -> windowize(x, partitionBy, orderBy, frame)).toList(), rt.asData(), rt.where());` |
| `executionplan/MirJson.java:988-990` | the reader: `new SqlExpr.RaiseText(named(RaiseKind.class, o.getString("kind"), "raise kind"), readExprs(o.getArr("operands")), o.getStringOr("asData", null), o.getStringOr("where", null))` (writer at `:382-392`) |
| `sql/dialect/FoldToListReduce.java:138-139` | `case SqlExpr.RaiseText r -> r.withChildren(r.children().stream().map(x -> unwrapElemRefs(x, elem)).toList());` |
| `sql/dialect/UnqualifyPivotArgs.java:93-94` | `case SqlExpr.RaiseText r -> r.withChildren(r.children().stream().map(UnqualifyPivotArgs::unqualify).toList());` |

These keep the kind, `asData` and `where` and **rewrite the operands**, so an operand's expression can change between
the construction site and the `RaiseSite` (a requalified column, a windowed reducer). One rewrite changes a literal's
kind: `Scalars.undoubled` (`lowering/Scalars.java:3007-3018`, `default -> e.mapChildren(Scalars::undoubled)`) reaches
inside a nested raise and turns a `FloatLit`, or `Cast(FloatLit, DOUBLE)`, operand into a `DecimalLit` when the raise
sits inside a decimal arithmetic's argument. `mapChildren`/`withChildren` appear 317 times in main; only the rewrites
that name `RaiseText` were read.

Traversal only (no construction): `sql/SqlExpr.java:149`, `lowering/SubselectPrune.java:292`,
`lowering/LetBinding.java:384`, `sql/dialect/AnsiSqlRenderer.java:1057, 1204, 1214, 1229, 1241`,
`sql/dialect/MongoDb.java:310`, `sql/dialect/Postgres.java:1922`, and the edge's `raiseMessage` overrides
(`edge/EngineStyleH2.java:414`, `edge/PureText.java` x12).

### Raises that are not a RaiseText (no site, no operands; pinned by `core/src/test/java/com/legend/RaiseTextGuardTest.java` `EXEMPT`)

- `lowering/VerdictSql.java:1915` `SqlExpr.Call.of(SqlFn.ERROR, SqlExpr.Column.of("i", messageOut))` -- the test road's
  verdict message, a column.
- `lowering/CanonicalRenderSql.java:668` `SqlExpr.Call.of(SqlFn.ERROR, new SqlExpr.StringLit(UNTRACKED))`.
- `sql/dialect/MongoDb.java:722, 902, 1137, 1140` `raiseText("...")` -- four constant-text pipeline raises
  (helper at `:935`).

---

## 5. The other literal kinds as operands

`NullLit`, `TimeLit`, `ArrayLit`, `StructLit`, `DatePartLit`, `FormatLit`, `TypeNameLit`, `JsonKindLit`: **none is built
as an operand at any of the 51 sites.** At an EITHER position one could only arrive as the lowered argument itself, and
the argument types there are `Number[1]` (rem's dividend), `Integer[1]` (quotient's dividend, `at`'s index, slice's
bounds), `String[1]` (fail's message, the parsed texts) and `Date[1]` (the date parts), none of which lowers to one of
those eight (`TimeLit` is a StrictTime literal and no site takes a StrictTime; a singleton `[x]` lowers to `x`, not an
`ArrayLit`). `NullLit` would need the empty literal where the signature says `[1]`; that is not type-admissible as
written, but it was not proven impossible through a cast. Under `Operand.of` all eight would be COMPUTED.

`BoolLit` (in the prototype's list, not in the eight): no position is Boolean-typed, so it does not occur.
Note HEAD's `wordsOrNull` did not accept `BoolLit` either (its literal test, HEAD `PureMessages.java:135-137`, lists
String, Int, Float, Decimal, Date, Timestamp).

---

## 6. What legend-pure's sentence prints (`edge/PureMessages.java`)

`sentence(RaiseText)` (`PureMessages.java:22-103`, unchanged in the working tree) is the SQL sentence, the authority on
which positions upstream's text prints. Its helpers:

- `text(RaiseText r, int i)` (HEAD `:294-296`, working tree `:282-284`): `((SqlExpr.StringLit) r.operands().get(i)).value()`
  -- a hard cast: the position must be planner-known text.
- `str(SqlExpr x)` (HEAD `:308-310`, working tree `:296-298`): `new SqlExpr.Cast(x, VARCHAR, SqlExpr.Reason.PRINT)` -- the
  value as the database's cast to text writes it.
- `parts(RaiseText)` (HEAD `:284-291`, working tree `:272-279`): every operand in order, a `StringLit` as it is, any
  other through `str`.
- `dateComponent(RaiseText)` (HEAD `:263-281`, working tree `:251-269`): 4 operands -> `"Invalid day: " + y + "-" + m + "-" + d`
  from p1, p2, p3, each hard-cast to `IntLit`, **p0 ignored**; 2 operands -> `"Invalid " + text(r, 0) + ": "` then p1 as
  `IntLit` digits, `FloatLit` / `DecimalLit` plain digits (`toPlainString`), else `str(p1)`.

`wordsOrNull(kind, List<SqlExpr>)` exists at **HEAD only** (`PureMessages.java:132-187`; with `wordable` `:111-114`,
`words` `:119-126`, and the Java helpers `text(List, int)` `:241-243`, `str(List, int)` `:248-258`): it returns null as
soon as any operand is not one of `StringLit, IntLit, FloatLit, DecimalLit, DateLit, TimestampLit`, and its `str` words
only `StringLit`, `IntLit`, `DateLit` (a Float, Decimal or Timestamp literal printed through `str` is null: not worded).
`SQRT_OF_NEGATIVE` is worded only for `o.get(0) instanceof SqlExpr.FloatLit` -- which the one site (L22) never produces.

The **working tree** replaces it with `worded(kind, List<Operand>)` (`PureMessages.java:133-178`), same arms, with
`str(List<Operand>, int, Tally)` (`:205-235`): `Text` as is, `Int` digits, `Day` iso, `Flt` / `Dec` / `Bool` / `Instant`
printed in Java (counted `javaPrinted`), `Computed` -> `"<computed>"` (counted `withheld`); `text(o, i, t)` is `str`
(`:197-199`); `asFloat` (`:239-246`) for sqrt; `dateComponentWords` (`:181-193`).

| kind | `sentence` prints | planner-text positions (hard `text(r, i)`) | ignored |
|---|---|---|---|
| USER_FAIL | p0, returned as the whole message | | |
| DIVIDE_BY_ZERO | no operand: fixed `Division by zero`; else p0 through `str` (`Cannot divide <p0> by zero`) | | |
| SHIFT_OUT_OF_RANGE | nothing (fixed) | | |
| DATE_PART_ABSENT | p0 (label), p1 through `str` | p0 | |
| INDEX_OUT_OF_BOUNDS | p0 and p1 through `str` | | |
| INSERT_OUT_OF_BOUNDS | nothing | | |
| SQRT_OF_NEGATIVE | p0 through `FLOAT_SHORTEST_TEXT` | | |
| SIZE_NOT_MULTIPLICITY | 1 operand: p0 (`... of size 0 to multiplicity <p0>`); 2 operands: p1 through `str`, p0 | p0 | |
| SLICE_BOUNDS_INVERTED | p0 and p1 through `str` | | |
| DATE_COMPONENT_INVALID | 4 operands: p1, p2, p3 (hard `IntLit`); 2 operands: p0, p1 | p0 (2-operand form) | p0 (4-operand form) |
| CAST_IMPOSSIBLE | p0, p1 | p0, p1 | |
| INSTANCE_NOT_OF | p0, p1 | p0, p1 | |
| MATCH_FAILURE | p1 empty: p0; p1 not empty: p1 | p0, p1 | p0 when p1 is not empty |
| RANGE_STEP_ZERO | nothing | | |
| CONSTRAINT_UNEVALUABLE | p0 | p0 | |
| LIST_LENGTHS_DIFFER | p0 | p0 | |
| MAP_KEYS_MISALIGNED, MAP_KEY_NULL, MAP_KEYS_NOT_UNIQUE | nothing | | |
| TEXT_NOT_INTEGER, TEXT_NOT_FLOAT, TEXT_NOT_DATE | p0 raw (concatenated, no cast) | | |
| VALUE_NOT_HELD, FRACTION_NOT_KEPT, CSV_TOO_WIDE | every position, in order (`parts`) | | |
| WINDOW_FRAME_INVERTED, WINDOW_FRAME_OFFSET_INVALID | nothing | | |

Every hard-cast position (`text(r, i)`, and the `IntLit` casts of the 4-operand date form) is LITERAL at every
construction site, so the casts hold.

---

## 7. Per-kind summary

L = lowering sites, D = dialect (render-time) sites. "Printed" is from section 6.

| kind | sites | operand positions and class | printed | can a printed position be COMPUTED / EITHER? |
|---|---|---|---|---|
| DIVIDE_BY_ZERO | L 3 (`DecimalKindRules:61`, `Scalars:477`, `Scalars:2533`) | `:61` none; `:477`, `:2533` p0 EITHER (no wrapper) | p0 | **yes** (p0; not the no-operand form) |
| SHIFT_OUT_OF_RANGE | L 1 (`Scalars:437`) | none | none | no |
| DATE_PART_ABSENT | L 1 (`Scalars:1249`) | p0 LITERAL-String; p1 EITHER (`DateLit`, `TimestampLit`, `Cast(StringLit, TEMPORAL_TEXT)`, or computed) | p0, p1 | **yes** (p1) |
| INDEX_OUT_OF_BOUNDS | L 2 (`Scalars:1588`, `TaggedLists:201`) | p0 EITHER; p1 COMPUTED always | p0, p1 | **yes** (p0 EITHER, p1 always) |
| INSERT_OUT_OF_BOUNDS | L 1 (`Scalars:1629`) | none | none | no |
| SQRT_OF_NEGATIVE | L 1 (`Scalars:1927`) | p0 COMPUTED always (`Cast` to DOUBLE around EITHER) | p0 | **yes** (always) |
| SIZE_NOT_MULTIPLICITY | L 4 (`Coercions:65`, `:103`, `:162`, `ToOneGuard:69`) + D 2 (`AnsiSqlRenderer:817`, `:825`) | p0 LITERAL-String; p1 (`ToOneGuard:69`, `AnsiSqlRenderer:825` only) COMPUTED always | p0, p1 | **yes** in the 2-operand form (p1 always); no in the placed form |
| SLICE_BOUNDS_INVERTED | L 1 (`ListEncodings:267`) | p0, p1 EITHER (`IntLit` clamped at 0, or `GREATEST(arg, 0)`) | p0, p1 | **yes** |
| DATE_COMPONENT_INVALID | L 4 (`DateCtorRule:48`, `:52`, `:64`, `:83`) | `:48` String + Integer x3; `:52` String + Integer; `:64` String + Float/Decimal; `:83` String + COMPUTED | p1..p3 (4-form); p0, p1 (2-form) | **yes** at `:83` only (p1 always); the three literal forms no |
| CAST_IMPOSSIBLE | L 2 (`CastPolicy:235`, `Coercions:138`) | p0, p1 LITERAL-String | p0, p1 | no |
| INSTANCE_NOT_OF | L 1 (`Coercions:138`) | p0, p1 LITERAL-String | p0, p1 | no |
| MATCH_FAILURE | L 1 (`Coercions:138`; two origins) | p0, p1 LITERAL-String | p0 or p1 | no |
| RANGE_STEP_ZERO | L 1 (`ListRules:28`) | none | none | no |
| CONSTRAINT_UNEVALUABLE | L 1 (`CheckedEnvelope:87`, asData: never a site) | p0 LITERAL-String | p0 | no |
| LIST_LENGTHS_DIFFER | L 1 (`Scalars:2146`, placed) | p0 LITERAL-String | p0 | no |
| MAP_KEYS_MISALIGNED | D 2 (`BigQuery:2390`, `Postgres:452`) | none | none | no |
| MAP_KEY_NULL | D 2 (same two) | none | none | no |
| MAP_KEYS_NOT_UNIQUE | D 2 (same two) | none | none | no |
| TEXT_NOT_INTEGER | D 4 (`AnsiSqlRenderer:944`, `MariaDb:2285`, `MongoDb:2173`, `SQLite:1946`) | p0 EITHER (no wrapper) | p0 | **yes** |
| TEXT_NOT_FLOAT | D 5 (`AnsiSqlRenderer:898`, `DuckDb:1276`, `MariaDb:1731`, `MongoDb:2173`, `SQLite:1341`) | p0 EITHER; `SQLite:1341` COMPUTED always | p0 | **yes** |
| TEXT_NOT_DATE | D 2 (`MariaDb:2795`, `SQLite:2445`) | p0 EITHER (a literal is rewritten at planning) | p0 | **yes** |
| VALUE_NOT_HELD | L 1 (`MixedEncoding:1041`) + D 8 (`ClickHouse:2105`, `MariaDb:1728`, `:2066`, `:2116`, `SQLite:458`, `:1334`, `:1773`, `:1823`) | p0 LITERAL-String | all | no |
| FRACTION_NOT_KEPT | L 1 (`DateCtorRule:165`) + D 2 (`AnsiSqlRenderer:2095`, `ClickHouse:2105`) | `DateCtorRule:165`: String, COMPUTED, String; the two placed: LITERAL-String | all | **yes** at `DateCtorRule:165` only (p1 always); lite's own sentence, no upstream text |
| CSV_TOO_WIDE | D 1 (`AnsiSqlRenderer:2491`) | p0 LITERAL-String | all | no |
| WINDOW_FRAME_INVERTED | D 1 (`FrameGuard:83`) | none | none | no |
| WINDOW_FRAME_OFFSET_INVALID | D 1 (`FrameGuard:83`) | none | none | no |
| USER_FAIL | L 1 (`Scalars:270`) | p0 LITERAL-String `"fail"` (no argument) or EITHER | p0 | **yes** -- but never a site: the statement raises the message itself |

---

## 8. What matters for a typed Operand

1. **Literals a strict "is it a literal node" rule loses**, though the query wrote a literal:
   - `SQRT_OF_NEGATIVE` p0: always `Cast(arg, DOUBLE, OPERATOR)` (L22). Every sqrt site is COMPUTED.
   - `DATE_PART_ABSENT` p1: a year or year-month literal is `Cast(StringLit, TEMPORAL_TEXT, USER)` (L19).
   - `TEXT_NOT_FLOAT` on SQLite: the dialect's derived column `__pf_t` (D21).
   - `INDEX_OUT_OF_BOUNDS` p1 and `SIZE_NOT_MULTIPLICITY` p1: `LIST_LENGTH(...)` / a row count, computed even over a
     literal list. No cast to look through; only folding would recover a size.
2. **Literals whose value is not what the query wrote**: slice's bounds are clamped at 0 (L13); a parseDate literal has
   its `T` replaced and its time padded (D16, D25); an hour- or minute-precision date literal is a padded
   `TimestampLit`, in `dbTimeZone` when one is set (L19); a Float literal may be a `DecimalLit` (exact digits), and
   `decimalJoin` turns a `FloatLit` dividend into a `DecimalLit` (L18).
3. **A Pure type does not fix the literal class.** An Integer position can hold a `DecimalLit` (beyond a long); a Float
   position a `DecimalLit`; a Date position a `DateLit`, a `TimestampLit` or a cast `StringLit`.
4. **An operand can contain another raise** (L11's p1 is L10's guarded CASE), and an EITHER operand can be any
   expression, a raise included.
5. **Fixed text reaches the operand list three ways**: `placed` (operand 0 = the where, 15 sites); the 4-argument
   constructor with a where and separate operands (L1, L5, L11, L26, D2), or a where and **no** operand (D7, D18);
   and a plain `StringLit` operand with no where (L15, plus the labels of L7-L10, L19, L2).
6. Positions `text(r, i)` hard-casts are LITERAL-String at every site, so they need no Computed case:
   `DATE_PART_ABSENT` p0, `SIZE_NOT_MULTIPLICITY` p0, `DATE_COMPONENT_INVALID` p0, `CAST_IMPOSSIBLE` /
   `INSTANCE_NOT_OF` / `MATCH_FAILURE` p0 and p1, `LIST_LENGTHS_DIFFER` p0, `CONSTRAINT_UNEVALUABLE` p0.

---

## 9. Not determined

- **Dynamic frequency.** This is the static census: which sites a PCT or corpus run actually reaches, and how often an
  EITHER position is a literal there, was not measured (nothing was run).
- **Rewrites between lowering and render.** Only the rewrites that name `RaiseText` were read (section 4). Whether another
  `mapChildren` user (317 occurrences) or `CommonSubexpressions` can turn a literal operand into a computed one, or the
  reverse, was not checked. `Scalars.undoubled` is the one found that changes a literal's kind.
- **`NullLit` at an EITHER position**: not admissible as written (the positions are `[1]`), not proven impossible
  through a cast.
- **The exact text of a padded `TimestampLit`** for an hour- or minute-precision literal (`PureDateLiteral.canonicalText()`
  was not read), so whether it matches what legend-pure prints in `Cannot get <part> for <date>`.
- **How the edge's dialects spell `Cast(x, VARCHAR, PRINT)`** for a Float, a Decimal or a Timestamp (the `str` path),
  i.e. what the SQL sentence printed for those literals before; not read.
- `PureMessages.java` and its callers were being edited during the read; section 6 separates HEAD's `wordsOrNull` from
  the working tree's `worded`, but the working tree may have moved again since.
