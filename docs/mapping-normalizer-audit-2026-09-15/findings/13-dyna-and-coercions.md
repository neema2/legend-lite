# 13 — Dyna-function translation and declared coercions

**Scope read end to end:** `RelOpTranslator.java` (731), `DeclaredCoercions.java` (223),
`DynaFnArms.java` (56), `RequiredNullableCensus.java` (130), `MilestoningFacts.java` (30),
`model/RelationalOperation.java`, plus `translateEnumeratedSource` (`MappingNormalizer:2447`),
`translatePmToField` (`:2050`), `nullOfPhysicalKind`, `booleanizeCaseLiterals`.

---

## Finding 0 — HIGH — the reference checkout is NOT at the pin, and the receipt test is RED

`docs/TRANSLATOR_AUDIT_2026_09_15.md:8` states *"Engine tree: `$HOME/legend/legend-engine` at the pin
(4.145.0)"*. The tree on disk is at **`legend-engine-4.137.0-36-g943d38b3dc2`**.

The consequence is not theoretical — the committed surefire report (timestamped today, 11:41) shows
the registry receipt test failing:

```
spec/target/surefire-reports/com.legend.generators.DynaFnRegistryTest.txt:4
Tests run: 4, Failures: 1 ... registryMatchesTheCheckout
AssertionFailedError: DynaFn drifted from the checkout's registries
```

Parsed delta: engine=229 names, ours=232.

- **Only in ours:** `allOf`, `anyOf`, `split`
- **6 rows with differing dialect facts**, e.g. `regexpLike` — checkout says `[DUCKDB, SNOWFLAKE]`,
  `DynaFn.java` claims `[DATABRICKS, DUCKDB, MEMSQL, POSTGRES, SNOWFLAKE, SPANNER, TRINO]`; `matches`
  claims `SPANNER` the checkout does not register.

> **IMPORTANT CAVEAT — do not act on this without re-running against the pin.** This test compares our
> generated registry against the **local** checkout, which is 8 minor versions behind the 4.145.0 pin.
> It is one of the 5 `com.legend.generators.*` failures that `09-test-quality.md` independently
> attributes to upstream-checkout parity. **The red is real; its *cause* is unestablished** — the three
> extra names may simply be new in 4.145. Resolve `FIXLIST.md` P0-0 first.

What *is* solid: `DynaFn.java:9-15`'s javadoc asserts the registry is *"read from the pinned
legend-engine checkout"*, and **that assertion is false as the tree stands**. Five sibling
upstream-receipt tests are red the same way.

---

## Finding 1 — HIGH — the `toString` dynafunction is a **silent wrong translation**

`toString` is `Resolution.PURE` (`DynaFn.java`, `TO_STRING("toString", Resolution.PURE, …)`) and has
**no arm**, so it passes through to Pure's `toString`.

**Engine** (`…/core_relational/relational/sqlQueryToString/dbSpecific/h2/h2Extension2_1_214.pure`):

```
dynaFnToSql('toString',  $allStates,  ^ToSql(format='cast(%s as varchar)')),
```

**Ours** (`core/src/main/java/com/legend/lowering/Scalars.java:2220-2222`):

```java
if (t == Type.Primitive.DATE_TIME) {
    return SqlExpr.Call.of(SqlFn.STRFTIME, args.get(0),
            new SqlExpr.FormatLit(com.legend.sql.DateFmt.ISO_PURE_UTC));
}
```

…and `floatRepr` for FLOAT. So `toString(TS_COL)` yields Pure's ISO form where the engine yields the
database's `CAST … AS VARCHAR` form. **No error, no ledger row, different rows.**

**The codebase already knows this**: `RelOpTranslator.java:407-411` (the `concat` arm) says *"the
DATABASE's own formatting: '2014-01-01 06:30:00', not pure toString's ISO form — audit"* — and works
around it by emitting `cast(@String)` inside `concat` **only**. The bare `toString` dyna gets no such
treatment. **The workaround exists in one arm and is missing from the one place the same name is
spelled directly.**

This is the general shape of the risk: for a `PURE`-resolution dyna there is **no loud failure** — the
name resolves, the semantics silently differ. `dynaFnName` (`:701-715`) is loud only for `TRANSLATED`
(ISE) and `UNSUPPORTED` (`NotImplementedException`).

---

## Finding 2 — HIGH — the two coercion lanes contradict each other, and the Expression lane contradicts the engine

`DeclaredCoercions.coerceToDeclaredNumeric` (`:52-66`) — the **Expression PM** lane:

```java
String simple = declaredPlatformKind(propName, ownerClassFqn, model);
if (simple == null || !Set.of("Float","Integer","Decimal","Number",
        "DateTime","StrictDate","Date","Boolean").contains(simple)) {
    return value;
}
return new AppliedFunction(Pure.Lite.CAST_AS_DECLARED, …);
```

**It never looks at the value's kind.** This is a cast-on-declared-type-alone path — i.e. the
*defensive cast-always* pattern. **T13's "a cast only on a genuine kind mismatch" is not implemented here.**

`DeclaredCoercions.coerceColumnToDeclared` (`:175-203`) — the **Column PM** lane — does the opposite
for the very same declared kinds, and says why:

```java
// NUMERIC declared-vs-column mismatches are IDENTITY in the engine
// (SetImplTransformers passes numerics through untouched; audit 19 F7)
…
return new AppliedFunction(Pure.Lite.TYPE_AS_DECLARED, …);   // no SQL cast
```

**The engine receipt confirms the Column lane and refutes the Expression lane.**
`legend-engine-core/…/plan/execution/result/transformer/SetImplTransformers.java:95-106`:

```java
switch (transformerInput.type) {
    case "Boolean":    return this::toBoolean;
    case "StrictDate": case "DateTime": case "Date":
        return o -> o instanceof Date ? DateFunctions.fromDate((Date) o) : o;
}
return Functions.identity();
```

Numerics: identity. **No `Integer`/`Float`/`Decimal` case exists.**

`CastPolicy.lower` (`core/src/main/java/com/legend/lowering/CastPolicy.java:74-94`) does emit real SQL
for a **narrowing** cast, so **this is value-changing, not cosmetic**: a `Float`-producing expression
on an `Integer[1]` property becomes `CAST(… AS BIGINT)` (truncation) via the Expression lane, and
stays the raw float via the Column lane. **Same declared property, same underlying value, two answers
depending on which PM form the author spelled.**

---

## Finding 3 — MED — enum decode: duplicate source values resolve to the **opposite** enum value from the engine

**Ours**, `MappingNormalizer.java:2504-2541` — the chain is built back-to-front, so the
**first-declared** value's test is outermost and wins:

```java
for (int i = values.size() - 1; i >= 0; i--) {
    …
    tail = new AppliedFunction("if", List.of(disj, …then…, …tail…));
}
```

**Engine**, `legend-engine-core/…/plan/execution/nodes/helpers/ExecutionNodeResultHelper.java:46-47`:

```java
MutableMap<String,String> reverseEnumMap = UnifiedMap.newMapWith(
    …flatCollect(s -> ListIterate.collect(s.getValue(), z -> Tuples.pair(z, s.getKey()))));
return s -> reverseEnumMap.get(s == null ? "" : s.toString());
```

**Map-put semantics: the last-declared value wins.** Neither side rejects the duplicate. Silent,
value-changing divergence.

Two smaller divergences from the same three lines:

- **`NULL` source**: the engine looks up `""`, so an enum mapping declaring `''` as a source value maps
  SQL `NULL` to that enum value. Ours emits `equal(read, '')`, which is SQL `NULL` → falls to the else
  branch → `[]`.
- **Where the decode happens**: the engine's is a host-side Java map keyed on `s.toString()` (exact,
  case-sensitive, type-agnostic). Ours is emitted **into the SQL** as an `if`/`equal` chain, so
  matching is subject to the database's collation and implicit numeric coercion.

**What matches (verified):** unmapped source value → `[]` on our side (`:2496`, the empty
`PureCollection` tail) vs `null` from `reverseEnumMap.get(...)` on the engine's — same behaviour, and
yes, that null lands in a `[1]` slot on both sides. Implicit enum-mapping id
(`enumFqn.replace("::","_")`, `:2458-2464`) exactly matches `HelperMappingBuilder.getEnumerationMappingId:348-351`.
A value the enumeration does not declare is a compile error on both sides (`:2506-2513` vs
`context.resolveEnumValue(…)` at `HelperMappingBuilder.java:231`).

---

## Finding 4 — MED — `extractFromSemiStructured`: our type table rejects two legal engine types and mis-kinds `DECIMAL`

Engine's authoritative list, `…/core_relational/relational/sqlQueryToString/dbExtension.pure:904-905`:

```
let supportedTypes = ['BOOLEAN','CHAR','VARCHAR','STRING','INTEGER','DECIMAL','FLOAT','DATE','DATETIME','TIMESTAMP'];
assertContains($supportedTypes, $p2, …);
```

Ours, `RelOpTranslator.java:194-207`:

```java
case "VARCHAR", "CHAR" -> "String";
case "INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT" -> "Integer";
case "FLOAT", "DOUBLE", "REAL", "DECIMAL", "NUMERIC" -> "Float";
```

- **`STRING` and `DATETIME` are missing** → `ModelException("Unsupported SQL type …")` on two spellings
  the engine explicitly accepts. Loud, but **a rejection of legal input**.
- **`DECIMAL` → `"Float"`**, contradicting our own kind table
  (`core/src/main/java/com/legend/compiler/RelationalKinds.java:33-34` maps `Decimal`/`Numeric` →
  `"Decimal"`). **Precision loss, and an internal inconsistency.**
- We also accept `INT/BIGINT/SMALLINT/TINYINT/DOUBLE/REAL/NUMERIC/BIT`, which the engine rejects, and
  we skip the engine's path-regex assertion (`dbExtension.pure:902`).

---

## Finding 5 — MED — `booleanizeCaseLiterals` is a second, redundant Boolean mechanism whose documented motivating case is shadowed

`MappingNormalizer.java:2874-2905`. Its javadoc: *"`case(cond,'true','false')` bound to a Boolean
property coerces by EMISSION …"*

But `case(...)` only arises as a `PropertyMapping.Expression`, and `translatePmToField` (`:2079-2084`)
routes **every** Expression PM through `coerceToDeclaredNumeric` **first** — whose type set
(`DeclaredCoercions.java:55-57`) includes `"Boolean"`. So the value reaching `buildNewInstanceToOne` is
already `castAsDeclared(if(...), @Boolean)`, an `AppliedFunction` that is not `if`, and
`booleanizeCaseLiterals` returns it unchanged (`:2900-2904`). The same holds for Column PMs via
`coerceColumnToDeclared:161-165`.

**It is therefore live only for the relation-function / variant lanes** (call sites `:835`, `:875`,
`:897`), not for the legacy relational lane its doc describes. Worse, the two mechanisms give
different answers for a non-`'true'`/`'false'` string: `castAsDeclared(@Boolean)` **errors**
(documented deliberate divergence, `DeclaredCoercions.java:45-50`), `booleanizeCaseLiterals` leaves the
string for the checker, and the engine returns `FALSE` (`SetImplTransformers.toBoolean:72-75`,
`Boolean.parseBoolean`). **Three answers for one input.**

**Verdict on the brief's question:** it is **a hack around a typing gap, not a correctness fix** — it
exists because the emitted `if(cond, 'true', 'false')` does not type as `Boolean`. Delete it and the
variant lane's Boolean-declared string literals stop conforming; the relational lane is unaffected
because `DeclaredCoercions` already covers it.

---

## Finding 6 — MED — `[1]` property over a NULLABLE column: census only; the null flows, and the type system is told otherwise

`RequiredNullableCensus.pair` (`:114-118`) is terminal — it records a ledger row and returns:

```java
if (cd.primaryKey() || cd.notNull()) { return; }
ledger.census(bucket, ownerClassFqn + "." + propName + multText(c) + " over " + table + "." + column);
```

No fail, no coerce. Then `buildNewInstanceToOne` (`MappingNormalizer.java:2864-2869`) wraps the
`[1]`-declared field in `Pure.Lite.TRUST_ONE` — and the lowering **explicitly refuses to guard it**
(`core/src/main/java/com/legend/lowering/Lowerer.java:458-460`: *"`trustOne` (synthesized conformance)
never guards"*). **So a database NULL passes through a non-null slot, with the typed tree asserting
always-present.**

This is **documented as deliberate** (`RequiredNullableCensus.java:28-39`: *"pure MODEL DEBT …
Deliberately a CENSUS, not a warning … the user-facing diagnostic waits for the dialect-levels
split"*). The honesty buckets (`unresolved-property`, `unresolved-column`) are a genuinely good touch.

Recorded because the answer to "fail, coerce, or silently pass" is: **silently pass**, and nothing in
this file's javadoc says how many rows that is on the live corpus. *(And per `02-stamped-facts.md`
HIGH-2, the census it feeds has no reader at all.)*

---

## Finding 7 — MED — nothing in the verification chain checks dyna **semantics**

`spec/src/test/java/com/legend/generators/DynaFnRegistryTest.java`:

- `armsAreDerivedFromTheTranslatorSource:158-166` matches `DynaFn\.([A-Z][A-Z_0-9]+)` over the
  translator **source text** — a `DynaFn.X` mention in a comment or in `DYNA_HASH_TYPES` counts as an
  arm. **It proves a name is *mentioned*.**
- `resolutionsHold:128-132` checks a `PURE` name has *a catalog native of that name*. **Nothing
  compares the pure native's semantics to the engine's `dynaFnToSql` rendering.**

So T19's *"each function's receipt is the PCT row"* holds only for the pure function **in isolation**;
the *dyna→pure mapping* — the thing `RelOpTranslator` actually decides — **has no receipt at all.**
This is exactly what let Finding 1 (`toString`) through, and it matches the standing note that a green
check proving well-formedness will pass a placeholder.

---

## Finding 8 — MED — `columnRead` and the `ColumnRef` arm are two different rules for one job

`RelOpTranslator.columnRead:122-136` vs the `ColumnRef` arm at `:224-238`:

| | `columnRead` (`:122`) | `ColumnRef` arm (`:224`) |
|---|---|---|
| canonicalizes the table name | **no** | yes — `MappingNormalizer.canonicalTable(ref.table())` |
| table not in scope | falls back to `defaultTable` (`:129`) | **throws** (`:233-236`) |

Both are live: `columnRead` from `MappingNormalizer.java:2072` and `:2435`, the arm from every
expression translation. **A column PM and the identical column inside an expression can resolve to
different rows (or one resolves and the other dies) purely by which spelling the author used.**

---

## Finding 9 — LOW, but a crash — the hash arm has no arity guard

`RelOpTranslator.java:248-257`:

```java
case RelationalOperation.FunctionCall call
        when dyna(call) instanceof DynaFn hashed && DYNA_HASH_TYPES.containsKey(hashed) ->
        new AppliedFunction("hash", List.of(
                toOne(translate(call.args().get(0), …)),
```

`md5()` with zero arguments → raw `IndexOutOfBoundsException`. Every other arm guards arity. (All other
malformed arities fall through to `dynaFnName` and fail loudly and legibly.)

---

## Finding 10 — LOW — arity gaps where the engine is variadic

- **`add`**: engine `extensionDefaults.pure:188` → `getTransformForAddPlus()`, which joins `String[*]`
  with `' + '` — **variadic**. Our arm guards `args().size() == 2` (`:471-475`); `add(a,b,c)` hits
  `dynaFnName` → ISE. Loud, but rejects legal engine input.
- **`concat`**: arm guards `>= 2` (`:399`); engine renders `concat(x)` fine. `concat(x)` → ISE.
- **`case`**: our guard is `>= 3 && size % 2 == 1` (`:366-367`). The engine floors
  (`dbExtension.pure:919-920`) and **silently drops** the odd parameter on an even-arity call. **Ours
  is stricter — better, worth keeping.**

---

## Finding 11 — LOW — the string-literal-unit family has one arm out of five

`adjust` gets an arm converting its string literal to an `EnumValue` (`RelOpTranslator.java:298-311`).
**Four sibling dynas with the identical shape get none**, and their Pure signatures take enums:

| dyna | engine spelling | our Pure signature |
|---|---|---|
| `dateDiff(a,b,'DAYS')` | `h2Extension2_1_214.pure` — `$p->at(2)->replace('\'','')->processDateDiffDurationUnitForH2()` | `Pure.java:958` — `dateDiff(Date[1],Date[1],DurationUnit[1])` |
| `mostRecentDayOfWeek(d,'Friday')` | `formatMostRecent…` over `String[1..2]` | `Pure.java:987` — `(Date[1], DayOfWeek[1])` |
| `previousDayOfWeek` | same | `Pure.java:989` — `(Date[1], DayOfWeek[1])` |
| `cast(x,'VARCHAR')` | `extensionDefaults` — `cast(%s as %s)` | Pure `cast(v, @Type)` needs a `TypeAnnotation` |

All four are `Resolution.PURE`, so they pass through to a signature the argument cannot satisfy → type-check
failure. **Loud**, and so not a correctness hazard — but a whole legal-input family the `adjust` arm's
own pattern would have covered.

---

## Finding 12 — LOW — dead code and dead imports in scope

- `RelOpTranslator.java:5` `import com.legend.compiler.ModelBuilder;` and `:11`
  `import com.legend.model.DatabaseDefinition;` — each appears exactly once (the import line). Unused.
- `RelOpTranslator.java:415` — `parts.size() == 1 ? parts.get(0) : …` is unreachable; the arm's own
  guard is `call.args().size() >= 2` (`:399`).
- `MappingNormalizer.java:2467-2469` — `owner` comes from `.orElseThrow(...)`, so
  `owner == null ? null : …` on the next line cannot fire.
- `RelationalOperation.Lambda` / `LambdaParam` are handled by `collectTablesIn` (`:115-116`) but have
  no arm in `translate`/`translateTail`; they land on
  `default -> throw new IllegalStateException("relational-op dispatch: unexpected " + op.getClass())`
  (`:599-600`). The record's javadoc (`RelationalOperation.java:97`) promises *"the translator refuses
  it loudly until then"* — it does, but as an **internal-error type with a `getClass()` message**, not
  a `NotImplementedException` naming the feature.

---

## Finding 13 — LOW — duplication, and the method-length premise inverted

- `translateArgs:148-166` and the `Comparison` arm's inline `side` lambda (`:556-563`) implement the
  identical "bare `ColumnRef` outside a join condition gets a `TRUST_ONE` wrap" rule twice — and
  `translateArgs`'s own javadoc (`:145-147`) exists to explain that comparisons *don't* route through it.
- `dyna(call)` (`:62-64`) is a `HashMap` lookup re-evaluated in **every** `when` guard — ~26 lookups to
  reach the catch-all arm.
- **Method sizes:** `RelOpTranslator.translate` is **171 lines** (`:218-388`) and `translateTail` is
  **211 lines** (`:392-602`) — both far larger than `translateEnumeratedSource` (97) and
  `translatePmToField` (87). The `:390-391` javadoc calls the split *"at an arm boundary"* to satisfy a
  shape limit; **no test enforcing any method-length limit was found in `core/src/test`**, so the
  limit is convention only and the split produced two ~200-line switches rather than smaller units.

---

## Typing vs string hacking — the commit's claim **holds inside this scope**

Dispatch is `dyna(call) == DynaFn.X` where `dyna` is:

```java
// RelOpTranslator.java:62-64
private static @Nullable DynaFn dyna(RelationalOperation.FunctionCall call) {
    return DynaFn.of(call.name()).orElseGet(MissProbe::miss);
}
```

`DynaFn.of` (`DynaFn.java:340-342`) is an **exact, case-sensitive `HashMap` lookup into a typed enum.**
There is **no** `substring(lastIndexOf(':') + 1)` in `RelOpTranslator`, `DeclaredCoercions`,
`DynaFnArms`, `RequiredNullableCensus`, or `MilestoningFacts`.

*(The simple-name pattern does exist at `lineage/PkInference.java:47`, plus ~15 other sites —
`AssertVerdicts.java:1362/1414/1641`, `plan/PurePrint.java:82`, `plan/PlanText.java:617/649`,
`lowering/Scalars.java:1869`, `lowering/StampCensus.java:191` — all outside this dimension's scope.)*

Simple-name collision is structurally impossible here: relational-DSL function names are unqualified by
grammar, so there is nothing to truncate. The residual risk is one layer down — an unregistered name
falls through `dynaFnName:706-708` (`return call.name();`) and is emitted as a bare `AppliedFunction`.
The landing was checked: `ModelBuilder.findFunction:869` is exact-FQN-keyed via
`symbols.resolveId(fqn)`, so a bare name does not fan out across packages.
`DeclaredCoercions.declaredPlatformKind:74-93` is likewise exact (*"Suffix-matching is the banned
idiom"*, and the code matches the comment).

---

## `nullOfPhysicalKind` is dialect-blind — the ArchUnit rule holds

`MappingNormalizer.java:1335-1352`:

```java
String kind = model.knowledge().columnKind(rmMain.database(), rmMain.table(), col);
if (kind == null) { throw new NotImplementedException(…); }
return new AppliedFunction("cast", List.of(new PureCollection(List.of()),
        new TypeAnnotation.Named(new TypeExpression.NameRef(kind))));
```

The kind comes from the **DDL-declared** column type through `RelationalKinds.pureKindOf`
(`compiler/RelationalKinds.java:22-34`), a pure `RelationalDataType` → Pure-kind switch with no dialect
branch. `grep -rn "com.legend.sql.dialect" core/src/main/java/com/legend/normalizer/` returns
**nothing**, so `compileSideLayersAreDialectBlind` (`ArchitectureTest.java:607-624`) is satisfied. It
also fails loud when the kind is underivable. **No finding.**

*(Both `nullOfPhysicalKind` and `nullOfDeclaredType` are nonetheless callerless — see
`01-mapping-normalizer-core.md` §3.)*

---

## Dyna-function table

232 names in our registry; 229 in the checkout (3 delta — Finding 0, cause unestablished).
Resolutions: **158 PURE · 8 SHIM · 24 TRANSLATED · 42 UNSUPPORTED**. `DynaFnArms.ARMS` = 27
(24 TRANSLATED + `and`/`or`/`parseDate`, PURE names whose engine-only *shape* is rewritten).

> **Note on T19's count:** the rule says *"`RelOpTranslator`: 30 functions"*. The code has 27 arms /
> 24 `TRANSLATED` members. **Neither number is 30.**

### The 27 armed functions

| dyna | our translation | engine rendering | handled? | correct? |
|---|---|---|---|---|
| `md5`/`sha1`/`sha256` | `hash(toOne(x), HashType.X)` `:248-257` | `md5(%s)` etc. | yes | yes — but **no arity guard** (F9) |
| `dayOfWeek` | `toString(dayOfWeek(x))` `:258-264` | `dayname(%s)` | yes | yes |
| `dayOfWeekNumber` | 1/2-arg; Sunday → `mod(isodow,7)+1` `:265-297` | H2 `DAY_OF_WEEK(…)` | yes | yes; non-`Sunday`/`Monday` → loud, mirrors the engine assert |
| `adjust` | `adjust(a,b,DurationUnit.X)` `:298-311` | `dateadd(unit,n,d)` | yes | yes — the only string-literal-unit arm (F11) |
| `convertTimeZone` | `Lite.CONVERT_TIME_ZONE_FORMAT` `:312-315` | `transformConvertTimeZone` | yes | shim; semantics not receipted |
| `parseDate` | `Lite.PARSE_DATE_FORMAT` `:321-324` | `cast(parsedatetime(%s,%s) as timestamp)` | yes (2-arg only) | shim |
| `convertDate` | `Lite.CONVERT_DATE_FORMAT`, default `yyyy-MM-dd` `:325-332` | `convertToDateH2/DuckDB` | yes | shim |
| `convertDateTime`/`toTimestamp` | `Lite.CONVERT_DATE_TIME_FORMAT` `:333-338` | H2 only (DuckDB rows **commented out**) | yes | shim |
| `convertVarchar128` | `cast(x,@String)` `:339-342` | H2 `convert(x,VARCHAR(128))`; DuckDB `CAST(x AS VARCHAR)` | yes | matches DuckDB; **no 128 truncation** (LOW) |
| `splitPart` | `splitPart(a0, toOne(a1), cast(toOne(a2),@Integer))` `:343-364` | `split_part(%s,%s,%s)` | yes | yes |
| `case` | nested `if` `:365-384` | flat `case when… else… end` | yes | rows equivalent; SQL text differs; ours rejects even arity where the engine silently drops a param |
| `concat` | `plus`-run of `toOne(cast(@String))` `:398-417` | `concat(a,b,…)` | yes | NULL-skip semantics match; the per-arg `CAST … AS VARCHAR` is ours; `concat(x)` rejected (F10) |
| `extractFromSemiStructured` | `to(get(col,path), @T)` `:423-435` | `processExtractFromSemiStructured` | partly | **F4** |
| `indexOf` | verbatim `indexOf(a0,a1)` `:446-452` | H2 `LOCATE(a1,a0)`, DuckDB `instr(a0,a1)` | yes | **yes** — arg order verified both dialects |
| `substring` | verbatim `:458-462` | verbatim | yes | yes (1-based passthrough) |
| `add` / `sub` | `plus`/`minus` run over `toOneAll` `:471-480` | `%s + %s` (variadic) / `%s-%s` | yes (2-arg) | yes for 2-arg; variadic `add` rejected (F10) |
| `position` | `indexOf(toOne(arg1), arg0)` `:484-495` | H2 `position(needle, hay)`, DuckDB `position(needle IN hay)` | yes | **yes** — reversal verified both dialects |
| `isNull`/`isNotNull` | `isEmpty`/`isNotEmpty` `:496-503` | `%s is [not] null` | yes | yes |
| `group` | unwrap `:504-507` | `(%s)` | yes | yes |
| `if` | `if(c, {\|t}, {\|e})` `:508-518` | `case when … end` | yes | yes |
| `and`/`or` (>2 args) | left-fold to binary `:519-533` | `makeString(' and '/' or ')` | yes | yes |

### Not armed — the risk band

| class | count | behaviour on encounter |
|---|---|---|
| `UNSUPPORTED` | 42 | **LOUD** — `NotImplementedException` naming the operator *and its dialects* (`:711-713`) |
| `TRANSLATED` at an unexpected arity | — | **LOUD** — `IllegalStateException("declared TRANSLATED but no translator arm rewrote it")` (`:709-710`) |
| `SHIM` | 8 | passes through under its `Lite` identity |
| `PURE`, semantics assumed equal | 158 | **SILENT** if the engine's SQL rendering ≠ Pure's function — the `toString` hole (F1), the enum-arg family (F11) |
| name the engine registers as no dyna | — | emitted under its own name; resolves exactly or fails in the Typer |

---

## What is genuinely good

1. **The loud-by-construction fall-through.** `dynaFnName:701-715` is the best thing in this scope. A
   `TRANSLATED` name with no matching arm **raises** rather than passing through — so every arity guard
   in F10 fails *loudly*, never silently. `UNSUPPORTED` names the operator **and the engine dialects
   that register it**, a diagnostic a user can act on. **For the 24 `TRANSLATED` and 42 `UNSUPPORTED`
   names, an unhandled dyna is unambiguously loud.** The silent band is only `PURE`-resolution semantic
   drift (F1).
2. **The registry as a typed enum, generated, with a regeneration path.** `DynaFn` carries the engine's
   name, the registering dialects, and inference-map membership as data, with `-Ddynafn.generate=1` to
   re-derive from the checkout. **When the checkout matches the pin this is a genuinely strong receipt
   mechanism — it caught the drift in F0 exactly as designed.** The failure is the stale tree, not a
   weak instrument.
3. **`declaredPlatformKind` (`DeclaredCoercions.java:74-93`) does exact identification.** The bare
   spelling *only when not shadowed by a user class*, or the full platform FQN — with the reason
   spelled out (`m::Number` must never coerce) and *"Suffix-matching is the banned idiom"* in the
   comment. **The code matches the comment.**
4. **The `RequiredNullableCensus` honesty buckets** (`:99-113`): `unresolved-property` and
   `unresolved-column` rows for pairings the adjudication can't resolve, with the rationale *"a silent
   skip would read as covered."* Precisely the discipline most censuses lack. Accumulating on the
   compile's own `ModelBuilder` rather than static state (`:41-47`) is also right.
5. **`nullOfPhysicalKind` / `nullOfDeclaredType`**: dialect-blind, DDL-derived, loud when the kind is
   underivable. **No default-to-VARCHAR anywhere.**
6. **Arg-order verification on `position` / `indexOf`.** The classic place to get a reversal wrong, and
   both are right against both H2 and DuckDB — including the note that the pre-shift/re-shift pair was
   dropped on *both* sides together rather than left half-migrated.
7. **`case` is stricter than the engine.** The engine's `processCase` floors an even parameter count
   and silently discards a parameter; ours rejects it. **Keep that.**
8. **`MilestoningFacts`** (30 lines): a fold over `model.knowledge().lineage(...)` with no hierarchy
   walk of its own, and the class comment records that the bitemporal twin was deleted for having no
   caller. Nothing to report.
