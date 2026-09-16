# Leg 7 — `parseDate` kinds (`testParseDateTypes`)

**Row:** `testParseDateTypes`. **Charted error:** bare `Assert failed`
(“instanceOf checks over parsed kinds — likely near-passing after B8/J8a;
re-diagnose first”).

> ## RE-DIAGNOSIS VERDICT: **REFUTED**
>
> The charter's stated mechanism is **wrong**, not merely stale — and its
> "likely near-passing" is **wishful and self-contradicting**.
>
> **Leg 7 is not a date-kind leg at all.** Date kinds are modelled completely
> and selected correctly. It is the **A24/D92 boolean-carrier defect**, already
> a CONFIRMED ledgered unsoundness, surfacing through a date test.

See `README.md` for the shared tenet quick-reference and provenance notes.

---

## The three refutations

**The charter row, verbatim** — `docs/CHANNELB_BURNDOWN_HANDOFF.md:56`:

> `| 7. parseDate kinds | testParseDateTypes | Assert failed (instanceOf checks over parsed kinds — likely近 after B8/J8a; re-diagnose first) |`

(The file literally contains a stray CJK codepoint `近` where a word was
intended. The string "near-passing" appears **nowhere** in
`/Users/neema/legend/legend-lite/docs/`.)

1. **The message cannot come from an `instanceOf` check.** `assertInstanceOf` in
   our tree is a *platform-owned native* (`PlatformTypes.java:235-236`, registered
   at **:245**; declared `Pure.java:2059-2060`; doctrine at `Pure.java:2052-2058` —
   "the parsed bodies suppress"), adjudicated at `AssertVerdicts.java:262-277`,
   whose failure text is `PureAsserts.assertInstanceOf`'s formatted string —
   `"expected " + repr(v) + " to be an instance of " + type + ", actual: " + actual`
   (`PureAsserts.java:196-197`). Exactly **two** sites in the whole tree emit the
   literal `Assert failed`: `AssertVerdicts.java:260` (the `assert`/`assertFalse`
   arm) and `AssertVerdicts.java:460` (the *quantified* default message). **Both
   are `assert`/`assertFalse`.** So the failing line is `parseDate.pure:44` or
   `:48` — a `has*` predicate — not either `assertInstanceOf`.

2. **The charter postdates both landings it hopes for.** Its header is
   `opened 2026-08-27`; its roster line reads
   `## 1. The roster (measured 2026-08-27, full lane 1115/0 green)`, and its own
   §Predecessor-state (**:13-20**) lists B8 as already landed. J8a landed the same
   day (`ADAPTER_NECESSITY_CENSUS.md:112`, `OPEN_REGISTER.md:70`, code at
   `Scalars.java:2268-2278`). The `Assert failed` it records was **measured with
   both landings in the tree**.

3. **B8 has no causal surface here at all.** `ADAPTER_NECESSITY_CENSUS.md:259`
   describes B8 entirely in terms of `CFloat`/`TypedCFloat` exact digits,
   `DecimalLit` emission, the equality fold, and BigDecimal-under-FLOAT bridge
   decode. No temporal surface whatsoever.

### THE TRUE CURRENT MECHANISM

> `assert($dt->hasSecond())` (`parseDate.pure:44`) fails because `hasSecond` over
> a **non-`TypedCDate`** argument emits `SqlExpr.IntLit(1)` instead of
> `SqlExpr.BoolLit(true)` (`Scalars.java:729-737`). The output label flips
> `BOOLEAN → BIGINT` at `SqlTyping.reconcileLabels` (`SqlTyping.java:213-218`),
> the wire delivers a Java `Integer`/`Long`, and `AssertVerdicts.java:259` tests
> `Boolean.TRUE.equals(c)` — false for a `Number`. `held = false` →
> `fail("Assert failed")` (`AssertVerdicts.java:260`).

**The computed truth value is correct.** `datePrecision`
(`Scalars.java:3378-3411`) reads the *stamp*: `DATE_TIME → SUBSECOND`,
`STRICT_DATE → DAY`. The stamps are right (`Typer.refineParseDate`,
`Typer.java:1645-1664`). **Only the carrier type is wrong.** This is a **DEFECT in
the emission's SQL kind**, not a missing feature — we model date kinds fully and
pick them correctly.

Not a new discovery — an already **CONFIRMED, ledgered unsoundness**:

- `docs/type-audit-2026-08/findings/A24-scalar-functions.md:158-179`: *"[UNSOUND]
  `hasDay/hasHour/hasMinute/hasMonth/hasSecond/hasSubsecond` on a COLUMN emit an
  INTEGER, and the decoder hands back `java.lang.Integer` for a declared
  `Boolean[1]`"* — with a live repro at **:167-170** (`SQL: 1 AS x` → `Integer(1)`)
  and the money line at **:178-179**: *"the same Pure function returns two
  different Java types depending on whether its argument is a literal."*
- `docs/type-audit-2026-08/V2-falsifier.md:229` (A24, **CONFIRMED**);
  `docs/type-audit-2026-08/MASTER.md:100` (D92, **CONFIRMED**).
- `docs/AUDIT_23_SPECIAL_CASING.md:173-175` ledgers it as a *deliberate*
  divergence — a claim §d shows is **ungrounded**.

---

## a) REFERENCE SEMANTICS

*(all citations `/Users/neemsandv/...`)*

### a.1 `testParseDateTypes` — ALL FOUR assertions, exact

`legend-pure/legend-pure-core/legend-pure-m3-core/src/main/resources/platform/pure/essential/string/parse/parseDate.pure:40-50`:

```
40  function <<PCT.test>> meta::pure::functions::string::tests::parseDate::testParseDateTypes<Z|y>(f:...):Boolean[1]
41  {
42     let dt = $f->eval(|parseDate('2014-02-27T00:00:00.000000'));
43     assertInstanceOf($dt, DateTime);
44     assert($dt->hasSecond());
45
46     let date = $f->eval(|parseDate('2014-02-27'));
47     assertInstanceOf($date, StrictDate);
48     assertFalse($date->hasHour());
49
50  }
```

| # | line | assertion | literal | asserted fact | failure text it *can* produce |
|---|---|---|---|---|---|
| 1 | `:43` | `assertInstanceOf($dt, DateTime)` | `'2014-02-27T00:00:00.000000'` | kind = `DateTime` | `expected … to be an instance of DateTime, actual: …` |
| 2 | `:44` | `assert($dt->hasSecond())` | (same value) | second-precision | **`Assert failed`** |
| 3 | `:47` | `assertInstanceOf($date, StrictDate)` | `'2014-02-27'` | kind = `StrictDate` | `expected … to be an instance of StrictDate, actual: …` |
| 4 | `:48` | `assertFalse($date->hasHour())` | (same value) | no hour | **`Assert failed`** |

Exactly four. Message provenance: `assert.pure:29-32` (`assert(cond)` →
`assert(cond,'Assert failed')`), `assert.pure:34-37` (`assertFalse(c)` →
`assert(!c)` → same string), vs `assertInstanceOf.pure:17-20`
(`format('expected %r to be an instance of %s, actual: %s', …)`).

### a.2 The date type lattice — exact and complete

Declared **only** in `legend-pure/.../platform/pure/grammar/m3.pure`; no other
file in either reference repo declares them.

| edge | file:line |
|---|---|
| `Date <: Any` | `m3.pure:1564` (general) / `:1565` (specific) |
| `StrictDate <: Date` | `m3.pure:1575` / `:1576` |
| `DateTime <: Date` | `m3.pure:1586` / `:1587` |
| `LatestDate <: Date` | `m3.pure:1597` / `:1598` |

Declarations at `m3.pure:1558` (Date), **:1569** (StrictDate), **:1580**
(DateTime), **:1591** (LatestDate). Complete primitive set at
`legend-pure-m4/.../ModelRepository.java:79`. **StrictDate, DateTime and
LatestDate are pairwise incomparable** — each declares exactly one generalization,
and it is `Date`. There is no fifth date kind.

```
Any └── Date ├── StrictDate ├── DateTime └── LatestDate
```

Subtype walk: `legend-pure-m3-core/.../navigation/type/Type.java:184-190`;
`M3ProcessorSupport.java:207-213`.

### a.3 The kind-selection rule by input FORM — the heart of the leg

**Parser** — `legend-pure-m4/.../primitive/date/DateFormat.java:415-604`
(`parsePureDate(String,int,int)`), reached from `DateFunctions.parsePureDate`
(`DateFunctions.java:321-324`), which is what the compiled native calls
(`.../compiled/.../natives/essentials/string/parse/ParseDate.java:19-25`,
`super("…DateFunctions.parsePureDate", …)`).

**Kind assignment** — `DateFunctions.java:132-147`:

```java
public static String datePrimitiveType(PureDate pureDate) {
    if (LatestDate.isLatestDate(pureDate)) return LATEST_DATE_TYPE_NAME;
    if (pureDate.hasHour())                return DATETIME_TYPE_NAME;
    if (pureDate.hasDay())                 return STRICT_DATE_TYPE_NAME;
    return DATE_TYPE_NAME;
}
```

**The exhaustive form table** (every branch `DateFormat` takes):

| input form | example | Java class | `DateFormat.java` line | Pure type |
|---|---|---|---|---|
| year only | `2014` | `Year` | `:458` | **Date** |
| year-month | `2014-02` | `YearMonth` | `:480` | **Date** |
| year-month-day | `2014-02-27` | `StrictDate` | `:502` | **StrictDate** |
| +hour | `…T10` | `DateWithHour` | `:524` | **DateTime** |
| +minute | `…T10:01` | `DateWithMinute` | `:547` | **DateTime** |
| +minute +TZ | `…T10:01+0500` | `DateWithMinute`, `addMinutes(-offset)` | `:552-558` | **DateTime** |
| +second | `…T10:01:35` | `DateWithSecond` | `:573` / `:593` | **DateTime** |
| +subsecond | `…T10:01:35.231` | `DateWithSubsecond` | `:589` | **DateTime** |
| +second/subsecond +TZ | `…-0500`, `…Z` | same class, shifted | `:596-601` | **DateTime** |
| `Z` suffix | any | offset 0 → class unchanged | `:609-612` | unchanged |
| numeric `±HHMM` | `-0500` | class preserved | `:638-641` | unchanged |
| **named zone** (`EST`, `GMT`) | — | **throws** `IllegalArgumentException("Invalid time zone: …")` | `:630` / `:635` | n/a |
| leading `%` | `%2014-02-27` | prefix stripped | `:436-443` | — |
| leading `-` (BCE) | `-0044-03-15` | accepted | `:447` | — |
| non-padded m/d | `2014-2-27` | accepted | `:470`, `:492` | — |

**`parseDate` can never return `LatestDate`** — `%latest` fails at **:455**
("Error parsing year"); `LatestDate.instance` is only ever produced by explicit
grammar handling (`ModelRepository.java:428-431`).

Static return type is **never narrowed**: `legend-engine/.../Handlers.java:2402`
registers `res("Date","one")`. The kind is a *runtime* fact.

Guards on explicit narrowing: `ModelRepository.java:476-484`
(`newStrictDateCoreInstance` throws if `!hasDay() || hasHour()`), **:486-492**
(`newDateTimeCoreInstance` throws if `!hasHour()`).

### a.4 `instanceOf` — **RUNTIME**, not static, in every reference backend

- **Interpreted**:
  `.../interpreted/natives/essentials/meta/type/InstanceOf.java:55-60` →
  `Instance.instanceOf(value, type, ps)` →
  `legend-pure-m3-core/.../navigation/Instance.java:79-82` =
  `type_subTypeOf(getClassifier(instance), type)`. The classifier was fixed at
  construction by `datePrimitiveType` (`ModelRepository.java:471-474`).
- **Compiled**: `.../compiled/.../natives/essentials/meta/type/InstanceOf.java:50-51`
  emits **`<JavaClass>.class.isInstance(param0)`**;
  `TypeProcessor.java:212/216/224/228` maps `Date→PureDate`,
  `StrictDate→StrictDate`, `DateTime→DateTime`, `LatestDate→LatestDate`. Dynamic
  path `Pure.java:797-826` → `javaClass.isInstance(obj)`.
- Java hierarchy makes the lattice hold: `StrictDate.java:21`
  (`final … extends AbstractDateWithDay`), `DateTime.java:17` (interface),
  `AbstractDateWithHour.java:19` (`extends AbstractDateWithDay implements DateTime`).
  `StrictDate` is **not** a `DateTime`; `Year`/`YearMonth` are neither.

**Consequence for us:** the reference tests a **runtime tag on the value**. A SQL
`DATE` column has no per-value kind tag, so legend-lite must carry the kind — and
it does, in two places (§b).

### a.5 `has*` — RUNTIME value methods, and **NO SQL translation exists**

- Declarations: `essential/date/has/hasSecond.pure:17`, `hasHour.pure:17`,
  `hasDay.pure:17`, `hasMonth.pure:17`, `hasMinute.pure:17`,
  `hasSubsecond.pure:17`, `hasSubsecondWithAtLeastPrecision.pure:17` — all
  `(d:Date[1]):Boolean[1]`.
- Interpreted: `NativeDateHasElementFunction.java:44-48` → `date.hasSecond()` etc.
  (`HasSecond.java:28-32`, `HasHour.java:28-32`, …). No static type consulted.
- Truth table on the value classes: `StrictDate.java:33-37` (`hasHour=false`),
  `AbstractDateWithSecond.java:36-39` (`hasSecond=true`), `Year.java:33-96` /
  `YearMonth.java:38-101` (all false except month), and **`LatestDate.java:34-98`
  — every accessor THROWS `UnsupportedOperationException`**.
- **Decisive:** the reference relational adapter has **no SQL translation for any
  of them**. `legend-engine/.../relational-duckdb/EssentialFunctions_manifest.json:214-233`
  lists `testHasDay`, `testHasHour`, `testHasMinute`, `testHasMonth`,
  `testHasSecond`, `testHasSubsecond`, `testHasSubsecondWithAtLeastPrecision` —
  each with `"No SQL translation exists for the PURE function 'hasX_Date_1__Boolean_1_'."`
  The only reference uses of `has*` are **SQL-generation-time** decisions about how
  to *spell a date literal* (`duckdbExtension.pure:148-153`,
  `extensionDefaults.pure:148-152`, `oracleExtension.pure:174-178`).

### a.6 Why testParseDateTypes is an oracle "frontier" row

`pct/src/test/resources/oracle/EssentialFunctions_manifest.duckdb.json:382-383`
(a pinned snapshot of the engine's `relational-duckdb` manifest) says the reference
DuckDB adapter fails it with
`"[unsupported-api] The function 'toTimestamp' … is not supported yet"`. So there
is **no reference row-parity oracle** for this test — but it *is* in the exclusion
set that `ChannelBEssentialTest.engineDuckDbExclusions()` reads
(`ChannelBEssentialTest.java:244-261`), which is why it currently counts as
ENGINE-FRONTIER and keeps `trueWireBug == 0` (**:237-238**).

---

## b) OUR SEAM

### b.1 Date kinds are modelled — **BOTH statically and dynamically**. Nothing is missing.

**Static channel (typed IR).** `Type.Primitive` carries all four kinds:
`Type.java:81` `DATE`, **:82** `STRICT_DATE`, **:83** `DATE_TIME`, **:84**
`LATEST_DATE` (+**:85** `STRICT_TIME`). The lattice is **not** re-encoded there
(`Type.java:67-71`: "the `extends` chain already declared in `Pure.java` … walked
via `ModelContext.isSubtype`"), and `Pure.java:201-204` declares it **identically
to the reference**:

```java
201 Date       extends Any
202 StrictDate extends Date
203 DateTime   extends Date
204 LatestDate extends Date
```

**Dynamic channel (wire value).** `values/PureDateLiteral.java` is a sealed
interface with **seven** record variants (**:75-82**): `Year`(**:265**),
`YearMonth`(**:271**), `StrictDate`(**:279**), `DateWithHour`(**:287**),
`DateWithMinute`(**:298**), `DateWithSecond`(**:309**), `DateWithSubsecond`(**:321**)
— a faithful mirror of the reference `PureDate` hierarchy, with the same
GMT-normalise-and-discard-offset rule (**:38-48**, `shift` at **:557-586**). It is
**THE** wire temporal carrier (**:192-197**): "sql/java.time temporals never escape
the fetch seam."

### b.2 Where the kind is decided — every site

| # | site | file:line | what it does |
|---|---|---|---|
| 1 | **Typer refinement** | `compiler/spec/Typer.java:1645-1664` (`refineParseDate`, called at `:1635`) | `parseDate(<String literal>)`: `-?\d{4,}-\d{2}-\d{2}[T ]\d.*` → `DATE_TIME`; `-?\d{4,}-\d{2}-\d{2}` → `STRICT_DATE`; else keeps abstract `DATE` |
| 2 | **source `%`-literal** | `Typer.java:177` | the only ordinary `TypedCDate` construction site (the other two are milestoning snap dates, `resolver/TemporalFrame.java:1492`, `:1556`) |
| 3 | **let binding** | `Typer.java:245-248` | the let-bound var inherits the RHS's exact `ExprType` — so `$dt` **is** `DATE_TIME`, `$date` **is** `STRICT_DATE` |
| 4 | **parseDate emission (J8a)** | `lowering/Scalars.java:2243-2279` | `Cast(in, PureSql.type(rt == STRICT_DATE ? STRICT_DATE : DATE_TIME))` — **:2268-2278** is verbatim `// Conform-by-emission (slice-4 J8a)` |
| 5 | **SQL type mapping** | `lowering/PureSql.java:92`, `:94`, `:108` | `BOOLEAN→BOOLEAN`; `STRICT_DATE→DATE`; `DATE_TIME/DATE/LATEST_DATE→TIMESTAMP` |
| 6 | **fetch decode** | `exec/Executor.java:671-687` | `java.sql.Date`/`LocalDate` → `PureDateLiteral.StrictDate`; `Timestamp`/`LocalDateTime` → `fromLocalDateTime` (`PureDateLiteral.java:219-230`: nano==0 → `DateWithSecond`, else `DateWithSubsecond`) |
| 7 | **runtime kind → Pure type** | `exec/PureAsserts.java:200-220` (`carrierTypeName`) | `Year`/`YearMonth`→`"Date"`; `StrictDate`→`"StrictDate"`; any other `PureDateLiteral`→`"DateTime"`; `OffsetDateTime`→`"DateTime"` |
| 8 | **`instanceOf` adjudication** | `exec/PureAsserts.java:182-198` | `"Date"` accepts `StrictDate\|DateTime\|Date` (**:192-193**) — the reference lattice, correctly |
| 9 | **channel-A bridge** | `pct/…/extension/ValueBridge.java:404-418` | `precision()` → `YEAR,MONTH→"Date"`, `DAY→"StrictDate"`, default→`"DateTime"` — exactly `DateFunctions.datePrimitiveType` |
| 10 | **precision predicates** | `lowering/Scalars.java:721-739` + `datePrecision` at `:3378-3411` | **the defect** — see b.4 |
| 11 | CSV/abstract-Date render | `lowering/Render.java:693-729` | slot-kind-driven date vs datetime spelling |

Kind is therefore **static in the IR AND dynamic on the wire**, and the two agree
by construction (J8a made the emission speak the stamp).

### b.3 Where `instanceOf` is evaluated

Two disjoint routes:

- **Verdict route (the one this test takes).** `assertInstanceOf` is
  platform-owned (`PlatformTypes.java:235-236`, **:245**), so its parsed Pure body
  is suppressed at the overload merge and it never β-inlines to
  `assert(cond, msg)`. `AssertVerdicts.java:262-277` reads the type argument via
  `typeRefName` (**:1084-1092**), executes the instance side in the DB, and
  adjudicates the **runtime carrier** host-side via `PureAsserts.assertInstanceOf`.
- **Lowering route (not taken here).** `Scalars.instanceOfFold`
  (`Scalars.java:2484-2508`, dispatched from `Lowerer.java:2920-2923`) folds only
  `ClassType`-vs-`ClassType` and `TabularDataSet`; a primitive `DATE_TIME` is
  neither, so it would **throw**
  `NotImplementedException("instanceOf undecidable statically: …")`. **This is
  load-bearing:** any change that routes `assertInstanceOf` through the parsed Pure
  body would turn this test into an ERROR, not a pass.

### b.4 The exact failing assertion, and why — the full chain

Channel B strips the adapter (`ChannelB.eliminateAdapter`, `ChannelB.java:271-316`;
`$f->eval(|expr) → expr` at **:396-409**), so the executed body is:

```
let dt   = parseDate('2014-02-27T00:00:00.000000');   // Typer → DATE_TIME
assertInstanceOf($dt, DateTime);
assert($dt->hasSecond());                              // ← FAILS HERE
let date = parseDate('2014-02-27');                    // Typer → STRICT_DATE
assertInstanceOf($date, StrictDate);
assertFalse($date->hasHour());
```

`Scalars.java:721-739` — the precision-predicate rule:

```java
721  for (var e : Map.of("hasMonth",MONTH,"hasDay",DAY,"hasHour",HOUR,
                         "hasMinute",MINUTE,"hasSecond",SECOND,"hasSubsecond",SUBSECOND).entrySet()) {
728    for (String f : Pure.nativeKeysAt(e.getKey())) {
729      RULES.put(f, (n, args) -> {
730        boolean has = datePrecision(n.args().get(0)).atLeast(e.getValue());
734        return n.args().get(0) instanceof TypedCDate
735                ? new SqlExpr.BoolLit(has)
736                : new SqlExpr.IntLit(has ? 1 : 0);
737      });
```

`$dt` is a `TypedVarRef`/`TypedNativeCall`, **never** a `TypedCDate` (only
`Typer.java:177` and the two `TemporalFrame` sites build those) → the `IntLit`
branch.

Then, step by step:

1. `datePrecision($dt)` → `Scalars.java:3402-3403`: `DATE_TIME → SUBSECOND`;
   `.atLeast(SECOND)` = **true**. Value correct.
2. Emission: `SqlExpr.IntLit(1)`. Its `TypeFact` is **unconditionally `T_BIGINT`**
   (`SqlExpr.java:426-429`, compact ctor overwrites).
3. `Lowerer.scalarRoot` (**:325-408**) labels the output
   `sqlTypeOf(spec.info().type())` = `BOOLEAN` (**:376**, **:406**).
4. `SqlTyping.reconcileLabels` (**:213-218**), called from `SqlSelect`'s canonical
   constructor: `BIGINT != BOOLEAN` and `subsumes(BOOLEAN, BIGINT)` is false
   (`SqlTyping.java:362-375` — only `TIMESTAMP←DATE` and same-scale Decimal
   widening) → **"untagged label lie: adopt the wire"**, `type = BIGINT`. *(This is
   why `SqlTypeCensus.mismatchCount()` stays 0 and the suite's pin at
   `ChannelBEssentialTest.java:227-230` does not catch it.)*
5. `Executor.fetch` (**:571-599**) → `rs.getObject(1)` → `Integer`/`Long`.
   `unwrap` (**:602-687**) has **no** Integer→Boolean arm; `SqlDialect.normalize`
   defaults to identity (`SqlDialect.java:27-30`) and **DuckDb has no override**
   (only `H2.java:481-496`).
6. `AssertVerdicts.decodeSide` (**:1030-1070**) yields `[Integer 1]`.
7. `AssertVerdicts.java:249-261`: `Boolean.TRUE.equals(Integer(1))` → **false**;
   `held = false == true` → false → **`fail("Assert failed")`** (**:260**) →
   `SQLException` → `ChannelB.runOneInner:239-243` records `FAIL … Assert failed`.

**Live corroboration in the current run**
(`pct/target/surefire-reports/…ChannelBEssentialTest.xml`, mtime 2026‑08‑27 17:37):

- **:366** `[chB] FAIL …parseDate::testParseDateTypes :: Assert failed`
- **:363-365** `testParseDate`, `testParseDateWithZ`, `testParseDateWithTimezone`
  all **PASS**
- **:230-235** `testHasMinute`, `testHasDay`, `testHasSubsecondWithAtLeastPrecision`,
  `testHasSubsecond`, `testHasSecond`, `testHasMonth` all **PASS** — **because every
  argument in those tests is a `%`-literal** (`hasSecond.pure:21-27`), taking the
  `BoolLit` branch.
- Channel A (`…Test_LegendLite_EssentialFunctions_PCT.txt`):
  `Tests run: 327, Failures: 0` — the row **passes** in Channel A (its `testcase` at
  XML **:2714-2720** carries no `<failure>`), because `ValueBridge.java:404-418`
  hands the real interpreter a precision-faithful `PureDate` and the reference
  answers `hasSecond()` from the value.

**Per-assertion enumeration — every candidate, and what each would require:**

| # | line | can it produce `Assert failed`? | current predicted outcome | if it *were* the failure, what would be required |
|---|---|---|---|---|
| 1 | `:43` `assertInstanceOf($dt, DateTime)` | **NO** — formatted message only (`PureAsserts.java:196-197`) | **PASS** (UNVERIFIED by run): `CAST('2014-02-27 00:00:00.000000' AS TIMESTAMP)` → nano 0 → `DateWithSecond` → `carrierTypeName`=`"DateTime"` (`PureAsserts.java:216`) | would mean the TIMESTAMP cast or `fromLocalDateTime` degraded the kind — a J8a-adjacent emission fix |
| 2 | `:44` `assert($dt->hasSecond())` | **YES** | **FAIL** — the diagnosed root cause | boolean-faithful emission for non-literal `has*` args |
| 3 | `:47` `assertInstanceOf($date, StrictDate)` | **NO** | **PASS** (UNVERIFIED): `CAST('2014-02-27' AS DATE)` (J8a) → `java.sql.Date` → `fromLocalDate` → `PureDateLiteral.StrictDate` → `"StrictDate"` (`PureAsserts.java:215`). *Also unreachable if #2 fails first.* | would mean J8a regressed — check the `Cast` target at `Scalars.java:2275-2278` |
| 4 | `:48` `assertFalse($date->hasHour())` | **YES** | **PASSES BY ACCIDENT** — `IntLit(0)`; `Boolean.TRUE.equals(Integer(0))`=false, name≠"assert" → `held=true`. *Unreachable if #2 fails.* | same fix as #2; note it will still pass after the fix (`BoolLit(false)`) |

**So: exactly one assertion (#2) is the live failure, and #4 is a latent false
pass.** Both #1 and #3 are structurally incapable of producing the observed message.

### b.5 Our own internal inconsistency — the strongest argument for the fix

`hasSubsecondWithAtLeastPrecision` (`Scalars.java:740-757`), the sibling registered
eight lines later, returns `SqlExpr.BoolLit` in **both** its branches (**:752**,
**:754**) — literal *and* non-literal. Its PCT row passes
(`ChannelBEssentialTest.xml:232`). **The `IntLit` arm is a one-off in an otherwise
boolean-faithful family.**

---

## c) MINIMUM DESIGN — decisions, not lines

**D1 — Classify the leg correctly before touching anything.** This is **not** a
date-kind leg. Date kinds are complete and correct (§b.1, §b.2). It is the A24/D92
**boolean-carrier** defect: `Scalars.java:734-736` emits an INTEGER under a
`Boolean[1]` stamp. Effort is the *defect* order of magnitude (one emission arm +
pin moves), not the *missing-feature* one.

**D2 — Fix at the EMISSION: make the non-literal `has*` arm boolean-faithful.**
Delete the `TypedCDate`-vs-not fork at `Scalars.java:734-736`; return
`SqlExpr.BoolLit(has)` unconditionally. Justification chain:

- **Conform by emission, never by weakening a checker.** `AssertVerdicts.java:259`
  (`Boolean.TRUE.equals(c)`) is *correct* — a Pure `Boolean[1]` must arrive as a
  Java `Boolean`. Relaxing it to accept `Number` would be exactly the banned move.
- **Invariant 4 (AGENTS.md:244-252) — no fallbacks, no defaulting.** The integer
  carrier *is* a silent defaulting of kind at the wire.
- **TENET_CHARTER C2.2 is satisfied, not violated:** the answer is computed by
  `datePrecision` from the **stamp** (`Scalars.java:3401-3410`), i.e. *"ask the
  plan, never the cell."* No value is inspected. Note `datePrecision` already
  **refuses loudly** for abstract `Date` (**:3408-3410**) — that is C2.2/Invariant-4
  compliance, and it must stay.
- **Invariant 2 (AGENTS.md:160-175) is *not* violated: it is RESTORED.** This arm
  currently branches on `instanceof TypedCDate` — HIR type dispatch to pick a MIR
  shape, precisely the banned pattern. Removing the fork **restores** Invariant 2.
- The `AUDIT_23_SPECIAL_CASING.md:173-175` justification ("the engine's integer
  surface for column arguments") is **factually ungrounded**: the reference has *no*
  SQL surface for `has*` at all (`relational-duckdb/EssentialFunctions_manifest.json:214-233`,
  six "No SQL translation exists" rows). Nothing in the reference is being conformed
  to. **That ledger entry must be retired with a dated note.**

**D3 — Move the two pins that assume the integer carrier, with dated
justification (AGENTS.md:10-15).** They are the *entire* cost of D2 beyond the one arm:

- `core/src/test/java/com/legend/AuditRound3Test.java:196-197` —
  `assertEquals(0L, ((Number) scalar("|date(2015,4,16,14)->hasMinute()")).longValue())`
  and the `1L`/`hasHour` sibling. Note the argument is a `date()` **constructor**,
  not a literal, so it takes the same non-`TypedCDate` branch; the arity rule at
  `Scalars.java:3385-3400` still supplies the right value.
- `core/src/test/java/com/legend/integration/ExtendCheckerTest.java:2022-2027` —
  `// DuckDB returns integers for has* functions (1 = true, 0 = false)` + five
  `((Number) …).intValue() != 0` asserts. The comment is itself false (it is our
  lowering, not DuckDB) and should be corrected in the same move.

No `.pure` corpus golden pins the 1/0 form (searched `corpus/`, `rcorpus/`,
`pct/src`, `core/src/test` — the only hits are those two Java tests plus
literal-argument tests at `ScalarFunctionIntegrationTest.java:396-420` and
`TypeInferenceIntegrationTest.java:1734-2483`, which take the `BoolLit` path and
are unaffected).

**D4 — Do NOT touch `instanceOf`, `assertInstanceOf`, `PureAsserts.carrierTypeName`,
or the date-kind model.** All four are correct against the reference (§a.2, §a.4 vs
§b.1, §b.2). In particular do not make `PureAsserts.assertInstanceOf` (**:188-195**)
permissive — its `"Date"` arm already encodes exactly the m3 lattice.

**D5 — Do NOT re-route `assertInstanceOf` through its parsed Pure body.**
`Scalars.instanceOfFold` (**:2484-2507**) only decides `ClassType`/`TabularDataSet`;
a primitive date kind would hit `NotImplementedException` and convert a FAIL into an
ERROR. The platform-native route (`PlatformTypes.java:245`) is load-bearing.

**D6 — Renderer stays untouched.** Under Invariant 3 (AGENTS.md:177-203) `BoolLit`
already has a render arm on every dialect; `DATE`/`TIMESTAMP` spellings live in
`TypeNames.java:59-60`. This change introduces **no new MIR variant and no new render
arm** — it *removes* a shape, so Invariant 3a's "new native = new MIR variant + new
render arm" does not bite.

**D7 — Expect the census pins to move in the right direction, and check them.**
With `BoolLit` the projected type becomes `T_BOOLEAN`, matching the label, so
`reconcileLabels` (`SqlTyping.java:213-218`) stops adopting BIGINT. That should shift
rows from the adopt/diverge buckets. `ChannelBEssentialTest.java:218-223` pins
`wireDivergeCount() <= 75` and `wireAdoptPendingCount() <= 103` — both shrink-only
ceilings, so improvement is safe; `mismatchCount() == 0` (**:227-230**) must stay 0.

**D8 — Score expectation.** `ChannelBEssentialTest.java:78` pins `pass >= 305`. A
clean D2 should give **306**, and moves `testParseDateTypes` out of the
ENGINE-FRONTIER bucket. It is currently **not** a Channel-A expected failure
(verified: `Test_LegendLite_EssentialFunctions_PCT.java` contains no `parseDate`
`one(...)` row), so it is a genuine WIRE-BUG row masked as frontier by the DuckDB
manifest.

**C1.6 litmus (TENET_CHARTER:36) applied to `instanceOf` over a date kind:** for
`parseDate` over a *literal in test source* — build-time model text — the kind **is**
answerable with no database attached, and our Typer already answers it
(`Typer.java:1645-1664`). For `parseDate` over a *query-time string* the kind is
genuinely runtime data; `refineParseDate` correctly declines there (**:1661** "partial
or exotic shapes keep the abstract Date") and `datePrecision` then refuses loudly
(`Scalars.java:3408-3410`). **That boundary is already drawn correctly and must not
be moved** — moving it would be the banned "kind chosen by inspecting a value's text
shape."

---

## d) TRAPS

1. **The bare `Assert failed` genuinely hides which line failed.** Two lines can
   produce it (`parseDate.pure:44`, `:48`), and `AssertVerdicts.java:249-261`
   **discards the message argument entirely** — it never reads `args.get(1)`, so
   *any* `assert(cond, msg)` failure reports the same string. The static chain in
   §b.4 pins it to **:44**, but the burn must **print the actual value** before
   editing (§e).

2. **`Lowerer.java` is EXACTLY at the 3500-line hard cap.** Verified: `wc -l` = 3500;
   the guardrail is `core/src/test/java/com/legend/CodeShapeGuardrailTest.java:35`
   (`FILE_LIMIT = 3500`, `METHOD_LIMIT = 250` at **:34**); flagged in
   `CHANNELB_BURNDOWN_HANDOFF.md:21` and `ADAPTER_NECESSITY_CENSUS.md:259`.
   **Nothing may grow it.** D2 touches only `Scalars.java` (3424 lines — 76 of
   headroom) and is net line-*negative*.

3. **The label-flip masks the bug from the census.** `SqlTyping.reconcileLabels:213-218`
   silently adopts BIGINT over the BOOLEAN contract, which is precisely why
   `SqlTypeCensus.mismatchCount()==0` (`ChannelBEssentialTest.java:227`) passes today.
   **Do not read a green mismatch pin as evidence the wire is honest.** Conversely,
   after D2 watch that `wireDivergeCount`/`wireAdoptPendingCount` (**:218-223**) move
   down, not sideways.

4. **The `AUDIT_23_SPECIAL_CASING.md:173-175` "deliberate divergence" is a stale,
   ungrounded claim.** It cites "the engine's integer surface" which does not exist
   (`relational-duckdb/EssentialFunctions_manifest.json:214-233`). Do not treat it as
   a reason not to fix. It must be retired in the same change.

5. **`ENGINEERING_LOG.md` is a month stale and is NOT an oracle for these landings.**
   mtime 2026‑08‑14; latest dated entry **:159**/**:166** is 2026‑08‑14; it contains
   **zero** occurrences of `J8a`, `B8`, `CFloat`, or `parseDate`; its "current state"
   (**:79-86**) is several campaigns behind. The authoritative 2026‑08‑27 landing docs
   are `ADAPTER_NECESSITY_CENSUS.md` (J8a at **:70**/**:112**, B8 at **:259**),
   `OPEN_REGISTER.md:70`, and `PROGRAM_MAP.md`. `PCT_EXPECTED_FAILURES.md` is a
   **month** stale (mtime 2026‑07‑30) and contains no parseDate row.

6. **The charter is stale in more places than this row.** It records Essential 297
   with Leg 1 "IN FLIGHT" (**:29**, **:49**, **:60**), but
   `ChannelBEssentialTest.java:78` now pins `pass >= 305` (legs 1 and 2 landed,
   **:71-77**). Re-derive from the test source, never from §1 of the charter.

7. **DuckDB / H2 timestamp-vs-date divergence.** `PureSql.java:94`/**:108** map
   `STRICT_DATE→DATE`, `DATE_TIME→TIMESTAMP`; `TypeNames.java:59-60` spells both.
   `Executor.fetch:585-598` re-fetches only `java.sql.Timestamp` as `LocalDateTime`
   (BC-era fidelity), and `unwrap:676-678` handles `java.sql.Date`/`LocalDate`
   separately — so a driver that hands a DATE column back as a `Timestamp` (or a
   TIMESTAMP as a `String`, see the **:684-685** VARCHAR arm) would silently change
   the observed kind. **H2 is the divergence risk here**, and `H2.normalize`
   (`H2.java:481-496`) has no temporal arm. Assertion #3's pass is DuckDB-specific
   until probed on H2. `SqlTyping.subsumes:365-368` explicitly tolerates
   `TIMESTAMP←DATE`, so a slot erasure there would not be flagged.

8. **Record-equality / inliner hazard.** `PureDateLiteral` variants are Java
   `record`s; `PureAsserts.equalScalar` and `carrierTypeName` dispatch on the *record
   class*. `fromLocalDateTime` (`PureDateLiteral.java:219-230`) picks `DateWithSecond`
   vs `DateWithSubsecond` **by whether nano==0** — so `'…T00:00:00.000000'` comes back
   as `DateWithSecond` where the reference produces `DateWithSubsecond("000000")`. Both
   are `DateTime`, so assertion #1 is unaffected, **but `hasSubsecond()` over that
   value would disagree with the reference.** Not asserted in this test; do not let a
   "cleanup" widen the change into that.

9. **`Type.Primitive` has no `default ->` safety net by design.** `datePrecision`
   (`Scalars.java:3401-3410`) *throws* for abstract `DATE`. Do not "fix" that throw
   into a guess to make some adjacent row green — it is Invariant 4 working.

10. **Do not delete the `TypedCDate` fork by making *everything* `IntLit`.** The
    literal branch's `BoolLit` is what makes the six `testHas*` rows pass today
    (`ChannelBEssentialTest.xml:230-235`). **The unification must go toward `BoolLit`.**

---

## e) CONFIDENCE + LIVE PROBES

**Confidence that the charter's mechanism is REFUTED: very high (~97%).** It rests
on a closed argument, not inference: only two code sites emit `Assert failed`, both
are the `assert`/`assertFalse` arm (`AssertVerdicts.java:260`, **:460**);
`assertInstanceOf` is platform-owned so its body cannot inline into `assert`
(`PlatformTypes.java:245`, `Pure.java:2052-2058`); and its own failure text is
different (`PureAsserts.java:196-197`). Residual risk is only that some unenumerated
path produces the identical string.

**Confidence in the true mechanism (`IntLit` at `Scalars.java:734-736` → `Integer`
wire → `Boolean.TRUE.equals` false): high (~90%), but the chain is STATIC — it has
not been executed.** Supporting evidence: the A24 audit's own **live repro** of the
same emission (`A24-scalar-functions.md:167-170`: `SQL: 1 AS x` → `Integer(1)`),
CONFIRMED at `V2-falsifier.md:229` / `MASTER.md:100`; and the literal/non-literal
contrast in the current run (six `testHas*` PASS with literal args, this row FAIL
with a non-literal arg).

**Confidence that assertions #1 and #3 pass: moderate (~80%) — UNVERIFIED, and they
are unreachable behind #2.** They depend on DuckDB's driver returning
`java.sql.Date` for a `CAST(… AS DATE)` and `Timestamp`/`LocalDateTime` for
`CAST(… AS TIMESTAMP)`, traced through `Executor.fetch`/`unwrap` but not observed.

### THE LIVE PROBE IS REQUIRED. Run it BEFORE editing anything.

Because the symptom is a bare `Assert failed`, the diagnosis cannot be closed
without printing the actual values. Use the scoped-run idiom already built in
(`ChannelB.java:156-161`): **`-Dchb.only=parseDate`**.

**Print, in this order — the whole point is the Java runtime class, not the value:**

1. **The emitted SQL and the decoded value for each of the four assertion
   arguments**, one line each:
   - `$dt->hasSecond()` → expect SQL `SELECT 1 AS "value"` and
     `java.lang.Integer`/`Long` **1**. *This single line closes the diagnosis.* If it
     prints `SELECT true` / `java.lang.Boolean true`, the whole §b.4 chain is wrong
     and everything below must be re-derived.
   - `$date->hasHour()` → expect `SELECT 0` and `Integer 0` (the latent false pass).
   - `$dt` → expect `CAST('2014-02-27 00:00:00.000000' AS TIMESTAMP)`, decoded class
     **`PureDateLiteral.DateWithSecond`**, `carrierTypeName` → `"DateTime"`.
   - `$date` → expect `CAST('2014-02-27' AS DATE)` (J8a), decoded class
     **`PureDateLiteral.StrictDate`**, `carrierTypeName` → `"StrictDate"`.
2. **The `OutputCol` type on each plan** — confirm `$dt->hasSecond()`'s label reads
   `BIGINT`, not `BOOLEAN` (the `reconcileLabels` adoption at `SqlTyping.java:213-218`).
   This is the proof that the census pin cannot see the bug.
3. **The `getClass().getName()` of every decoded value**, printed verbatim. `1` and
   `true` look identical in a log; the class name does not.
4. **Which statement index raised**, and the raw `SQLException.getMessage()` before
   `ChannelB.first()` flattens it (`ChannelB.java:254-256`).
5. To separate #1/#3 from #2, run once with statement **:44** temporarily
   unreachable (e.g. against a scratch copy of the two `assertInstanceOf` lines alone)
   — confirming assertions #1 and #3 pass **on their own** turns their "predicted
   PASS" into an observed one.

**After the fix, re-probe:** all four values must print as `java.lang.Boolean`,
`pass` must read **306**, `mismatchCount()` must stay **0**, and
`wireDivergeCount`/`wireAdoptPendingCount` must not grow.

---

## OPEN QUESTIONS

1. **Does `$dt->hasSecond()` actually deliver a `java.lang.Integer` on this DuckDB
   build?** The chain is fully traced statically (`Scalars.java:736` →
   `SqlExpr.java:428` → `SqlTyping.java:216` → `Executor.java:574` →
   `AssertVerdicts.java:259`) but **never executed**. Probe 1 settles it. **This is
   the one question the burn must answer first.**
2. **Do assertions #1 and #3 pass today?** UNVERIFIED — unreachable behind #2. If
   either also fails, the leg is larger than one emission arm and §b.4's table must
   be re-walked.
3. **Is the label flip (`BOOLEAN → BIGINT`, `SqlTyping.java:213-218`) intended to
   cover *emission defects*, or only genuine driver-kind facts?** It is currently
   absorbing a compiler bug and hiding it from the `mismatchCount()==0` tripwire. Out
   of scope for Leg 7, but a real hole in the census's guarantee that deserves its own
   register row.
4. **What was the actual provenance of `AUDIT_23_SPECIAL_CASING.md:173-175`'s
   "engine's integer surface for column arguments"?** No reference witness was found —
   the reference refuses all six `has*` in SQL. The original evidence could not be
   located; it may have been an H2-era golden that no longer exists. Whoever retires
   the ledger entry should say so rather than silently deleting it.
5. **Does H2 decode `CAST('2014-02-27' AS DATE)` to `java.sql.Date` (→ `StrictDate`)
   or to a `Timestamp`/`String` (→ `DateTime`)?** `H2.normalize` (`H2.java:481-496`)
   has no temporal arm, and `Executor.unwrap:684-685` has a VARCHAR-under-temporal-label
   path. Assertion #3 is DuckDB-verified only. Not blocking (Channel B runs DuckDB,
   `ChannelB.java:230`), but it is the portability risk in J8a.
6. **Should `fromLocalDateTime` (`PureDateLiteral.java:219-230`) produce
   `DateWithSubsecond("000000")` rather than `DateWithSecond` for a `.000000` input, to
   match the reference?** The reference returns `DateWithSubsecond`
   (`DateFormat.java:589`). Both satisfy `instanceOf(DateTime)`, so this test does not
   discriminate — but `hasSubsecond()` would. The class doc at
   `PureDateLiteral.java:205-218` says this was **adjudicated deliberately**
   (2026-08-22, canonical-minimal). Flagged only so the burn does not "fix" it as
   collateral.
7. **`LatestDate`**: our `Type.Primitive.LATEST_DATE` exists (`Type.java:84`) and maps
   to SQL `TIMESTAMP` (`PureSql.java:108`), but `PureAsserts.carrierTypeName`
   (**:200-220**) has **no `LatestDate` arm** and `PureDateLiteral` has **no
   `LatestDate` variant** (**:75-82**). In the reference every `LatestDate` accessor
   **throws** (`LatestDate.java:34-98`). Not exercised by this test and `parseDate` can
   never produce one (`DateFormat.java:455`) — but it is a real gap in the kind model,
   and how `%latest` flows through `protocol/spec/CLatestDate.java` /
   `compiler/spec/typed/TypedCLatestDate.java` was not chased. **UNVERIFIED.**
8. **Is `testParseDateTypes` the *only* row failing on this mechanism?** The six
   `testHas*` PCT rows pass (literal args) and no other `has*`-under-`assert`-with-
   non-literal shape was found in the Essential suite — but Standard/Relation/
   Unclassified/Grammar were not swept for the same shape. **A D2 fix may bank more
   than one row.**
