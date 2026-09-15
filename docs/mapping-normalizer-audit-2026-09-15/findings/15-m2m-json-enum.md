# 15 — M2M, JSON-source, embedded, XStore, clean-sheet

The non-relational source kinds — where less attention has gone, and therefore where rot accumulates.

**Scope read end to end:** `MappingNormalizer.java` (all named sections), `M2mRouteGuards.java` (159),
`ModelJoinNesting.java` (202), `resolver/JsonSourceFrame.java` (281), `XStorePureEnds.java` (303), plus
the resolver / lowering / SQL-dialect tail of the JSON path. Engine reference:
`/Users/neemsandv/legend/legend-engine` and `/Users/neemsandv/legend/legend-pure`.

---

## 1. JSON-source path (rule T14 — no engine receipt, and the divergences are real)

### H1 — HIGH — TWO independent owners of "bind a JSON-sourced class's properties", disagreeing on every edge

- **Owner A** (named-runtime `JsonModelConnection`): `MappingClosures.java:90-120` cross-bakes a
  synthetic `ClassMapping.Relational(sourceUrl=…)`, which `MappingNormalizer.java:1619-1651`
  (`synthJsonSourceMapping`) turns into
  `sourceUrl(url) -> map(row | ^C(p = to(get($row.data,'p'), @T)))`.
- **Owner B** (inline `from(){ JsonModelConnection… }`): `JsonSourceFrame.java:140-236`
  (`sourceUrlFrame`) builds a `TypedTds` VALUES relation directly.

Both are live: A fires when a named runtime binds the class (`MappingClosures.java:97`), B when
`jsonSources` arrives through a `from()` scope (`ClassSources.java:691-695`, `:938-943`).

| | Owner A (`synthJsonSourceMapping`) | Owner B (`JsonSourceFrame`) |
|---|---|---|
| class-typed properties | **included**, emits `to(get(…), @NestedClass)` — dies at `PureSql.java:143` *"no SQL type for Pure class … at the lowering boundary"* | **skipped silently** (`:203-206`) |
| `[1]` conformance | none | `trustOne` wrap (`:216-220`) |
| zero bindable properties | no check | loud throw (`:228-231`) |
| row-identity column | schema is `(data)` only (`SourceUrlChecker.java:33-35`) | `(data, u_frame_ord__)` (`:190-192`) |
| payload shapes | DuckDb-only `SELECT unnest(CAST(content AS JSON[]))` (`DuckDb.java:202`) — **array form only**; a single object or the engine's `{..}{..}` stream form fails | `objectTexts` (`:77-138`) handles array, single object, and concatenated stream |
| url schemes | `data:` + `file:` (`DuckDb.java:196-218`); every other dialect throws `DialectCapability` (`AnsiSqlRenderer.java:198-200`) | `data:application/json,` only, loud otherwise (`:142-147`) |

### H2 — HIGH — the engine's JSON coercion is Jackson node-type-strict with a defect protocol; ours is SQL `CAST`

Engine side: `graphFetchJson.pure` (the generator), `jacksonSupport.pure:45-84` (value extraction) and
`:259-290` (accepted node types), `BasicDefect.java:113-131` (severity). Our side: `get` → `to`/`TypedCast`
→ `Cast(VARIANT_GET(x,'$'), carrier)`, carrier from `PureSql.java:118-190`.

| case | engine | ours |
|---|---|---|
| missing key on `[1]` | **Critical** `ClassStructure` defect, object → `null`; in non-checked M2M the generated iterator **throws** `IllegalStateException` | **silent NULL.** `JsonSourceFrame.java:216-218` states it outright: *"toOne erases value-wise in SQL — an absent key stays a NULL cell"* |
| `"123"` → `Integer` | **rejected**: *"Unexpected node type:STRING for PURE Integer"* (`jacksonSupport.pure:259-290`) | `CAST('123' AS BIGINT)` = 123, **accepted** |
| JSON number → `String` | **rejected** | `CAST(n AS VARCHAR)`, **accepted** |
| `1.1` → `Integer` | **silently truncated** via `longValue()`; engine fixture `allTypesBadData.json:43` produces zero defects | DuckDB `'1.1'::BIGINT` → **runtime conversion error** |
| `"true"` → `Boolean` | **rejected** (only `JsonNodeType.BOOLEAN`; the XML path *does* use `Boolean.valueOf`, the JSON path deliberately does not) | `CAST('true' AS BOOLEAN)` = true, **accepted** |
| dates | `PureDate.parsePureDate` — Pure's own grammar; `"12-12-12"` parses as **year 12** with no defect; StrictDate/DateTime not distinguished | SQL `CAST(text AS DATE/TIMESTAMP)` — the DB's parser: **different accepted grammar *and* different value** |
| **enum property** | JSON string matched against the enum value name (optional `pkg::Enum.` prefix stripped); no match = Error defect, or an **uncaught** `IllegalArgumentException` on the M2M path | `PureSql.java:147`: `case Type.EnumType e -> SqlType.Scalar.VARCHAR` — **the raw string passes through with zero validation against the declared enumeration. Silently wrong value.** |
| nested object → class property | recursively built via generated `read_<Class>`, defects propagated with a relative path, `@type` subtype dispatch | owner B: property absent; owner A: lowering crash. **No nesting, no `@type`.** |
| array → `[*]` | per-element bind, per-element defects; a scalar wrapped as a 1-element list | **neither owner fans out** — the array's JSON text becomes one scalar value |
| array → `[1]` | **Critical** multiplicity defect | `toOne` erases; the array text becomes the value |
| extra keys | ignored | ignored ✅ (the one match) |
| url templating | **FreeMarker** (`StoreStreamReadingExecutionNodeContext.java:65-69`), then `UrlFactory` — `data:` incl. `;base64`, `executor:`, `http:`, `file:` | our own regex `\$\{(\w+)\}` (`JsonSourceFrame.java:53-68`); no base64, no `executor:`, no FreeMarker expressions |

### M3 — MED — `substituteUrlParams` contradicts its own comment

`JsonSourceFrame.java:50-53` claims a non-literal `let` staying verbatim is a *"loud divergence, never
crash"*. **It is neither:** the unsubstituted `${var}` text flows into the payload and is parsed as
data. A silent wrong answer documented as a loud one.

### M4 — MED — 60+ JSON integration tests, zero pin coercion

`JsonM2MChainIntegrationTest.java` (1,183 lines, ~55 `@DisplayName`s) plus `JsonM2MIntegrationTest.java`
(178) cover 1–4 hop chains, mapping filters, user filters, sorts, graphFetch, multiple sources, and
NDJSON/array/unstructured file shapes — **all `String`/`Integer` happy paths.** No test touches a
missing key, a null, a date, an enum, a nested object, an array, or a type mismatch. **T14's "ours by
design" has no test floor under it either.**

---

## 2. M2M

### H5 — HIGH — a self-referential M2M class-typed property is rejected as a "cycle"

`MappingNormalizer.java:1245-1249`. `synthM2M` seeds `cycleStack` with `pcm.className()` (`:1153`) and
is called once with a fresh set (`:795`). `m2mPropertyValue` **never recurses into `synthM2M`** — it
emits a deferred `NewInstanceCast` (`:1252-1253`) and pops in `finally`. **The only way the guard can
trip is `innerFqn == pcm.className()`.**

So `Class P { manager: P[0..1]; }` mapped `*P: Pure { ~src S manager: $src.m }` is rejected with a
misleading message. Because it is a `ModelException` it lands in `ledger.strictErrors` (`:369-371`) and
**fails a strict build outright.** No test exercises it: `grep -rn "Cycle materializing" core/src/test/`
→ nothing. *(Independently proven by probe — see `01-mapping-normalizer-core.md`.)*

### H6 — HIGH — `materializeEmbedded`'s cycle guard is dead code; a cyclic embedded model stack-overflows

**All three call sites pass a fresh `new HashSet<>()`:** `:2125` (Embedded), `:2210`
(OtherwiseEmbedded), `:2246` (InlineEmbedded). The `cycleStack` parameter is **never threaded across the
recursion**, so `if (!cycleStack.add(innerFqn))` at `:2172` can never be false. An
`Inline[a]`→`Inline[b]`→`Inline[a]` model recurses to `StackOverflowError` — and `withElement`
(`:236-255`) deliberately lets raw `Error`/`ISE` escape unwalled, so it **kills the build** rather than
walling the element.

> **Adjudicated.** `01-mapping-normalizer-core.md` originally judged this guard sound. The call sites
> settled it in favour of this finding: the descent *is* recursive, but the fresh-set-per-call defeats
> the guard.

### M7 — MED — `M2mRouteGuards` guards in the right place but its central claim is unpinned and cites a symbol that does not exist

`M2mRouteGuards.java:103` justifies honouring a route by *"the graph consumer dispatches by it
(wholeSrcChild)"*. **`wholeSrcChild` appears only in two comments** (`M2mRouteGuards.java:103`,
`ClassSources.java:1131`) — **there is no such identifier anywhere in the codebase.** The mechanism does
exist (`ClassSources.java:1002-1007`, `:1058-1061`, `:1150`), but nothing ties the M2M route to it.

### M8 — MED — `m2mSetRoute_nonRootSet_poisons` pins the message, not the substance

`MappingNormalizerTest.java:4960-4979` asserts `reason.contains("set-routed") && reason.contains("u2")`
— i.e. that *our own error string* mentions the set id. Its companion
`m2mSetRoute_soleOrRootSet_isBenign` (`:4981-5000`) asserts only `poisons.isEmpty()`. **Neither asserts
that the emitted `NewInstanceCast` carries `targetSetId`, which is the actual claim the guard exists to
protect.** A refactor that dropped `pb.targetSetId()` from `MappingNormalizer.java:1252` would pass both
tests green.

### Guard behaviour when it trips: always LOUD, never silent

`requireBenignRoute` (`M2mRouteGuards.java:64-115`), `m2mBindingKey` (`:29-46`) and `localField`
(`:142-166`) all throw `ModelException`. They run from `MappingValidation.java:85-91`, whose result
feeds `pp.invalid()` → per-class poison (`MappingNormalizer.java:349-352`). Loud at use, isolated per
class. **That part is good.** *(Subject to BUG-2 in `14-guards-fallbacks-census.md`: a null `tgt`
disables the route guard entirely.)*

### M9 — MED — `AllVersions` is string-suffix-typed identity, implemented twice

`M2mRouteGuards.java:36-42` and `MappingNormalizer.java:1231-1234` independently strip the literal
suffix `"AllVersions"`. **Neither checks that the base property is actually on a milestoned/temporal
class**, so a typo `nameAllVersions` on a class declaring `name` silently binds to `name`.

### M10 — MED — M2M is missing two capabilities the relational path already has, both as hard walls

Enum transformers (`:1204-1211`, *"roadmap feature (source-value decode on the M2M read)"*) and
explosion (`:1195-1200`). **`translateEnumeratedSource` (`:2447-2542`) already implements the decode for
the relational side** — the M2M wall is a wiring gap, not a missing algorithm.

### Chained / included / union-source M2M: correct, no finding

`ledger.isMapped` is seeded from `MappingLedger.mappedInClosure` (`:181-182`), so an M2M whose target
property class is mapped in an *included* mapping resolves. A chained M2M defers through
`NewInstanceCast` and the resolver's `binds()` / `selfSourced` dispatch (`ClassSources.java:925-955`).
`pcm.filter()` composes correctly as `filter(getAll(Src), {src | …})` (`:1169-1174`).

---

## 3. Embedded / inline-embedded (rule T15 — the receipt exists; it was found)

**T15's engine line is `helperFunctions.pure:432`:**

```pure
| let cm = $_this.parent->_classMappingByIdRecursive($_this.inlineSetImplementationId);
  let result = $cm->cast(@InstanceSetImplementation)->toOne()->allPropertyMappings();
  $result->map(r | ^$r(owner = $_this.owner, sourceSetImplementationId = $_this.sourceSetImplementationId));
```

with `_classMappingByIdRecursive` at `functions_Mapping.pure:66-72` walking
`$_this.includes->map(i | $i.included)` transitively with `removeDuplicates()`. **So the rule IS
engine-correct, and the self-audit should cite that line instead of a corpus test.** But ours diverges
four ways:

### H11 — HIGH — No ambiguity wall. The engine has an explicit one

`mappingExtension.pure:266-272`:

```pure
let cm = $m->classMappingById($i.inlineSetImplementationId);
assertEquals(1, $cm->size(), | 'Found too many or not enough matches ['+$cm.id->makeString(',')+'] for inline implementation Set Id [' + $i.inlineSetImplementationId+']');
```

Ours (`MappingNormalizer.java:2229-2240`) takes the **first match and breaks**:

```java
outer:
for (LegacyMappingDefinition m : closure) {
    for (ClassMapping cm : m.classMappings()) {
        if (cm instanceof ClassMapping.Relational rcm
                && Objects.equals(ResolvedMapping.idOf(rcm), ie.setId())) {
            referenced = rcm;
            break outer;
```

**Two different sets sharing an id across two included mappings → silently the first in closure order.
Order-dependent wrong rows.**

### H12 — HIGH — No subtype check on the referenced set's class

`materializeInlineEmbedded` passes `referenced.className()` straight through as `innerOverride`
(`:2246-2248`), and `materializeEmbedded:2163` takes it verbatim. **The engine rejects the mismatch:**
`RelationalInstanceSetImplementationValidator.java:130-141` — *"The inlineSetImplementationId '…' is
implementing the class '…' which is not a subType of '…' (return type of the mapped property)"*.
**Ours materializes the wrong class silently.**

### M13 — MED — We only scan `ClassMapping.Relational`; the engine flattens embedded sets into `classMappings`

`RelationalCompilerExtension.java:367,520`:
`parentMapping._classMappingsAddAll(embeddedRelationalPropertyMappings);`. So
`Inline[myBondMapping_issuer]` (an embedded set's auto-id, pattern `parentId + "_" + propertyName`,
`HelperRelationalBuilder.java:1304`) **resolves in the engine** — the live passing test
`testInlineEmbeddedMappingWithAssociationFromRootMapping` (`testInlineEmbeddedMapping.pure:119-124`)
exercises exactly this. **In ours it reports *"references unknown setId"*.**

### L14 — LOW — The engine's precedence rule is missing

`helperFunctions.pure:428-436` tries `$_this.owner->propertyMappingsByPropertyName(...)` **first** and
only falls back to the inline splice when that is empty. **Ours always splices.**

### M15 — MED — T15's cited receipt is a *parser* corpus entry, not an execution one

`testInlineEmbeddedTargetIds.pure` appears in `parser-equivalence/src/test/resources/corpus-manifest.tsv:2533`
tagged `C3/C10 engine` — the round-trip corpus (`Corpus.java:207`). **It proves the file parses, not
that our normalizer resolves the id the way the engine does.** Our only normalizer-level inline tests
are `MappingNormalizerTest.java:1234-1290`: one happy path, one unknown-id. **No cross-include test, no
ambiguity test, no subtype test.** (Worth knowing: the engine's own end-to-end projection test over that
mapping is `<<test.ToFix>>`, i.e. disabled upstream.)

### Are `materializeEmbedded` and `materializeInlineEmbedded` two owners of one rule? **No — this one is fine.**

`materializeInlineEmbedded` (`:2219-2249`, 30 lines) is a thin resolve-then-delegate wrapper;
`materializeEmbedded` (`:2145-2195`, 50 lines) is the single owner of the materialization;
`materializeOtherwiseEmbedded` (`:2205-2215`) delegates to it as well. **Good structure — the
hypothesis does not hold.**

---

## 4. XStore — we reject shapes already sitting in our own corpus

### H16 — HIGH — The engine's XStore model is direction-scoped, not relation-scoped

The association-level container is *empty* — `mapping.pure:174-175`:

```
Class meta::pure::mapping::xStore::XStoreAssociationImplementation extends AssociationImplementation
{ }
```

All semantics live per-end — `mapping.pure:178-180`:

```
Class meta::pure::mapping::xStore::XStorePropertyMapping extends PropertyMapping
    crossExpression : LambdaFunction<{Nil[1],Nil[1]->Boolean[1]}>[1];
```

**There is no association-level join condition to share.** (Contrast `ModelJoinAssociationMapping`,
which *does* emit a single `joinCondition` — `HelperMappingGrammarComposer.java:137-140`. **The engine
models the two cases differently on purpose; we collapsed them into one.**)

Each end compiles in isolation with its own `$this`/`$that` bound to its own source/target sets —
`PropertyMappingBuilder.java:151-281`, returning at `:276-280`. Every executor and planner reads **only
the navigated end's lambda**: `XStore.pure:134,139`; `graphFetchExecutionPlan.pure:277`;
`relationalGraphFetch.pure:558,880`; `graphFetchInMemory.pure:510,548`; `cluster.pure:128`;
`routing.pure:154`. The complete list of engine XStore validations (8 checks:
`PropertyMappingBuilder.java:215-274`, `graphFetch_routing.pure:313`,
`relationalGraphFetch.pure:561-584`, `graphFetchInMemory.pure:468-476`) contains **nothing comparing end
A to end B** — and nothing requiring end B to exist at all.

Ours emits only the first line and discards the second after an equality check —
`MappingNormalizer.java:1032-1042`.

**The fact that makes this urgent: all four asymmetric engine fixtures are in our own
`corpus-manifest.tsv` — they parse and round-trip, but are never normalized.**

- `core_relational/relational/modelJoins/testModelJoinsToRelationalJoins.pure:399-400` — the decisive one:
  ```
  client[trade, legal_entity]: ($this.value >= $that.value) && ($this.value > $that.value) && ($this.value <= $that.value) && ($this.value < $that.value),
  trades[legal_entity, trade]: ($that.value >= $this.value) && ($that.value > $this.value) && ($that.value <= $this.value) && ($that.value < $this.value)
  ```
  `canonicalizeEqualOperands` (`:732-757`) swaps operands **only** for `equal`/`==` and sorts operands
  only for `and`/`or`. **Four ordering comparisons flipped per side canonicalize differently, so the
  wall fires.**
- `core_relational/relational/tests/mapping/relation/relationMappingSetup.pure:638-639` — genuinely
  asymmetric bodies, not inverses:
  ```
  biztEmployees[biztFirm, biztPerson]: ($this.id == $that.firmId) && ($this.id < 2),
  biztFirm[biztPerson, biztFirm]:      ($this.firmId == $that.id) && ($that.id < 2)
  ```
- `core_relational/relational/tests/mft/xStore/testMappingCrossStore.pure:239-242` — **four** property
  mappings for one association across two set pairs (`[person1,firm1]`, `[person2,firm2]`). We read set
  ids from line 0 only and then require all four conditions to agree — **we cannot represent this shape
  at all.**
- `core_relational_snowflake/.../executionPlanTestSnowflake.pure:493-499` — a **one-ended** XStore (only
  `children`, no reverse). The engine accepts it; our loop passes trivially and nothing pins the behaviour.

### H17 — HIGH — The rule is implemented twice, verbatim

`MappingNormalizer.java:1032-1042` and `XStorePureEnds.java:221-231` are near-identical: same
`conds.get(0)`, same `canonicalizeEqualOperands` loop, same message string. The surrounding end-name
validation (`:1006-1013` / `:184-192`) and the self-association orientation block (`:1017-1024` /
`:196-202`) are **also** duplicated. **Neither copy has a test:**
`grep -rn "direction-specific" core/src/test/` → nothing.

### M18 — MED — `canonicalizeEqualOperands` uses `toString()` for structural ordering and covers only `==`/`and`/`or`

`MappingNormalizer.java:748-753`. String-typed structural identity: any record-field reordering silently
changes the canonical order, **and ordering comparisons are never normalized** (the direct cause of the
`testModelJoinsToRelationalJoins` rejection above).

### M19 — HIGH (upgraded) — per-line set ids are the engine's actual model, not decoration

`MappingNormalizer.java:964-973` reads them from `propertyMappings2().get(0)` only. The engine resolves
`[source, target]` **per property mapping** (`PropertyMappingBuilder.java:214-223`), *selects among*
multiple property mappings for one property by set id (`graphFetch_routing.pure:257`,
`cluster.pure:128,158`), and uses the pair as **cross-association cache identity**
(`GraphFetchCrossAssociationKeys.java:44-64`). **Reading line 0 only is a structural mismatch, not a shortcut.**

---

## 5. Clean-sheet path — the bypass is NOT complete

`normalizeMapping` (`:257-455`) returns the **9-arg** `MappingDefinition`; `cleanSheetToCanonical`
(`:475-533`) returns the **6-arg** convenience constructor (`MappingDefinition.java:46-56`), which
hard-codes `Map.of(), Map.of(), NormalizationFacts.NONE`. Facts present in one door and absent in the other:

### H20 — HIGH — Included enumeration mappings are dropped

`normalizeMapping:451` passes `md.enumerationMappingsWithIncludes()` (flattened transitively);
`cleanSheetToCanonical:533` passes `md.enumerationMappings()` (**own only**).
`CleanSheetMappingDefinition` **does** carry `includes` (`:37`). The javadoc at `:445-448` states the
flattening exists precisely so *"no post-compile consumer re-derives it from the legacy surface"* — **so
nothing recovers them.**

### M21 — MED — Include store substitutions are dropped

`resolveAllStores` at `:160-163` filters `parsed.elements()` to `LegacyMappingDefinition.class::isInstance`
— clean-sheet mappings never enter store-substitution resolution, and `cleanSheetToCanonical` stamps
`resolvedStores = Map.of()`.

### M22 — MED — The whole fact channel is empty

`routedTargetSets`, `poisons`, `mixedUnions`, `unionKeyThreads`, `unionMembers`, `nullableCensus`,
`routedTargetClasses` — all `NONE` (`MappingDefinition.java:72-92`). Some is by design (Door 1/3 only
has `Kind { RELATIONAL, PURE }`, `CleanSheetMappingDefinition.java:58` — no Union/Operation), but **the
poison channel absence means a clean-sheet mapping has no per-class fault isolation**:
`cleanSheetToCanonical` throws all-or-nothing into the wall sink (`:216-221`), **sinking the whole
mapping where the legacy door would withhold one class.**

### M23 — MED — `DeclaredKeys.NONE` for every clean-sheet relational binding

`:505`, justified at `:495-497` as *"grow by witness"*. The legacy door threads
`declaredKeys.getOrDefault(SetKeyFacts.setKey(rSrc), …)` (`:325-326`, `:399-400`). **Any consumer of key
facts sees a clean-sheet mapping as key-less.**

**Good here:** `inlineRootSource` (`:574-613`) is genuinely solid — cycle-guarded via a **threaded**
`seen` set (unlike H6), follows user-function chains, and throws rather than inventing an "unknown
source" variant.

---

## 6. General

### M24 — MED — `ModelJoinNesting.compose` picks the LAST matching nested ModelJoin, silently, and only from the own mapping

`ModelJoinNesting.java:85-93`:

```java
for (AssociationMapping am2 : md.associationMappings()) {
    if (am2 instanceof AssociationMapping.ModelJoin cand && ...) {
        nmj = cand;          // no break — last wins
    }
}
```

`ResolvedMapping.associationMappings()` returns the **own** mapping only (`:74`); `closure()` is a
separate accessor (`:102`). A nested hop whose ModelJoin lives in an included mapping throws *"has no
ModelJoin in this mapping"* (`:95-101`) — **inconsistent with `materializeInlineEmbedded` and
`xstoreEndOf`, both of which do walk `md.closure()`.**

### M25 — MED — `xstoreEndOf` always receives `setId == null` from ModelJoin and picks the first set by kind precedence

`MappingNormalizer.java:1084-1085` calls `xstoreEndOf(md, classA, null, model)`.
`XStorePureEnds.java:71-145` scans the whole closure in three passes — RelationFunction, then
Relational, then Pure — returning the first hit. `rcm.root()` is read (`:141`) but **never used to
*prefer* the root set.** A class with multiple sets binds to whichever appears first in closure order.

### L26 — LOW — Dead code: `ModelJoinNesting.java:76-78`

`orElseThrow` cannot return null; the `if (nad == null) continue;` and its comment describe a path that
no longer exists.

### L27 — LOW — `Set<String[]> hops = new LinkedHashSet<>()` never deduplicates

`ModelJoinNesting.java:58` — arrays use identity `equals`. Harmless only because
`nestedCols.containsKey(prop)` at `:67-70` catches the duplicate downstream; **the declared intent is a
no-op.**

### L28 — LOW — `translateEnumeratedSource` duplicates `sourceRead` once per (enum value × source value)

`:2513-2519`. A 10-value enum with 2 source values each inlines an expression-bound or join-chain column
read **20 times**. Also: *"no match yields `[]`"* (`:2444`) — an unmapped source value silently becomes
NULL in a `[1]` slot, with no engine receipt for that choice; and the typo wall at `:2503-2510` is
skipped entirely when `knownValues` is null (`MissProbe::miss` at `:2502`).

### L29 — LOW — over-long methods

`normalizeMapping` 198 (`:257`), `synthTableBackedParts` 184 (`:1751`), `synthesizeXStoreMapping` 106
(`:957`), `translateEnumeratedSource` 96 (`:2447`), `translatePmToField` 86 (`:2050`) — in a 2,910-line file.

---

## Source kind | engine receipt? | divergence risk

| source kind | engine receipt? | divergence risk |
|---|---|---|
| **JSON-source (`sourceUrl != null`)** | **NONE.** T14 admits it. Engine does in-memory Jackson deserialization with a typed defect protocol; we do DB-side `CAST` over a Variant column. Nothing pins either side | **VERY HIGH** — 11 of 12 coercion cases diverge (H2). Enum values unvalidated, missing keys silent, arrays collapsed to text, nested objects absent. Plus two disagreeing in-house owners (H1) |
| **M2M (`ClassMapping.Pure`)** | Partial. Routing guards cite audit 21a and `PropertyMappingBuilder`, but the load-bearing dispatch claim names a **non-existent symbol** (M7) and no test pins the emitted `targetSetId` (M8) | **HIGH** — self-referential properties falsely rejected (H5); enum transformers and explosion are walls. Loud everywhere, so wrong *answers* are unlikely; **wrong *refusals* are certain** |
| **Inline-embedded** | **YES, and it checks out** — `helperFunctions.pure:432` + `functions_Mapping.pure:66-72`. T15 should cite these, not the corpus (M15) | **HIGH** — the closure walk is right, but the engine's `assertEquals(1, …)` ambiguity wall (H11) and its subtype validation (H12) are both missing, and embedded-set ids don't resolve (M13) |
| **Embedded (plain)** | Yes (`HelperRelationalBuilder.java:1297-1314`; auto-id `parentId + "_" + prop`) | **MED** — materialization is sound; the cycle guard is dead (H6) |
| **XStore** | **YES, and it refutes the rule.** Metamodel (`mapping.pure:174-180`), compiler (`PropertyMappingBuilder.java:151-281`), six independent executor paths, and four asymmetric fixtures already in our corpus manifest | **HIGH → confirmed present, not latent.** `testModelJoinsToRelationalJoins.pure:399-400` and `testMappingCrossStore.pure:239-242` are shapes we hold and refuse. The `"for now"` at `:1039` / `XStorePureEnds:229` is a capability gap with a **known, enumerable set of failing inputs** |
| **ModelJoin** | Thin — comments cite `relationalModelJoins.pure` goldens; no receipt for set selection or nested-hop composition | **MED-HIGH** — always `setId == null` (M25), last-match-wins nested lookup (M24), closure not consulted. Order-dependent |
| **Enumeration mappings** | Partial — `HelperMappingBuilder:348-351` for the implicit-id rule, `processEnumMapping` for the typo wall | **MED** — the "no match → `[]`" default is uncited (L28); clean-sheet drops included enum mappings entirely (H20) |
| **Clean-sheet (Door 1/3)** | N/A (our own door) | **MED-HIGH** — the bypass is not fact-complete: enum mappings, store substitutions, key facts and poisons all dropped (H20–M23) |

---

## What is genuinely good

- **Walls are loud and correctly placed.** Every deferral in scope throws — no silent drops in the M2M
  or XStore synth paths. `withElement` (`:236-255`) deliberately lets raw `NPE`/`ISE` escape *unwalled*
  so genuine bugs fail the build rather than quietly excluding an element. **That distinction is well
  made and rare.**
- **Fault isolation is per-class, per-set, and per-association** (`:349-352`, `:371-375`, `:424-437`),
  with the strict/module split honest about which errors a strict build must re-raise (`:189-194`).
- **`M2mRouteGuards.setIdMatches` (`:120-133`)** correctly narrowed to the exact engine-default id
  (`fqn.replace("::","_")`), with the audit-23 comment explaining why short-name matching was a real bug.
- **`inlineRootSource` (`:574-613`)** — properly threaded cycle guard, follows user-function chains,
  refuses to invent an "unknown source" variant. **This is what H6's guard should look like.**
- **`XStorePureEnds.xstoreEndOf`'s lossy-view detection (`:105-145`)** is a genuinely subtle correctness
  call: recognizing that an expression-bound or join-chain `+prop` cannot survive the column view, and
  routing such an end through property space instead, with the engine's own reason cited.
- **`materializeEmbedded` / `materializeInlineEmbedded` / `materializeOtherwiseEmbedded` are one owner
  plus two thin wrappers** — no duplication.
- **`translateEnumeratedSource`'s typo wall (`:2503-2510`)** — refusing an `EnumerationMapping` entry
  naming an undeclared enum value, with the engine citation and an explicit note that silently skipping
  turned typos into NULLs in `[1]` slots. **Exactly the right instinct.**
- **The engine receipt for T15 exists and our behaviour matches on the main axis** (include-closure
  resolution). **The self-audit was harder on itself than warranted there; it just cited the wrong artifact.**
