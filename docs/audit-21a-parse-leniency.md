# Audit 21a — Parse leniency / silent-drop risk (commits 71552b5d, 28245c49)

## Verdict

The parse-unlock commits are **honest today but destroy one signal the corpus demonstrably
needs**: the M2M `prop[targetSetId]` route is consumed and dropped with no record, and the
corpus contains mappings (testCrossStoreUnion's `crossMappingUnion`/`crossMappingUnion2`)
where that route targets a NON-root set of a union-mapped class — the exact audit-11
wrong-rows shape, mirrored on the Pure side. Nothing goes silently wrong **today** only
because two unrelated walls happen to sit downstream (UnionSynthesis rejects non-relational
union members loudly; multi-set classes without a union root are poisoned). Neither wall
"knows" about the dropped route; when M2M union members are implemented, root-routing will
produce wrong rows with **zero** signal, because the parser threw away the only copy of the
information. That is a violation of the poison-or-record rule this codebase adopted after
audit 11 — graded HIGH (latent). One engine-parity claim in the commit message is also
half-wrong: for the XStore missing-comma quirk the engine's scanner does stop where ours
stops, but the engine then **discards** the remaining entries (no-EOF-anchor ANTLR rule),
while we parse and KEEP them — our model is knowingly different from the engine's for
`crossStoreUnionTestMapping` (MED). Everything else checks out: explosion-marker drop is
loud via the NewChecker multiplicity wall (MED, semantics still unrecorded), `+local` is
loud via the unknown-property throw and collision-free in the corpus (LOW), the stray-`)`
skip and PROJECTION/association-header leniencies are corpus-clean and cannot flip rows
(LOW). Counts: **1 HIGH, 2 MED, 3 LOW.**

Ground truth used: legend-pure `M3CoreParser.g4` (`mapping` / `mappingLine` rules),
`MappingParser.g4` (shared class/association-mapping header), `RelationalParser.g4`
(Database rule, no EOF), `XStoreParser.java` → `M3AntlrParser.parseMappingInfo`
(no-EOF `mapping()` entry), and engine explosion semantics in
`core_java_platform_binding/.../graphFetch/inMemory/graphFetchInMemory.pure` (~800–840)
and `core/store/m2m/inMemory.pure:217`.

The engine's grammar for an M2M body line, verbatim
(`legend-pure/.../m3parser/antlr/core/M3CoreParser.g4:81-85`):

```
mapping     : (MAPPING_SRC qualifiedName)? (MAPPING_FILTER combinedExpression)?
              mappingLine (COMMA mappingLine)*
mappingLine : ((PLUS qualifiedName COLON type multiplicity)
              | qualifiedName (sourceAndTargetMappingId)?)
              STAR? COLON (ENUMERATION_MAPPING identifier COLON)? combinedExpression
```

The engine walker (`M3AntlrParser.parseMapping`) RECORDS all three heads on
`PurePropertyMapping`: `sourceMappingId`/`targetMappingId`, `explodeProperty`,
`localMappingProperty` (with declared type/mult). We record none.

---

## 1. M2M `prop[targetSetId] : expr` — dropped route — **HIGH (latent wrong-rows generator)**

**Where:** `core/src/main/java/com/legend/parser/MappingGrammarParser.java`,
`parsePureClassMappingBody` (~line 1352): bracket group parsed and discarded;
`ClassMapping.Pure.PropertyBinding` has no targetSetId field to receive it.

**Corpus census (Pure bodies only — Relational-body brackets are a different, pre-existing
path that DOES record `Join.targetSetId`):**

| file | binding | route vs root |
|---|---|---|
| graphFetch/tests/testCrossStoreGraphFetch.pure (crossMapping3, crossMapping4) | `trader[trader_set] : $src` | Trader has ONE set → route == root, benign |
| graphFetch/tests/testCrossStoreUnion.pure (crossMappingUnion) | `product[prod_set_model] : $src` | Product has 2 sets + `*Product: Operation` special_union root → **route ≠ root** |
| graphFetch/tests/testCrossStoreUnion.pure (crossMappingUnion2) | `product[prod_set_model]`, `[prod_set_model2]`, `[prod_set_model3]`, `[prod_set_model4]` on four Trade Pure sets | Product has 5 sets (1 relational + 4 Pure over S_Trade/S_Trade2/3/4) union-rooted → **route ≠ root**, and each Trade branch must pair with a DIFFERENT Product set |
| graphFetch/tests/testOrderedCrossStoreGraphFetch.pure | `trade[trade_set] : $src` | single set, benign |

So YES — the corpus navigates M2M properties whose `[set]` route matters:
`crossMappingUnion2` is the canonical case. `product` resolves via the **association**
`Trade_Product`, and `MappingNormalizer.findPropertyTypeDeep` (line ~3300) deliberately
includes association ends — so the binding does NOT trip the unknown-property throw.
`m2mPropertyValue` (line ~1267) then emits `NewInstanceCast(Product, $src)` with **no set
selection whatsoever**; any later navigation resolution can only root-route. Root of
Product is the union operation → navigating `trade.product` from the branch mapped over
S_Trade2 would union products from ALL FIVE sets — wrong rows, silently.

**Why it is not silent today (verified, not assumed):**
- `UnionSynthesis.synthUnion` (line ~307) throws `"Operation union member set '<id>' ...
  "` for any member that is not Relational/RelationFunction → the whole Trade/Product
  union in both crossMappingUnion mappings is converted to a per-class poison by the
  catch in `MappingNormalizer.normalizeMapping` (~line 300) → loud at query.
- Classes mapped through multiple sets without a union root are poisoned at ~line 288.
- Single-set cases (crossMapping3/4) are additionally poisoned by the `+prodId`
  unknown-property throw (item 3), and route == root there anyway.

**Why HIGH anyway:** the guard rails are coincidental. The moment Pure member sets are
accepted by UnionSynthesis (the obvious next M2M milestone — these are exactly the "5
newly-EVALUABLE XStore cross-property FAILs" the commit says it un-darked in order to
burn), there is no poison, no record, no wall: the information no longer exists anywhere
in the model. Audit 11 established the rule for the relational side ("classified PER-PM
(Join.targetSetId) ... never silent" — comment at MappingNormalizer:779); the Pure side
now violates it at the parser.

**Fix shape:** add `targetSetId` (and `sourceSetId`) to
`ClassMapping.Pure.PropertyBinding` and record it (mirroring the relational Join PM), OR
poison the class mapping at normalize when a Pure binding carried a bracket whose set is
not the target class's only/root set. Recording is strictly better — it is also required
for item 2's eventual implementation.

## 2. `prop*` explosion marker — dropped — **MED (loud today by accident, semantics unrecorded)**

**Engine semantics (verified in sources, not assumed):** `explodeProperty` is recorded per
PurePropertyMapping (M3CoreParser `STAR?`; walker stores it). Execution
(`graphFetchInMemory.pure` ~800–840, `m2m/inMemory.pure:217`): all exploded properties of
a set are evaluated as lists, their **sizes must match** (engine throws
`"...sizes should match for exploded properties ... in set <id>"` otherwise), and the set
emits **one target instance per index** — an index-aligned zip fan-out, changing row
COUNT, not just typing. Note the engine grammar also permits `STAR` after a `[set]` group
and after a `+local` head (`STAR?` applies to both alternatives); we only match `*`
immediately after the bare name — divergence is loud (parse error), acceptable.

**Corpus census:** exactly one site — testCrossStoreGraphFetch.pure crossMapping5,
`tradeId*/prodId*/quantity* : $src.s_trades...` where `s_trades : S_Trade[*]` and the
target T_Trade properties are `[1]`.

**Our behavior, traced:** the binding survives parse (all three names ARE declared on
T_Trade), synthM2M builds `^T_Trade(tradeId = <[*] expr>)`, and
`compiler/spec/NewChecker.java` (~line 91: "FULL subsumption, exactly real pure's
NewValidator") rejects `[*]` into `[1]` → ModelException → per-class poison → loud at
query. **No silent path for the corpus shape.** Latent gap: an explosion over a
`[1]`-shaped expression would be silently accepted and happens to be semantically
identical (zip of size-1 lists), so no wrong-rows scenario exists even latently — but the
loudness is an accident of multiplicity, and the poison message ("declares multiplicity
[1] ...") mis-describes the real wall (unimplemented explosion). Record a boolean
`explode` on the binding so the wall can say what it means, and so item 1's fix has a
complete PropertyBinding.

## 3. `+local : Type[m] : expr` — annotations consumed, binding recorded as ordinary — **LOW (corpus-clean, loud), with a latent collision note**

**Corpus census (Pure bodies):** `+prodId : String[1]` in testCrossStoreGraphFetch
(crossMapping3, target Trade) and testOrderedCrossStoreGraphFetch (two mappings, target
ordered::Trade). Verified against the class declarations: **neither Trade declares
`prodId`**, no super/association property matches (ordered::Trade has `productId`/
`productIdString` quals — different names). So `synthM2M`'s check at
MappingNormalizer:1249 throws `"M2M PropertyBinding 'prodId' is not declared on class"`
→ per-class poison → loud at query. (The many `+firmId`/`+entityIdFk` corpus hits are
RELATIONAL bodies — a pre-existing recording path not touched by these commits.)

**Latent hazard worth the note:** if a `+local` name ever collides with a real (or
inherited, or association) property, the binding silently RETARGETS the real property —
the engine keeps a local mapping property DISTINCT from the class property (walker emits
`localMappingProperty` with its own type/mult). That would be a silent semantic change,
not a loud error. No corpus instance exists; recording a `local` flag (one boolean) both
removes the hazard and lets the poison message say "local mapping properties are a
roadmap feature" instead of the misleading "not declared on class".

## 4a. Stray top-level `)` skip — **LOW (verified corpus quirk; accepts-invalid only)**

**Quirk verified:** m2m2rExecutionPlanTests.pure has 174 `(` / 175 `)`; the balance goes
negative exactly once, at line 284 — a doubled `)` closing the `###Relational` Database
block. **Engine tolerance verified mechanically:** legend-pure `RelationalParser.g4`'s
`definition` rule (DATABASE ... GROUP_CLOSE) has **no EOF anchor**, so
`parser.definition()` stops after the balanced block and ANTLR silently ignores trailing
tokens — the engine never sees the stray token at all.

**Scope check:** `ElementParser.skipTopLevelNonElement` is called only from the
top-of-file element loop, and skips exactly one PAREN_CLOSE per call — but the loop
re-invokes it, so ANY number of stray `)` between ANY elements in ANY file are accepted,
which is broader than the engine's positional (trailing-only, island-section) tolerance.
It cannot flip rows: `)` can never begin an element, so no valid input changes meaning;
the only cost is accepting some inputs the engine's ###Pure section would reject, and
masking a hypothetical truncated-file bug. Acceptable; a stricter variant would only skip
when the previous element was a Database/Mapping block.

## 4b. XStore missing-comma tolerance — **MED (parity claim is half-wrong; we keep what the engine drops)**

**Quirk verified:** testMappingCrossStore.pure lines 239–242, mapping
`crossStoreUnionTestMapping`: `employees[firm1, person1]: ...` is followed by
`firm[person2, firm2]: ...` with **no comma**.

**Engine behavior verified from sources:** XStoreParser → `M3AntlrParser.parseMappingInfo`
→ `parser.mapping()` — the `mapping` rule (`mappingLine (COMMA mappingLine)*`) has **no
EOF anchor**. ANTLR matches the first two lines, then the rule completes and the
remaining tokens (`firm[person2, firm2]: ...`, `employees[firm2, person2]: ...`) are
silently DISCARDED. The engine's compiled mapping has only the person1/firm1 pair. The
file's own golden corroborates this: `filterAssociationIsEmptyTDS` for the union mapping
expects `John,Firm D\nJohn,TDSNull\nJoe,TDSNull` — the person2-branch rows cannot reach a
firm, exactly what a dropped second pair produces.

**Our behavior:** the scan stops at the `name[` head and the loop then parses entries 3–4
as REAL XStoreProperties — we keep four where the engine keeps two. The commit comment
("engine's scanner stops here too") is true of the scanner and false of the model. This
cannot produce a wrong-rows PASS (any execution through the extra pair diverges from
golden → loud mismatch; today the family sits at XStore walls anyway), but it is a
knowingly-different compiled model — the exact thing the scoreboard is supposed to
measure. Match the engine: after a missing comma, DROP the remaining entries (optionally
with a parse-note), or wall the mapping. Secondary tightening: the stop condition
(`isIdentifierToken && peek(1)==BRACKET_OPEN` at depth 0) does not verify the
`] ... :` tail of a genuine head; harmless for boolean XStore expressions (no `ident[`
form exists in that grammar position) but cheap to tighten while fixing the above.

## 5. Nominal PROJECTION stubs — **LOW (zero corpus references; loud-by-unmapped if ever used)**

**Census:** `projects` grammar appears in exactly one corpus file,
relational/tests/testModel/projectionTestModel.pure (4 projection classes + 1 projection
association). A corpus-wide grep for every projection name
(EntityWithAddressProjection, FirmProjection, PersonProjection, AddressProjection,
EmploymentProjection) finds **no reference outside the defining file** — no mapping maps
them, no query navigates them, no serialize touches them. The empty-ClassDefinition stub
is inert in this sweep; a hypothetical user query hits "class not mapped" /
"property not found" (both loud) before any empty envelope could pass an assert.

Two notes, no action required now: (a) `Association X projects Y<A,B>` registers as a
**ClassDefinition** — an element-kind swap; if a future corpus file references the
projected association BY association-kind lookup it will miss (loud) or, worse, kind
checks that fall back to class lookup could get confused — prefer a dedicated nominal
kind when projections get semantics. (b) The `"projects"` keyword test fires only in the
two positions after a class/association name where no other grammar production admits a
bare identifier — scope is narrow (verified against parseClassDefinition /
parseAssociation flow).

## 6. AssociationMapping header `*` / `[setId]` acceptance-and-drop — **LOW (grammar parity confirmed; no corpus duplicates)**

**Parity confirmed at the source:** legend-pure `MappingParser.g4:32` uses ONE header rule
for every class-or-association mapping:
`classMapping: (STAR)? qualifiedName (BRACKET_OPEN classMappingId BRACKET_CLOSE)? ...
COLON parserName mappingInstance` — so `*Vehicle_VehicleOwner` and
`Trade_LegalEntity[trade_legal]` are legal engine headers; our old guards were stricter
than the reference, and updating the pin from assert-reject to assert-accept was correct.

**Duplicate census:** a programmatic sweep of every `Mapping (...)` block in the corpus
for repeated association-mapping heads (XStore or legacy nested AssociationMapping, same
association FQN, any setId) found **zero duplicates**; nothing in the corpus references
an association-mapping id either. Mechanism note: our parser stores association mappings
in a LIST (`MappingGrammarParser` accum, `LegacyMappingDefinition.associationMappings`),
so a duplicate would be double-recorded rather than first-wins at parse — the dedup
behavior of the downstream consumer (AssociationSynthesis) is unpinned. If a duplicate
ever appears, add a loud duplicate-header guard rather than relying on consumer order.

---

## Recommended fix order

1. Record `targetSetId`/`sourceSetId`, `explode`, and `local` on
   `ClassMapping.Pure.PropertyBinding` (parser writes, nothing downstream may drop them
   unread — the audit-11 rule). Poison in synthM2M when a recorded route is not the
   target class's only/root set. (items 1, 2, 3)
2. Engine-match the XStore missing-comma path: discard entries after the missing comma,
   and correct the comment. (item 4b)
3. Optional hardening: positional gate on the stray-`)` skip; duplicate
   association-mapping-header guard. (items 4a, 6)
