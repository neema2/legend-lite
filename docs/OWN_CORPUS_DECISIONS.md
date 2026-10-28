# Own-Corpus Decision Ledger — the 75 pinned leniency rows

**State (2026-08-11, post census batches 1–3):** 949 snippets extracted
from our own test sources; 758 oracle-accepted; 114 both-refuse; **75
rows where the 4.138.2 oracle refuses and lite's lenient surface
accepts** — every one classified, pinned in
`OwnCorpusConformanceTest`, ZERO unclassified. This document lists every
row for an explicit keep/conform decision. Regenerate the row list any
time: run `OwnCorpusConformanceTest` and read
`parser-equivalence/target/own-corpus-conformance.txt`.

**How to read a row id:** `file#N` is the file's N-th extracted
string-literal run (extractor order), not a line number. The construct
column carries a grep-able cue.

**Decision key:** KEEP = deliberate, stays, class stays pinned.
CONFORM = rewrite the test/feature to oracle shape. Every DECISION
cell below is `DECIDED:` (2026-08-11 review — all classes confirmed).

---

## 0. Why the engine reads legend-pure without barfing (context)

The engine has **two entirely separate Pure parsers**, and the oracle
is only one of them:

1. **The user-model grammar** (`PureGrammarParser`, ANTLR
   `CorePureGrammarParser` + section grammars) — parses `###`-sectioned
   Legend text: what Studio/SDLC users write. This is the corpus
   oracle. It is deliberately a RESTRICTED dialect: no `native
   function`, no type/multiplicity parameters, no function-type
   signatures, star-only imports. Refusals like "Type and/or
   multiplicity parameters are not authorized in Legend Engine" are
   the engine NAMING its own subset.
2. **The legend-pure m3/m4 parsers** (`M3Parser.g4` and the m2 store
   grammars in the legend-pure repo) — parse full-dialect `.pure`
   files: the stdlib, the relation contracts, PCT sources.

The engine never feeds `.pure` files through parser #1. At **build
time**, legend-pure sources are compiled by parser #2 into
`legend-engine-pure-code-compiled-core` (verified in the 4.138.2 jar:
30,820 generated `Root_meta_*` Java classes + a 23.7&nbsp;MB
`pure-core.par` serialized graph archive + the `.pure` sources riding
along as inert resources). At **runtime** the engine loads that
precompiled graph — no `.pure` text is ever parsed by the user
grammar. That is why harvesting engine-embedded `.pure` files into the
corpus and feeding them to the oracle produces the ORACLE-DEFECT
crash class: those files target parser #2, and the oracle was never
meant to read them.

**Consequence for lite:** lite has ONE parser that speaks the union of
both dialects (with the strict/`legendStrict` surface reproducing
parser #1's refusals verbatim). Every "pure-dialect" row below is a
construct parser #2 accepts and parser #1 refuses — being ahead of the
oracle there is matching legend-pure, not inventing.

## 0b. `^Class()` in queries — probe verdict (ZCaretQueryProbe)

End-to-end against the real 4.138.2 engine (compile + relational plan
generation on the classpath):

| lambda | engine compile | engine plan |
|---|---|---|
| `\|Person.all()->project(...)` (control) | OK | OK — `RelationalTdsInstantiationExecutionNode` (SQL) |
| `\|^Person(name='x')` | OK | OK — but `PureExpressionPlatformExecutionNode` (in-memory platform eval, never SQL) |
| `^Person(...)` inside `filter` | OK | **REFUSED**: "No SQL translation exists for the PURE function 'new_Class…'" |
| `^Person(...)` inside `project` | OK | **REFUSED**: same |

So the engine does NOT ban `^X()` in queries grammatically or at
compile; its RELATIONAL LOWERING cannot translate instantiation to
SQL. Lite lowers `^X()` to SQL (STRUCT values / typed carriers —
corpus 2447 design). This is a **capability superset at the lowering
level**, not a grammar invention — and it is load-bearing for
mappings-as-functions (clean-sheet bodies are
`...->map(r|^Person(...))`).

- **DECISION:** DECIDED: **KEEP** (2026-08-11 review — required by
  mappings-as-functions; engine's own surface accepts the text and only
  its SQL backend falls short).

---

## 1. PURE-DIALECT-function-types — 13 rows

Function-type signatures (`Function<{Integer[1]->Integer[1]}>[1]`,
lambda-typed parameters). Engine message: *"The type {…->…} is not
supported yet"* — the engine names its own gap ("yet"). legend-pure
parses these (function types are core m3). Lite needs them for
higher-order user functions (the `eval`/HigherOrder suites execute
against DuckDB today).

| # | row | construct |
|---|---|---|
| 1 | compiler/TdsLambdaProbeTest#0 | `{->TabularDataSet[1]}` zero-arg fn type |
| 2 | compiler/spec/CompileFunctionTest#5 | `{Integer[1]->Integer[1]}` param |
| 3 | compiler/spec/CompileFunctionTest#7 | same, let-bound lambda |
| 4 | compiler/spec/TypeCheckerTest#294 | `{Integer[1]->Boolean[1]}` predicate param |
| 5 | compiler/spec/UserCallInlinerTest#0 | fn-typed param through the inliner |
| 6–11 | integration/UserFunctionIntegrationTest#68/75/87/92/97/132 | higher-order user fns executed on DuckDB |
| 12–13 | parser/ElementParserTest#51/56 | `{T[1]->U[1]}` generic fn types (parse pins) |

- **DECISION:** DECIDED: **KEEP** (2026-08-11, confirmed) — was proposed: **KEEP** (pure-dialect; engine says "yet";
  executable feature).

## 2. PURE-DIALECT-generics — 7 rows

Type/multiplicity parameters on functions/classes
(`function f<T>(…)`, `<T\|m>`). Engine: *"Type and/or multiplicity
parameters are not authorized in Legend Engine"* — again the engine
naming its own restriction; legend-pure is fully generic (the entire
relation-function surface depends on it).

| # | row | construct |
|---|---|---|
| 14–19 | parser/ElementParserTest#72/81/262/266/271/310 | generic fn/class decls (parse pins) |
| 20 | rcorpus/RelationalCorpusRunner#6 | `meta::pure::tds::extensions::firstNotNull<T>` — a REAL engine-checkout helper the corpus harness registers |

- **DECISION:** DECIDED: **KEEP** (2026-08-11, confirmed) — was proposed: **KEEP**.

## 3. DIALECT-milestoning-range — 1 row

| # | row | construct |
|---|---|---|
| 21 | compiler/spec/UserCallInlinerTest#34 | `.allVersionsInRange($s,$e)` — engine: "is not supported"; legend-pure milestoning has it |

- **DECISION:** DECIDED: **KEEP** (2026-08-11, confirmed) — was proposed: **KEEP**.

## 4. ENGINE-TEST-SCOPED-section — 1 row

| # | row | construct |
|---|---|---|
| 22 | parser/SectionGrammarRegistryTest#14 | `###Toy` — a fixture section for the registry SPI; oracle: "'Toy' is not a known section parser" (same refusal a production engine gives) |

- **DECISION:** DECIDED: **KEEP** (2026-08-11 review). Strict already
  fails engine-verbatim; the lenient skip is corpus-load-bearing
  ("refusing cost the relational corpus its whole library layer").

## 5. LENIENT-TIER-fixture — 1 row

| # | row | construct |
|---|---|---|
| 23 | parser/ElementParserTest#440 | `Service` without `pattern:` — pins lite's lenient default (`pattern -> "/"`). The STRICT surface refuses engine-verbatim ("Field 'pattern' is required") since census batch 1. |

- **DECISION:** DECIDED: **CONFORM** (2026-08-11 review) — the lenient
  `pattern -> "/"` default is REMOVED; `pattern` and `documentation`
  are grammar-required on BOTH surfaces (engine-verbatim messages); the
  fixture now pins the refusal. This row leaves the ledger.

## 6. LITE-DESIGN-inline-association — 2 rows

Clean-sheet inline association predicate:
`Assoc: AssociationMapping { {p, f \| $p.x == $f.y} }`.

| # | row |
|---|---|
| 24 | normalizer/MappingNormalizerTest#51 |
| 25 | parser/ElementParserTest#565 |

- **DECISION:** DECIDED: **KEEP** (2026-08-11, confirmed) — was proposed: **KEEP** (part of the mappings-as-functions
  design family — M5 in MAPPING_CLEAN_SHEET).
- **2026-08-12 retirement probe** (`ZInlineAssocSpellingProbe`): the
  engine's `ModelJoin` accepts the UNTYPED lambda verbatim
  (`assoc: ModelJoin { {p, f | $p.x == $f.y} }` is oracle-accepted),
  so the GRAMMAR is subsumable — but a respell was tried and reverted:
  the construct's identity is the CLEAN-SHEET association binding
  (`Realization.Inline` on a `MappingDefinition`), and a ModelJoin
  member drops the mapping into the legacy-DSL world (all-or-nothing
  mixing rule fires; realization differs). This family rises and falls
  with §8 mapping-as-function — treat §6+§8 as ONE design family. Any
  future respelling decision belongs to the clean-sheet surface design
  (dedicated member keyword / section), not to a keyword swap.

## 7. LITE-DESIGN-json-column-get — RETIRED 2026-08-12 (was 11 rows)

`COLUMN->get('key'[, @Type])` in relational property mappings — the
lite construct binding a JSON/variant column into a class property.

- **DECISION:** **RETIRED — respelled to the ENGINE's own grammar.**
  The 2026-08-11 "KEEP for now" rationale (engine parity = Binding
  transformer, a future leg) was WRONG for the scalar case: the
  engine's `core_relational` semistructured tests use plain scalar
  property mappings with NO Binding —
  `recordId: extractFromSemiStructured([SemiDB]SCHEMA.T.CONTENT,
  'payload.recordId', 'VARCHAR')` — an ordinary prefix `dynaFunc`
  (RelationalParserGrammar `functionOperation`), oracle
  parse-accepted (probe `ZJsonGetSpellingProbe`, which also pinned
  that ANY `->` in a relational operation is rejected: the engine
  relational grammar has no ARROW token). All 11 rows were respelled
  to `extractFromSemiStructured(col, 'path', 'SQLTYPE')`; the parser
  extension (arrowChain, `PRelTypeRef`, the Dialect gate in
  DatabaseProtocolParser) was DELETED, and the SQL-type→pure-type
  mapping lives in `RelOpTranslator.pureTypeFor` at NORMALIZE. The
  Binding transformer remains a future leg for the class-typed case
  only.

## 8. LITE-DESIGN-mapping-as-function — 20 rows

The standing design exception: mapping bodies as FUNCTIONS —
`X: Relational { acme::funcs::personMapping }` (function ref),
`Pure { RawPerson.all()->map(src\|^Person(...)) }` (inline pipeline),
`#>{db.T}#->map(r\|^Person(...))` (relation-source pipeline), and the
fn-ref service query (`query: my::funcs::peopleQuery;`).

| # | row | shape |
|---|---|---|
| 37–39 | compiler/element/PureModelContextTest#53/58/69 | fn-ref bodies (incl. negative fixtures: missing fn, not-a-class) |
| 40–43 | normalizer/LegacyCleanSheetConvergenceTest#14/20/26/32 | inline pipelines, `#>{}#` sources, assoc predicate |
| 44–46 | normalizer/MappingNormalizerTest#34/41/63 | `Pure { fnRef }`, `Relational { Class.all() }` |
| 47 | normalizer/ModelNormalizerTest#101 | service `query:` as fn ref |
| 48–53 | parser/CleanSheetProtocolShapeTest#2/5/8/11/14/18 | the protocol-shape pins for every clean-sheet form |
| 54–56 | parser/ElementParserTest#548/559/563 | parse pins |

- **DECISION:** DECIDED: **KEEP** (2026-08-11, confirmed) — was proposed: **KEEP** (your named exception).

## 9. LITE-DESIGN-sqlite-backend — 2 rows

| # | row | construct |
|---|---|---|
| 57–58 | integration/SQLiteIntegrationTest#46/58 | `type: SQLite` connections — a lite backend the engine's DatabaseType enum lacks (BACKEND_PORTABILITY) |

- **DECISION:** DECIDED: **KEEP**, shape-aligned (2026-08-11 review):
  the spec island is now `specification: SQLite { };` — spelled EXACTLY
  like the engine's DuckDB extension pattern (`SQLite { (path:'...')* }`,
  `PSQLiteSpec`), so a future engine SQLite extension finds our text
  conformant. Its JSON is lite's own since 2026-10-28 (the user, docs/ONE_CONNECTION_DESK_2026_10_28.md D1; the emitter
  refused it until then): `{"_type": "sqlite", "path"}`, on lite's surfaces; legend-engine's routes still refuse it.

## 10. PURE-DIALECT-diagram — 1 row

| # | row | construct |
|---|---|---|
| 59 | lexer/LexerTest#40 | `Diagram my::D(width=1.0) { TypeView v(color=#FFFFCC) }` — legend-pure m2 diagram spelling (geometry attrs; `DiagramAntlrParser.g4` widthFirst/heightFirst). A LEXER fixture: the `#FFFFCC` is deliberately unlexable content proving raw-section skipping. |

- **DECISION:** DECIDED: **KEEP** (2026-08-11, confirmed) — was proposed: **KEEP** (fixture needs foreign content; the
  spelling is real pure anyway).

## 11. PURE-DIALECT-signatures — 16 rows

The relation-contract signature dialect, ported VERBATIM from
legend-pure `.pure` sources (see
`verify-signatures-against-real-legend-pure`): column-set algebra
(`Relation<T+R>`, `T-Z+V`), subset constraints (`⊆`), spec forms
(`ColSpec<Z=(?:K)⊆T>`, `AggColSpec<{…}>`, `FuncColSpec<{…}>`,
`_Window<T>`), plus `native function` declarations (pure-only by
definition; strict refuses them engine-verbatim: "Unsupported
syntax").

| # | row | construct |
|---|---|---|
| 60–64 | parser/ElementParserTest#273/283/287/292/298 | `Relation<T+Z>` / rename `T-Z+V` / `SortInfo<X⊆T>` fn decls |
| 65 | #314 | `native function <<PCT.function>> …cast<T\|m>` |
| 66 | #319 | relation signature |
| 67 | #324 | native fn (strict: Unsupported syntax) |
| 68–72 | #334/344/352/368/376 | extend/sort/rename/filterAdults/write native+relation sigs |
| 73–75 | #383/388/391 | native fns (strict: Unsupported syntax) |

- **DECISION:** DECIDED: **KEEP** (2026-08-11, confirmed) — was proposed: **KEEP** (these ARE legend-pure; deleting
  them deletes the ported relation spec).

## 12. PURE-DIALECT-xstore-tolerance — 1 row

| # | row | construct |
|---|---|---|
| (75a) | parser/ElementParserTest#705 | XStore missing-comma completion — CORPUS-PROVEN (`testMappingCrossStore.pure:238`): legend-pure's compiler completes the rule and drops the rest; a refusing parser cannot read the corpus |

- **DECISION:** DECIDED: **KEEP** (2026-08-11, confirmed) — was proposed: **KEEP** (corpus-required).

## 13. TEST-MACHINERY-fixture — 1 row

| # | row | construct |
|---|---|---|
| (75b) | parser/SectionGrammarRegistryTest#10 | `Whatever my::D;` — a deliberately unknown element kind proving overlay/foreign-section routing |

- **DECISION:** DECIDED: **KEEP** (2026-08-11 review — `Whatever
  my::D;` is opaque bytes inside a skipped `###QueryPostProcessor`
  section, never parsed as an element; strict refuses the file
  engine-verbatim).

---

## THE BINDING PARITY GAP (found during this review)

Probed 2026-08-11 (`ZVariantTrialProbe`): the oracle **ACCEPTS**

```
Mapping m::M ( m::P: Relational { ~mainTable [db::D] T
    manager: Binding m::B: [db::D] T.F } )
```

and lite **REFUSES** it (`Missing table or alias for column 'Binding'`).
The binding transformer is real 4.138.2 engine grammar
(`relationalPropertyMapping: COLON (transformer)? operation`). No
in-scope corpus row exercises the spelling, so gate-8 parity is green —
this is a LATENT accept-parity gap. Closing it is a proper wire batch
(protocol record + PMCD byte-parity for the `bindingTransformer`
serialization + a compile-time wall until Binding semantics exist), and
it is the engine-parity migration path for `->get()` (row class 7).

---

## Open gaps this ledger does NOT cover

- **Resource `.pure` fixtures** (`core/src/test/resources/**`) are
  outside the literal-extraction census (bazel_smoke was sectioned by
  hand in batch 2; `stress/*.pure` and others have not been swept).
- **Assembled-model seams**: runtime string concatenation can still
  produce non-conformant documents the census can't see; the
  lenient→strict endpoint flip is the mechanical net that will catch
  every remaining one.
- **Lenient-tier tolerances not exercised by any test** (e.g.
  optional final `;` in multi-statement bodies is still ACCEPTED by
  the lenient parser — tests were conformed, the tolerance itself was
  not removed). The strict flip adjudicates the tier wholesale.

## 11. LITE-DESIGN-function-types-generics — declared 2026-08-12

Higher-order function-type literals (`{Integer[1]->Integer[1]}`) and
type/multiplicity parameters (generics) on declarations. The engine's own
refusal message is "is not supported **yet**" — reserved-but-unbuilt
grammar; lite's type checker supports the constructs end-to-end
(CompileFunctionTest, UserFunctionIntegrationTest). Declared as the fifth
LITE-DESIGN family after the own-dialect census (2026-08-12: the ENTIRE
compiler-reachable census was this one family — 11 rows). Parses at
LEGEND_LITE; LEGEND_ENGINE refuses with the engine's own messages.
Extension tests are whitelisted and asserted in OwnDialectCensusTest.

## 14. LITE-DESIGN-iq-island — declared 2026-10-14 (IQ island) — 1 row

The `#IQ{...}#` island (docs/IQ_SQL_STUDY_2026_10_13.md, the tenth line): a query written for SAP IQ 16.2 in an island,
`#IQ{ select ... }#` or `#IQ:<profile>{ select ... }#`, which the translator reads as IQ reads it and turns into an
ordinary Relation expression. Upstream has the `#SQL{...}#` island and no other SQL island, so its grammar refuses
`#IQ{` as an unknown island type. The construct is lite's by design.

| # | row | construct |
|---|---|---|
| 1 | protocol/sql/iq/IqIslandRoundTripTest (`aModelsFunctionMayBeAnIsland`) | a `###Pure` function whose body is `#IQ{SELECT TOP 2 id FROM tb('e2e::DB.t') ORDER BY id}#` |

- **Surfaces:** LEGEND_LITE reads it (the product surface) and so does LEGEND_PLATFORM, where it takes the place of
  legend-pure's legacy graph-fetch reading of the same spelling (a tree rooted at a class named `IQ`, which nothing in
  the repository writes). LEGEND_ENGINE keeps the engine's refusal of an unknown island type.
- **The census:** `OwnDialectCensusTest` hosts the row in its marked extension set (`IqIslandRoundTripTest.java`,
  pinned at 1); `OwnCorpusConformanceTest` classifies the engine's refusal under this family (pinned at 1);
  `FixtureAdjudicationTest` pins the engine's refusal as a leniency kind (no parser for the island type `IQ`).
- **DECISION:** DECIDED: **KEEP** (the user's ask, 2026-10-13: the island is the tenth line). A row is added only with a
  test that exercises the island in a model, and pinned in both census tests in the same change.

## 15. LITE-DESIGN-deephaven-backend — declared 2026-10-22 (Deephaven)

A Deephaven server as a database of lite's (docs/DEEPHAVEN_PLAN_2026_10_22.md §4): `type: Deephaven` in a
`RelationalDatabaseConnection`, with two words of its own, the `Deephaven { host; port; useTls; }` specification and
`PreSharedKey { keyVaultReference; }` authentication. legend-engine's `DatabaseType` has no Deephaven, and its grammar
has neither word (its Deephaven extension's `DeephavenConnection` and `###Deephaven` store are another construct, carried
as they are).

| # | row | construct |
|---|---|---|
| (per the census) | parser/DeephavenConnectionTest | `type: Deephaven` connections, the `Deephaven` specification, `PreSharedKey` authentication |

- **Surfaces:** LEGEND_LITE reads all three (the product surface); LEGEND_ENGINE refuses each by name at its token,
  `type: Deephaven` in the engine's own words ("Unknown database type 'Deephaven'"), as SQLite's.
- **JSON:** lite's own (the user, 2026-10-28, docs/ONE_CONNECTION_DESK_2026_10_28.md D1; until then none, the emitter
  refusing each by name): `{"_type": "deephaven", "host", "port", "useTls"}` and `{"_type": "preSharedKey",
  "keyVaultReference"}`, written as legend's extensions write theirs and read back the same, on lite's surfaces (lite's
  own `grammarToJson/model`; a model given as JSON). Backward compatibility still refuses: legend-engine's
  `grammarToJson/model` parses at the engine's dialect and refuses each in the engine's words, its `jsonToGrammar/model`
  has no rule to print either and says so by name.
- **DECISION:** DECIDED: **KEEP** (the user, 2026-10-22: Deephaven is just another backend, and its connection is
  modelled as its own, as Snowflake's is). A row is added only with a test that exercises the construct, and pinned in
  both census tests in the same change.
