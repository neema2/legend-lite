# LITE invention census — delete vs product feature

2026-08-14, user directive: "do a sweep of ALL LITE inventions and
categorize all of them as delete vs real product feature." Precedent:
the `}->` island exit (deleted same day — internal uses migrated to the
real `}#->` spelling, token and plumbing removed outright).

**The rule this census enforces:** an engine-lite *invention* — syntax
neither legend-engine nor legend-pure accepts — is never promoted to a
"lite extension". It gets migrated to real syntax and deleted. Only
DELIBERATE product features earn the `LEGEND_LITE` dialect gate.

**Why this list is complete by instrument, not by eyeball:**

- Everything the LITE surface accepts that the ENGINE surface refuses is
  measured by `OwnDialectCensusTest` (946 LITE-accepts vs 892
  ENGINE-accepts over the own-test snippet corpus); every such row must
  live in a whitelisted extension-test host tied to a declared family in
  `OWN_CORPUS_DECISIONS.md`. There is no unclassified residue.
- Inventions accepted on ALL surfaces (the `}->` class — invisible to
  the dialect census by construction) are guarded by the ENGINE-verdict
  differential gates: `MutationFuzzTest` (950 mutants),
  `FixtureCorpusParityTest` (266 sibling sources),
  `AdversarialParityTest`, and the corpus sweep's leniency catalog
  (ceiling 1470, every row classified). `}->` was found by exactly this
  instrument; nothing else of its class is currently detectable.

## KEEP — deliberate product features

| # | construct | gate | why it stays |
|---|---|---|---|
| K1 | Mapping-as-function + inline association (`X: Relational { fn }`, clean-sheet assoc predicate) | `refusesLiteExtensions`, §6+§8 | THE named design family of the mapping redesign; user-endorsed feature legs. One family — rises and falls together. |
| K2 | SQLite backend (`type: SQLite`, `specification: SQLite {}`) | §9 | Real product backend. Spelled exactly on the engine's DuckDB extension pattern so a future engine SQLite extension finds the text conformant. |
| K3 | Function-type literals + generics on declarations | §11 | The engine's own refusal says "not supported **yet**" — reserved-but-unbuilt engine grammar. Lite's type checker implements it end-to-end. Ahead of the engine, not contrary to it. |
| K4 | BigInteger widening of overflowing integer literals | SpecParser | Value REPRESENTATION, not syntax: legend-pure execution semantics (PCT huge-literal reference tests). The spelling is engine-legal; only the value model differs. |
| K5 | CDecimal promotion of precision-losing float literals | SpecParser | Same family as K4 (PCT decimal-exact reference). The `1.5f` suffix itself is engine-real (`CoreFragmentGrammar` has `('f'\|'F')?`). |

## DELETE — inventions (migrate internal uses to real syntax, then remove)

| # | construct | status | migration |
|---|---|---|---|
| D1 | `}->` fused island exit | **DELETED 2026-08-14** | Internal tests migrated to `}#->`; token, lexer arm, parser plumbing removed. |
| D2 | `comparator(a:T[1], b:T[1]): R[1] { body }` inline expression | **DELETED 2026-08-14** (aee013cb) | A PRINT-ONLY form: legend-pure's `printFunctionDefinition` emits it for a concrete function, but neither the engine grammar nor pure's own M3 parser reads it back (M3 `atomicExpression` has no inline named definition). Our parse arm exists solely for the PCT adapter's print→reparse round trip. Migration: the adapter prints function-valued arguments as REAL typed lambdas (`{a:T[1], b:T[1] \| body}` satisfies the same `Function<{T[1],T[1]->Boolean[1]}>` signature); then delete the `comparator` keyword, the parse arm, and migrate the two internal test pins. |
| D3 | `BOOLEAN` / `BOOL` relational column types | **LITE SIDE DELETED 2026-08-14** (aee013cb; orphaned model `Bool` record left for the sibling's model cleanup) | Migrate lite's own corpus/tests to the engine-legal `BIT`; coordinate with the sibling session (model/execution side reads the column type); then delete the dialect-gated arm in `DatabaseProtocolParser`. |
| D4 | Unterminated LAST statement in a multi-statement code block | **DELETED 2026-08-14** (aee013cb; 8 internal tests migrated) | The engine requires the final terminator, and so does REAL legend-pure (M3 `codeBlock: programLine (END_LINE (programLine END_LINE)*)?` — the multi-statement form carries the trailing `END_LINE`). Lite's tolerance is therefore an invention, not pure-compat. Migration: flip the gate to all dialects, fix whatever internal sources break. |

## NOT inventions — correctly tiered, no action

- The PLATFORM tier (`refusesPlatformDialect` gates): `<>`, date `Z`
  suffix, `.allVersionsInRange`, bracket indexing, class/association
  projections, m2 diagram views, single-line `'''`, parse-time date
  validation. These are REAL legend-pure dialect, needed to parse the
  legend-pure corpora; the LITE product surface already refuses every
  one of them.
- Engine-real oddities we reproduce byte-for-byte (probed): `~distinct`
  as a Mongo collection name, `mainCollection` tilde names, the float
  suffix, dotted/mixed store paths in Mongo connections, the doubled
  `_pure_protocol_type`, the nameless DeploymentConfiguration elements.
  These look invented but are the ENGINE's own behavior.

## The FUNCTION catalog (sibling INVENTION_AUDIT_2026_08_14, per-name re-verified)

The sibling audit found a second axis: 26 natives in
`meta::legend::lite::*` (their count was 19 — `convertDateFormat`,
`hash`, `isNumeric`, `join`, `sub`, `tds` and `traverse` were missed;
the governance test found them on its first run). Every name was RE-VERIFIED here against both upstream repos
(pure definitions, Java registrations, dynaFunction strings) because
the audit itself recorded false positives. Three of its verdicts were
wrong:

| verdict | names | evidence |
|---|---|---|
| **DELETED** (5) | `maxDate`, `minDate`, `variantTo` (zero emitters, zero upstream — dead surface); `percentileCont`, `percentileDisc` (zero emitters; the REAL `meta::pure::functions::math::percentile` 4-arg form carries the continuous flag and lite already lowers it — the pair was pure user-reachable duplication, which the audit had mis-filed as keep) | emitter census + upstream greps 2026-08-14 |
| **ENGINE VOCABULARY, not inventions** (11) | `avg` (legacy `~groupBy` aggregate spelling), `divideRound` (engine dynaFunction), `notEqualAnsi` (engine relationalExtension), `sub` (databricks dynaFn tests), `isNumeric` (duckdb extension), `hash` (memsql dialect), `join` (the REAL relation join's name — lite carries a same-name overload shim), `lessThan`/`lessThanEqual`/`greaterThan`/`greaterThanEqual` (+4 2026-08-16: engine DynaFunc ordering-comparison names — the engine leaves these operands UNTYPED (RelationalParseTreeWalker Literal), so join/filter conditions need Any-typed shims exactly like `notEqualAnsi`; ledger cluster 18) | the audit's census only saw pure-function FQNs, not the engine's relational operation vocabulary |
| **INTERNAL DESUGAR IR** (12, incl. `tds` — the #TDS literal's desugar target; +2 2026-10-16: `timeOf` / `atTime`, a time of day in SQL for the `#Cypher{...}#` island's `localtime` — legend-pure declares `StrictTime` and only prints it, legend-engine refuses it in SQL, so neither has an upstream counterpart; docs/CYPHER_STUDY_2026_10_15.md §12.7; +2 the same day: `instantIn` / `localIn`, a named zone read by the database for the island's `datetime`, each the `convertTimeZone` lowering's pair of zone conversions with a DateTime in and out — upstream knows zones only at formatting and on the connection; §12.8); +1 2026-10-17: `random`, the database's random float in [0, 1) for the island's `rand()`, and -1 the same day: legend-pure declares it (`essential/math/random.pure`), so the platform implements upstream's `meta::pure::functions::math::random` and lite's twin left (docs/CYPHER_STUDY_2026_10_15.md §12.51, §12.81); +1 the same day: `isKind`, the database's reading of a Variant's JSON kind (SqlFn.JSON_TYPE against the dialect's own kind name, SqlExpr.JsonKindLit; the kind named as Cypher names them) for the island's string predicates over a value the rows hold as a Variant — legend-pure's Variant has no kind function; §12.53; +2 2026-10-18: `dateTimeOfText` / `timeOfText`, a temporal value in text carriage (the database's ISO text labelled `TEMPORAL_TEXT`, read by the executor as the date-time or time of day it spells) for the island's nanosecond values as a date and a time of day — a value finer than every database's date-time type leaves the database so; legend-engine carries a date-time in the database's own type, so neither has an upstream counterpart (docs/CYPHER_TCK_TO_ZERO_PLAN_2026_10_17.md W13; +1 2026-10-17: `quotient`, an integer quotient cut toward zero, the database's own integer division (MIR's INT_DIVIDE) — legend-pure's `divide` of integers is a Float and upstream has no integer division, so the SQL islands' estimate read each operand about fourteen times; docs/IQ_CENSUS_CHECKLIST.md) | `legacyNavigate`, `legacyAssocPredicate`, `legacyLocalProperty`, `castAsDeclared`, `typeAsDeclared`, `otherwise`, `parseDateFormat`, `convertDateFormat`, `convertDateTimeFormat`, `convertTimeZoneFormat` (the last four are arity-disambiguating renames of the engine dynaFns `parseDate`/`convertDate`/`convertDateTime`/`convertTimeZone` with a format argument), `adjustTemporal` (+1 2026-08-15: `date::adjust` semantics carrying the LEGACY-print channel mark — engine `extensionDefaults.pure` `mapToDBUnitType` prints `dateadd` units UPPERCASE while `sqlDialectTranslation` defaults print lowercase; `TemporalFrame` stamps it on milestoning window-condition dates so `EngineStyleH2` renders the channel it quotes) 
| **INTERNAL DESUGAR IR, +1 2026-10-16: `inlineView`** | `inlineView<T>(rel:Relation<T>[1]):Relation<T>[1]`, emitted only by the SQL islands' compiler seam (`SqlIslands`, through `SqlToPure.Sources.inlineView`). It marks a derived table or a common table expression whose own query computes window functions: SQL computes those windows over all of the view's rows, before the enclosing WHERE, where Pure's relation composition folds a later filter under them (legend-engine's PCT `testExtendFilterOutNull`, `Fold.filterSlot`). `InlineViewChecker` types it as its relation with every window stamped `extentBoundary`, the rule `ClassSources` applies to a class extent's windows, and erases it. No upstream function of the name (both repos grepped 2026-10-16: one test-suite name only). | the IQ island's corpus lane found 21 wrong answers; Postgres-judged cases in `sql-parity/lite/windows.yaml` showed the same on the Postgres island |
| **INTERNAL DESUGAR IR, +1 2026-10-17: `recurse`** | `recurse<T>(base:Relation<T>[1], step:Function<{Relation<T>[1]->Relation<T>[1]}>[1]):Relation<T>[1]`, emitted only by the `#Cypher{...}#` translator (its unbounded variable-length patterns, docs/CYPHER_STUDY_2026_10_15.md §12.67). The base's rows and, repeatedly, the step's over the previous round's, until a round yields none: the database's recursive common table expression (`WITH RECURSIVE`, UNION ALL), lowered as a derived table (`Lowerer.recurse`, `TypedRecurse`); the step's parameter is the first relation-typed lambda parameter outside the window functions' partition. legend-pure has no recursion over relations (its functions recurse in the interpreter; the relational lowering has no fixpoint) and legend-engine spells no recursive CTE but for its own fold rewrite on lambda-less backends (`FoldsAsRecursiveCtes`). | the openCypher TCK's unbounded patterns (24 rows refused by name) |
| **LITE SURFACE** (2, user-facing product natives — bare-name resolvable) | `navigate` (relation navigation; subsumed traverse; ZERO internal emitters — the initial census filing as "internal" was refuted by the emit-site check), `sourceUrl` (data-URI relation source, deliberately user-callable per `SourceUrlUserCallableTest`'s charter javadoc; also internally emitted, by FQN) | integration tests pin both from user query text |

The audit's `%latest` dialect-leak claim was also wrong — `%latest` is
the engine's own `LATEST_DATE` lexer token (oracle accepts
`.all(%latest)`); its other two leaks (`#TDS`, `^$x(...)`) were real
and are now gated as declared lite extensions.

**`traverse` — RULED AND DELETED (2026-08-14):** it was the OLD
engine-lite first navigation implementation; `navigate` subsumed it
(NavigateChecker's javadoc records the replacement) and verification
showed the integration tests had ALREADY migrated to `navigate()` —
only their comments still said traverse. The two `traverse` natives,
the `_Traversal` class and both `extend(…,_Traversal,…)` overloads
had ZERO handlers anywhere in main code. All deleted; class pin
200 -> 199.

**`#TDS` and `^$x(...)` — PURE-DIALECT, not lite extensions (user
correction):** legend-pure ships the TDS DSL (`platform_dsl_tds`) and
the engine's own `xts-tds` extension parses the accessor forms — the
gate attempt on `#TDS` was REFUTED BY THE GATES (six oracle-accepted
sources) and reverted. **Sharpened by the 2026-08-14 adjudication
probe:** the two claims were about DIFFERENT spellings. The engine's
xt-tds accepts only the BRACE form `#TDS{...}#` (that's what the six
oracle-accepted sources use); the BRACE-LESS `#TDS ... #` form is
legend-pure's `platform_dsl_tds` spelling and the engine REFUSES it —
so the brace-less form now carries a drop-in gate
(`refusesLiteExtensions`, classified `PURE-DIALECT-tds-braceless`)
while PLATFORM/LITE keep it. Same probe: pure-m2 `Primitive X extends
Y` declarations crash the engine's wire parser outright (raw
`InputMismatchException`, null message — upstream row U17) and are now
gated the same way (`PURE-DIALECT-primitive`). The two gates closed
the strict/oracle asymmetry census from 22 rows to 2 (both walker-NPE
upstream defects) and the seam census from 18 to 0. `^$x`
is legend-pure copy-with-update (the engine wire walker NPEs on it —
upstream row): gated drop-in only, classified `DIALECT-copy-new`.

**`^$x(...)` — RULED KEEP at LITE (user, 2026-08-14):** it is the
record-update primitive (copy-with-update — construct a modified copy
of an immutable instance without respelling every field), legend-pure
vocabulary that PLATFORM needs mandatorily (the prelude and corpus
sources use it) and that the LITE product keeps as a declared
extension; only the engine drop-in surface refuses it (the engine's
own wire walker NPEs on it — upstream row).

**The user-reachability partition — DONE 2026-08-14.** Principle: a
bare name is a QUERY against the user's namespace; the compiler
talking to itself uses exact identity. Concretely:

- `Pure.Lite` holds one compile-time constant per lite-internal name
  (string literals of these names at use sites are banned;
  `NativeCatalogGovernanceTest` binds every constant to a registered
  catalog native, so a typo cannot survive one run).
- Every internal producer (normalizer/lowering emissions, the #TDS
  parser desugar), internal consumer (structural matchers,
  `JoinChecker`/`TdsChecker`/`SourceUrlChecker` signature
  resolutions) and lowering registration spells the constant.
- `Pure.wireEmissionName` is the DATA-BOUNDARY translation: engine
  wire vocabulary (`avg`, `divideRound`, dialect `hash`, …) arriving
  through the relational-operation channel is respelled to exact
  identity the moment it enters our AST.
- The bare-name index (`FN_BY_BARE`) simply excludes the lite package
  (minus `LITE_SURFACE`), and `CoreFn.of` refuses bare dispatch of
  INTERNAL_DESUGAR spellings — so user-written `->avg()`,
  `->otherwise(...)`, `tds(...)`, slot-`join` all refuse as unknown
  functions, while shim names that collide with REAL pure functions
  (`join`, `hash`) keep exactly their real overloads.
- Own tests that user-spelled internal vocabulary were themselves
  harness-compensation and were migrated to the REAL spellings:
  `->avg()` → `->average()`, 1-arg `->hash()` → `->hashCode()`.

`NativeCatalogGovernanceTest` pins the whole shape: three named sets
(11 desugar / 7 shims / 2 surface, shrink-only), constants bound to
the catalog, and a leak test asserting no lite-package definition
resolves from any internal bare name.

## Standing verification

`OwnDialectCensusTest` (whitelist shrink-only) + `MutationFuzzTest` +
`FixtureCorpusParityTest` + the sweep's leniency ceiling are the four
instruments that keep this census true going forward: a NEW invention
cannot land without going red in at least one of them.
