# L7, "resolve once": the design (2026-10-10, for the user's agreement before any edit)

Closes `docs/PARKED_WORK_LEDGER.md` PARK-5. The research is `DEBTS_RESOLVE_AND_TYPE_ONCE.md` (the profile, the
cause, the options weighed on 2026-10-07; the user chose option (c), the full fix, and the cold read's F1 moved it
before Phases 6, 4 and 5). This note is the plan to carry out; it adds nothing to the research but the exact shape.

## 1. The problem, plainly

A call written bare, `map($x, y|...)`, asks "which functions can this name mean". The name resolver answers that
question for the program's own functions (through the file's imports, the element's package, the core-import
packages) and records the answer on the call. For a platform function it answers nothing: the catalog's names are
not in the resolver's universe, so `map` stays bare with no record. Every later check that needs the answer works it
out again from the spelling: the engine's handler table, the name tried under each of the 32 core-import packages,
the form's own declarations, then the natives at each name filtered by arity (`ResolvedNames.referents` →
`BareNames.catalog`). Phase 3 rightly made 21 checks read a call's form by its resolved names, so the question is now
asked about five times per call and this one lookup is 29% of typing time (the eager corpus compile: typing 2,147 ms
on main's rule, 2,533 ms after Phase 3). The 243 calls the typer, the desugars and the mapping normalizer build after
the resolver are bare too, so they pay the same.

## 2. The rule

**Every call's names are worked out once and recorded on the call. Nothing downstream resolves a name.**

- A **parsed call** gets its record from the name resolver, in the one arm that already handles calls: the program's
  candidates from its tiers as today, plus the platform's referents by the bare-name rule (`BareNames`: the engine
  surface, the core-import packages, the form's owned declarations). The record holds every full name the call can
  mean, program and platform alike, not filtered by arity.
- A **built call** (the 243 sites) is born with its record: one builder, `Calls`, the only place after the resolver
  that may construct an `AppliedFunction` from a name. `Calls.platform("map", args)` records the bare-name rule's
  names for `map`; `Calls.exact(fqn, args)` records the one full name; `Calls.form(CoreFn.FILTER, args)` records the
  form's owned declarations; `Calls.like(original, args)` copies the original's record when a call is rebuilt with
  other arguments (the five rewriters). The bare-name rule's answer per name is a table built once per process from
  its three static inputs (they are generated tables already); the rule itself does not change.
- A **reader** reads the record: `ResolvedNames.referents(af)` becomes the record filtered by the call's arity (a
  lookup of the natives at each recorded name, no string building); `names`, `declaredNatives` and `form` keep their
  signatures and read through it. `BareNames.catalog` is then called from nowhere in the product but the resolver and
  the builder's table (PARK-5's anchor goes).
- The typer's candidate collection (`Overloads.candidatesOf`) reads the record; its bare path (candidates by the
  spelling when the record is empty) is deleted. A call whose record is empty after resolution is an unknown function
  and is reported as one, as today, from the record rather than from a second lookup.

Nothing about *which* names a call can mean changes: the three tiers, the own-package tier (W2.3b, separate), the
union of program and platform candidates and the typer's pick among them are all as today. What changes is *when*
the question is answered (once) and *where the answer lives* (on the call).

## 3. The record

`AppliedFunction.candidateFqns` is today "the program's overloads the resolver left for signature matching", empty for
a bare platform call. It becomes **`referents`: every full name the call can mean**, recorded once. Readers that treat
an empty list as "bare, use the spelling" (`Overloads.candidatesOf`, `ResolvedNames.form`'s last line,
`StatementInline`'s name list, `TdsLegacy.matches`) read the record instead. `TdsLegacy.matches`' spelling fallback is
PARK-11's anchor (Phase 4 deletes it with the legacy TDS rows); L7 leaves that line as it is and notes that the
record makes it unreachable for resolved calls.

The record is unfiltered by arity on purpose: the rewriters rebuild calls with other argument lists (`CallShapes`,
`SortChecker`, `OperatorParts`), and a record filtered for the old arity would be wrong for the new one.

## 4. What is deleted

- `BareNames.catalog` and `catalogTiered` as per-check lookups (the resolver and the builder's table are their only
  callers); `ResolvedNames`' dependence on `BareNames`.
- `Overloads.candidatesOf`'s bare path and the `DecisionProbe.bareCall` it reports.
- The resolver's "add the platform's names only when it also found one of the program's" condition
  (`captured && scope.prelude()`): the platform's names are always recorded.
- The identity guard's name-cutting count (`IdentityGuardrailTest` NAME_CUTTING, 106) goes down by the sites that cut
  a name to look it up again; the pin is lowered with the reason.

## 5. What is measured, and the bar

- **Typing time** on the eager corpus compile (`//spec:eager_corpus_compile`, run outside Bazel alternated with
  main's tree, six pairs, the median; `evidence/phase3/park5/` has the scripts and the numbers): at or below main's
  (1,973 to 2,231 ms on the runs recorded; the Phase 3 tip 2,305 to 2,606). A profile (JFR) showing
  `BareNames.catalog` gone from the samples.
- **Candidate sets identical:** `//core:shadow_binding` (the decision probe over the corpus) before and after, the
  CANDIDATES rows compared by call site: the same names at every site. This is the gate the compiler plan's W2.3a wrote
  for the same change, reused.
- **The judges unchanged:** the six corpus passes against a baseline built before the change, PCT, the reference
  lane (every class with its reason, no AGREE lost), the parity lane, the render census (a typer and resolver change
  always runs it).
- The ledger: PARK-5 deleted with its anchor in the fix commit.

## 6. The steps (one branch, `build/l7-resolve-once`)

1. **Baselines** (no code): the six corpus passes copied aside; the shadow probe's candidate rows; six alternated
   runs of the eager compile on main.
2. **The record and the readers:** `referents` on the call; `ResolvedNames` reads it; the resolver records the
   platform's names for every bare call; `Overloads` reads the record. At this point every parsed call is resolved
   once; built calls still bare. Judge: unit suites, the guards, the census; the shadow probe's candidates identical.
3. **The builder:** `Calls`; the 243 sites converted (the typer and its checkers, the desugars, the mapping normalizer,
   validation, lineage, testdatagen); an ArchUnit rule that nothing under `compiler`, `normalizer`, `validation`,
   `lineage` and `testdatagen` constructs an `AppliedFunction` by name except `Calls` (the parser and the resolver
   excepted). Judge as in step 2.
4. **The deletions** (§4), the measurement (§5), the ledger row, the identity pin.
5. The full judges on the final tip, the audit, its fixes, the judges again, one CI run, the landing.

Size: 3 to 4 sessions, most of it step 3 (mechanical, many files). Step 2 alone already removes most of the cost and is
judged on its own before step 3 starts.

## 7. The files (for IN_FLIGHT)

Core: `compiler/NameResolver.java`, `compiler/ResolvedNames.java`, `compiler/BareNames.java`,
`protocol/spec/AppliedFunction.java` (the record's name and meaning; a non-wire field already), a new
`compiler/spec/Calls.java`, `compiler/spec/Overloads.java`, `Typer.java`, `TdsDesugars.java`, `CallShapes.java`,
`SortChecker.java`, `JoinChecker.java`, `JsonChecker.java`, `GroupByChecker.java`, `IsDistinctChecker.java`,
`LambdaBodies.java`, `StaticFold.java`, `SourceSubst.java`, `ReceiverOwnedFunctions.java`, `StatementInline.java`,
`normalizer/RelOpTranslator.java`, `MappingNormalizer.java`, `JoinChainEmission.java`, `ViewRelation.java`,
`DeclaredCoercions.java`, `XStorePureEnds.java`, `validation/ValidateDesugar.java`, `lineage/PkInference.java`,
`lineage/ScanRelations.java`, `testdatagen/TestDataGenerationNatives.java`, `PlanAllocations.java`,
`builtin/TdsLegacy.java` (read only), `platform/CoreFn.java` (read only); `core/src/test/.../ArchitectureTest.java`,
`IdentityGuardrailTest.java`, `ParkedWorkLedgerTest.java`. Not touched: `Compiler.java`'s entry points, the parser.

## 8. Risks, and what answers them

- **A built call whose names differ from what the bare path found today.** The shadow probe's candidate rows are
  compared site by site after step 3; a difference is a finding to explain, never a pin to move.
- **The other lines that touch the resolver** (the protocol program's leg 9 imports; the tolerant-build route 6b):
  both sessions have agreed to design against L7, not beside it; L7 lands first.
- **Memory:** a record on every call. The record is a list of a few strings per call; the eager compile's heap is on
  the measurement (`BootProbe`/the eager probe report it).

## 9. What the user decides

1. The record's name: `referents` (recommended; it says what the list is) or keep `candidateFqns` with its meaning
   changed.
2. Whether step 2 lands on its own (recommended: it is the whole of the measured cost, judged alone, and step 3 is
   then a mechanical follow-up with its own judge), or the branch lands as one.
3. Nothing else: the rule, the record's contents and the measurement are PARK-5's acceptance as written.

## 10. Step 2 as built: where it departs from §2 and §3, and why (2026-10-10, from the audit)

- **The record is at the call's arity where the program found nothing.** For a bare call the program declares
  nothing for, the resolver records the platform's names at the call's arity: exactly the list main's read-time rule
  produced for that call (`BareNames.catalog` filtered by arity, deduplicated by name), so the shadow probe's
  candidate rows judge "nothing changed" row for row. An ambiguous call's record is as main's merge made it (the
  program's candidates, then the platform's names at every arity). The reader returns the record as it is: §2's
  reader-side arity filter ("a lookup of the natives at each recorded name") is not built, because it would drop the
  program's names (no natives there) and change the ambiguous case's list; the typer's matcher filters by arity. §3's
  reason for an unfiltered record, a rewriter that changes a call's arity, is step 3's: `Calls.like(original, args)`
  copies the record at the original's arity and records anew by the rule otherwise. No rewriter changes a
  nothing-found call's arity today (`FromChecker` keeps it; the audit's S2).
- **The resolver records only where the program found nothing** ("left bare or ambiguous" in §2 is "left bare" in the
  code): an ambiguous call's record already holds the platform's names through the merge that precedes it (the
  audit's N5: the ambiguous arm was dead work, a second catalog walk per `map` and `filter`).
- **A call with a record is not resolved again:** its spelling and record stay and only its parameters resolve. The
  normalizer re-resolves synthesized bodies (`ModelNormalizer`'s second pass over derived properties, constraints,
  service queries and views), and with one-name records a second pass would have qualified a bare `from`, `graphFetch`,
  `serialize`, `toMany` or `toVariant` to its platform name and emptied the record (the audit's B2; the test
  `NameResolutionContractTest.resolutionIsIdempotentForARecordedBareCall`).
- **The first scope decides, measured.** With a call's record kept on a second pass, the shadow probe's candidate
  rows (`evidence/l7/compare_candidates.py`, `candidates_step2.txt`) differ from main's in 48 rows, every one a call
  in a synthesized body (a lifted derived property, constraint or view; `ModelNormalizer.resolveSynthesized`) naming a
  platform function the first pass's universe does not hold (`buildModel` resolves with the model's and the boot
  layer's names; the normalizer's second pass adds `platformFqns`): `sin`, `sqrt`, `round`, `replace`, `toLower`,
  `trim`, `weekOfYear`, `sqlTrue` and the like. On main the second pass qualified them to the full name, and the typer
  then typed 46 of them without collecting candidates (no probe row) and 2 (`sqlTrue`, `sqlFalse`) by the bare path;
  now all 48 keep the bare spelling with the one-name record and are typed from it, as the same call in an ordinary
  body always was (main left it bare for the typer's bare path). Each row's ids are the one name's overloads. The
  census is identical (27 walls, 16,133 typed, 1,120 failing); the corpus passes, the render census, the reference lane
  and the lanes judge the outcome.
- **Outside a prelude scope** a bare call records no platform names and `referents` returns the program's candidates
  only, where the old rule appended the platform's at a read; no product caller builds such a scope (the audit's N6
  (v)).
- **The rebuilt call keeps every component** (`island`, `fControl`, `ownerClass` included), where the seven-argument
  constructor dropped the wire's three; nothing-found calls with a record are rebuilt now, so the pre-existing drop
  would have reached them (the audit's N4).

## 11. Step 3 sized from the sites (2026-10-10, while step 2's CI ran; `grep 'new AppliedFunction('` over `core/src/main`)

- **209 direct constructions** outside the parser, the resolver and the record's own file: 94 in `compiler/`, 96 in
  `normalizer/`, 5 in `validation/`, 12 in `protocol/` (the JSON and older-spec readers and the composers: they build
  calls before resolution, as the parser does, and are excepted with it), 2 in `test/` (the test runners' `letFunction`
  and the function under test, resolved through `resolveQuery`). `lineage/`, `testdatagen/` and `plan/` build none
  directly (§7 listed them from the 243 count; that count included the parser's and the readers' sites). So step 3's
  set is **195 sites in 23 files**: `RelOpTranslator` 33, `TdsDesugars` 31, `MappingNormalizer` 24, `Typer` 16,
  `JoinChainEmission` 13, `ViewRelation` 10, `DeclaredCoercions` 7, `LambdaBodies` 7, `JoinChecker` 6, `ValidateDesugar`
  5, `SortChecker` 5, `JsonChecker` 5, `CallShapes` 5, `IsDistinctChecker` 3, and the rest under 3 each.
- **By the name's shape:** 136 bare literals over 54 distinct names (`map` 13, `tableReference` 7, `project` 7, `not`,
  `isEmpty`, `filter`, `equal`, `distinct` 6 each, `if`, `cast` 5 each, ...): `Calls.platform(name, args)`, or
  `Calls.form(CoreFn.X, args)` where the site already holds the form; 25 lite-internal full names (`Pure.Lite.TRUST_ONE`
  18, `LEGACY_ASSOC_PREDICATE` 4, `JOIN_SLOT` 3) and `"meta::legend::lite::tds"`: `Calls.exact`; about 20 names from a
  variable (`original.function()` 6, `fn`, `function`, `name`, `p.fn()`, `d.bodyFunctionFqn()`): `Calls.like(original,
  args)` for the rebuilds, `Calls.exact` for the full names; 4 `AppliedFunction.NEW` (a parser carrier the language has
  no function for): a fifth shape, `Calls.carrier(spelling, args)`, whose record is empty by definition, so the typer's
  carrier paths read it as today. Nine private helper factories already wrap sites (`RelOpTranslator.operatorCall`,
  `SortChecker`'s three, `TdsDesugars`' two, `GetAllChecker.resolveClassRef`, ...): converting a helper converts its
  callers, so the mechanical work is nearer 120 edits than 195.
- **The ArchUnit rule** (§6 step 3): no class under `compiler`, `normalizer`, `validation`, `lineage`, `testdatagen`
  calls an `AppliedFunction` constructor; `Calls` alone may; `parser`, `protocol` (the readers), `test` and
  `NameResolver` are outside the rule. `AppliedFunction.withParameters`, `asGrouped` and `withWrittenDetails` copy the
  record and stay allowed everywhere.

