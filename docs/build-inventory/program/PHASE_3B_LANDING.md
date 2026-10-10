# Phase 3b (L3): what landed, measured (2026-10-09)

The brief is `PHASES_3B_6.md` (its 3b half); the homework is `PHASE_3B_HOMEWORK_2026_10_09.md`; the plan's row is L3 in
`docs/REBUILD_PROGRAM_2026_10_06.md` §4. The branch is `build/phase3b`, based on main at 691c5002d (Phase 3 landed and
re-judged) and rebased onto 634b2246b (the Python pages landing; python/ and datacube/ only) before CI. Every number below is from a run on the branch on the day given; the judges' procedures are
`START_HERE.md` §5.

## 1. What Phase 3b is (one paragraph)

Phase 3 made the implementation table the one authority over what runs a function, by id. Phase 3b fixes what the
manifest-world census then showed around the edges of that table: two bugs in how a module loads (a view inside a
`Schema` lifted twice; a function's overloads sharing one import scope), the boot layer's twins (the system
metamodel's own versions of 29 upstream functions, hidden by a spelling comparison that missed most of them), one
typing rule ported from legend-pure (a dot call with arguments is a qualified-property call), one crash made an error,
and the reference lane's 58 + 14 disagreement rows each given a verdict. The agreed design is in the brief's §3b.2 and
the user's rulings of 2026-10-09 (3b-O1 (b), 3b-O2 (a), 3b-O3 (b), PARK-14 refused).

## 2. The commits, in order, each with its judge

| # | Commit | Item | Judge, measured |
|---|---|---|---|
| 1 | fb1edea93 | step 1: the homework probes (`//spec:phase3b_probes`, manual) and the census ceilings re-pinned at the measured 37 walls / 1,437 failing bodies | H1: 29 twins by id with other spellings, 3 by spelling too, 14 other versions; H4: 18 names with overloads under different imports |
| 2 | e0b4b197c | 1a (F-L1): a schema view lifted once; the resolver resolves each view once and shares it; `firm-balance-sheet` out of quarantine | `//projects:tests` 58/58; three tests in `SchemaViewLiftTest` |
| 3 | 5ecb5dc73 + 70ee34753 | 5b: the import scope, source and position belong to each element (`ParsedModel.keyOf`: a function's id); a wall or error names the overload | census load walls 37 → 32 (the 2 F-L1 + 3 import-scope walls), bodies OK 15,735 → 15,795, failed 1,437 → 1,421; the own-corpus parity ratchet 2,751 → 2,762 (the new tests' snippets only; the key change moved none, measured on the pre-change tree) |
| 4 | 017c30b78 | 1b: the platform's own Pure is a row kind (`Implementation.PlatformPure`); twins merge by id, upstream's declaration with the platform's body; `shadows` deleted; the 14 other versions decided | census load walls 32 → 27 (the 5 twin files load), bodies OK 15,795 → 15,823, failed 1,421 → 1,430 (versions at boot names are typed now), duplicates 0; the implementation table: PlatformPure 34, Body 2,051 → 2,034, Refused 129 → 132, dangling 0, conflicts 0, unrowed 109 unchanged (after the audit's B1 fix: PlatformPure 35, Refused 131) |
| 5 | dc4633ff5 | 5a: the "ambiguous overload" error names every candidate with all its parameters (no index crash for no-argument candidates) | `AmbiguousOverloadMessageTest` |
| 6 | 407278df5 | 4: a dot call with arguments is looked up among the class's qualified properties through its generalizations; a same-named plain property no longer hides them; PARK-14 refused as legend-pure refuses it | `QualifiedPropertyLookupTest`; census after 4 and 5a: load walls 27, bodies OK 15,823 → 16,133, failed 1,430 → 1,120 (unknown-function failures 403 → 6: the `serializerExtension` rows), walled 32 |
| 7 | fd1f69610 | 3: the reference lane's rows reviewed; `reasons.tsv` rewritten with the verdicts; the golden re-blessed; S40/S41 in the semantics register; the H5 probe | AGREE 74,586 → 76,920; after the audit's B1 fix 76,884 (the 36 calls inside upstream's `inferRelationalType(rop, failOnMatchFailure)` body, which the platform's version now replaces, are ABSENT on our side as every adopted twin's are; measured by rebuilding the lane with the previous typer and with the previous boot order, both 76,884); DROPPED 32 → 22; FAILED bodies 1,469 → 1,152; "reference typed, we FAILED" 1,298 → 984; the three `propertyMappingsByPropertyName` classes and the `PROPERTY_AS_CALL` row gone, `isNotEmpty` (17 calls) joined; `//spec:reference_lane` and `//spec:update_reference_lane_test` green |
| 8 | 32269d1f6 | the audit's fixes (B1, S1-S7, N1, N4; §3) and PARK-13's trace deleted | core (26 targets), guardrails, census 27 / 16,133 / 1,120, spec_tests, the lane's tests; the lane AGREE 76,884 |
| 9 | 1f73f626e | the judges' round: channel B reads the maps by id, the wasm boot probe, the H5 literals as whole models, the compare script | the four corpus passes identical to the baseline; PCT DuckDB/H2/Postgres green; channel B 93 |
| 10 | 4e0c61366 | the documents (this note, the GATES entry, the in-flight line, the state) | — |
| 11 | c96d99238 | the local gate's two pins (`platform` reaches `error`; channel B's pass floor 93); the parity ratchet 2,775 | the local gate 321/321 on the rerun; parity and channel B green |

## 3. The audit, and the fixes (2026-10-09; `evidence/phase3b/AUDIT_PHASE3B.md`)

"Ready after fixes": 1 blocker, 7 should-fix, 5 notes. Each answered in the commit after fd1f69610:

| Item | Fix |
|---|---|
| B1 `inferRelationalType(rop, Boolean)` refused on a false reason (core_relational's mapping execution calls it) | the refusal is gone; the system metamodel gains the version with the flag, delegating to its row-reading one (a platform version, a twin by id: PlatformPure 34 → 35, Refused 132 → 131); the reason names the real caller |
| S1 the TranslationContext refusals' callers unnamed | the reason names them (the engine's graph-fetch planner and test-data generator) |
| S2 census ceilings stale | re-pinned at 27 walls / 1,120 bodies, dated |
| S3 the dot-call refusal wider (generic receivers' raw names) and narrower (primitives fall back) than the design; the milestoned branch keyed on the resolved name | the refusal covers every dot call with arguments except the parser's getAll family (`Person.all($x)`), names the receiver's type; the milestoned branch uses the simple name |
| S4 the lookup is depth-first by arity, not legend-pure's C3 walk with the matcher | the lookup walks the kernel's C3 linearization; the remaining difference (same arity in a class and a generalization with different parameter types) is SEMANTICS_REGISTER S42 |
| S5 the boot adopted before resolution (a system body resolved under the prelude's imports) | the boot resolves first and adopts after, as the graph merge does; the system bodies keep their empty-scope resolution |
| S6 the ledger rows closed one commit after their anchors; stale references | references reworded as history; the rows' deletion stayed in fd1f69610 (the branch lands as one push); PARK-13 closed too (the plan's L3 row), its trace deleted |
| S7 two commit messages overstate (5ecb5dc73's ratchet value; S40's unmeasured claims about legend-engine's SQL and legend-pure's value) | 5ecb5dc73 is corrected by 70ee34753 in history; S40, the `greaterThan` reason row and the homework's H5 say which parts are read from upstream and which are measured |
| N4 the probe's display names | corrected |
| N1 the corpus probe's dead name fallback | removed |

## 4. The judges on the final tip

The four corpus passes (DuckDB and H2, host and database) identical to the baseline built before the change, file for file (22 result files, `compare_judges.py`); the two warehouse passes built (no baseline before the change); PCT DuckDB, H2 and Postgres green; channel B green with one re-pin (unclassified discovery and pass 94 → 93: the platform root's `testGet` is no longer counted as an unclassified test); the parity lane green (`own_corpus.matched` 2,775). The local gate: 321 targets green (on the tip rebased onto protocol leg 5, 3304a48d9: every one on the first pass; before the rebase, five load timeouts passed alone and `core_layering_test` and channel B's pass floor were re-pinned with their reasons); the parity ratchet 2,796 = main's 2,772 + the branch's 24. CI: run 38022354707 on 991df952b, all platforms: green on every job (50 of 51, the dispatch's summary job skipped as always); the linux datacube lane's layout timing check (DataCube's own test: a drag task of 54 ms against a 50 ms budget; macOS and Windows green, main's own runs green) failed once and passed on the re-run of that job.

## 5. The ledger rows that close with Phase 3b

- **PARK-12** (the boot layer's versions): closed by item 1b — `SystemMetamodel.shadows` deleted; each of the 14 other
  versions has a row (8 upstream bodies, 2 walls, `superMapping` a twin after its return type was corrected to
  upstream's, `inferRelationalType(rop, Boolean)` and `relationTreeAsString`'s two `space` versions platform versions
  over the rows); the 5 files load.
- **PARK-14** (a dot call falls back to a function): decided — refused as legend-pure refuses it (item 4); no register
  row, since lite now matches legend-pure.
- **PARK-13** (the debug trace): closed — `Overloads.rawSchemaErasedExpansion`'s `LEGEND_LITE_RAW_EXPAND_TRACE` print
  deleted, the flag off the observability guard's list (dated), the row and its anchor gone (the plan's L3 row).

## 6. The brief's stale statements (§3b.9), corrected where

1. "7 upstream platform files drop" → 5 twins + 2 F-L1: PARK-12's closing text and this document.
2. "`Runtime` and `Mapping` not found (2 elements)" → one bug, the per-name import scope: `ParsedModel`'s javadoc and
   `ImportScopePerElementTest`.
6. `reasons.tsv` `PACKAGE size` had the two sides reversed: rewritten with item 3.
7. GATES Phase 3 entry's "the closure lacks the 4-argument `routeFunction`" → `router_main.pure` loads since item 5b;
   noted in the 3b GATES entry.
9. "about 12 upstream versions" → 14 by id (H2), and `superMapping` was the same-parameter version with another return:
   PARK-12's closing text.

## 7. What is left after 3b (the plan's order)

L7 "resolve once" (PARK-5's full fix, option (c)), then D24's second slice; the cold read's F2 and F3 await the user's
rulings; Phase 6 follows L7.
