## A2: legend-engine's SQL text printers deleted; lite's own SQL text in their place

Stage A ratified by the user 2026-11-19. Plan: `docs/records/compat-final-plan-2026-11-19.md` §4. Governed by `compat-rules-of-record-2026-11-19.md`. Build spec: `A2-shape-note.md` and `A2-roster.tsv` on `claude/census-b2`. Branch `claude/compat-a2-printers` (6551c2036), based on `main` b75d6c73a.

### Counts
- **Corpus fail rosters.** H2 276 -> 423 (+147); DuckDB 108 -> 260 (+152).
  - Every added test is in `A2-roster.tsv`, with its class: 0 extra, 0 missing, 0 removed.
  - The classes:
    - a1: the text failed and the rows appeal declined;
    - a2: a printed SQL, plan or DDL string compared with no appeal;
    - a3: lite has no form for the construct printed;
    - a4: SQL read from legend-engine's plan;
    - b: a diverged appeal;
    - c: legend-engine's plan structure.
- **The corpus registers.** Each is explained in `docs/GATES.md`.
  - Unordered: H2 899 -> 1,329 (+434, -4); DuckDB 913 -> 1,348 (+440, -5).
    - An added line is a test that passed by its text and is now judged by its rows through the appeal; its chain has no sort, so the rows compare as a multiset.
    - A removed line is a test that now fails.
  - Engine-order (DuckDB): -2, +1. Same cause.
  - Ord: H2 +4, DuckDB +5 `ordered-keys-unmappable`. These are sorted tests judged by rows now, whose sort keys the row compare cannot map, so they keep the counted multiset compare.
  - Skipped: unchanged.
- **The edge nets.**
  - The pure/v1 net is unchanged: its activities' SQL was already lite's.
  - All 27 PCT edge-net files are unchanged.
  - The corpus net's judge ledgers move on the roster's tests alone, plus 9 (H2) and 8 (DuckDB) tests that already failed at main, whose later asserts' lines left. Outcomes are unchanged.
- **Lean SQL:** `//spec:lean_sql_corpus_{h2,duckdb}` pass. Lite's SQL did not grow.
- **The planted pure/v1 requests** (`server/PureV1SqlTextTest`):
  - The net's own `tds` request, an ordinary one, answers execute and generatePlan byte-identical to the net's lines, which were recorded before A2.
  - A DDL native (the upstream plan facet's path) answers exactly the edge's H2 dialect's DDL.
  - An execute's activity SQL read back (the activity-text two-form's path) answers lite's H2 text.
  - Both are pinned in `edge/pure_v1_sql_text.tsv`. pure/v1 execute refuses a scalar result by name, before A2 and after, so these two are pinned through generatePlan.

### What changed
**Deleted:**
- the four printer files, 2,990 lines: `EngineStyleH2`, `EngineStyleDB2`, `EngineStyleComposite`, `EngineTextDialect`;
- `PureText.engineText` and `enginePlanText`;
- `EdgeLoweringForms.engineSql` and `EngineShape`;
- `RelationalRootForm.apply` and `enumRawColumn`. The class keeps the product resolver's key rule;
- the dead `relationalRootForm` flag on both `Compiler.lowerResolved` overloads;
- `PurePrintForms.enginePasses` and `FloatLiteral.AS_SPELLED`;
- `LegacyTextTest`, whose subject was legend-engine's legacy text.

**Added:** `edge/LiteSqlText` (package-private).
- It renders the plan's own SQL text through the edge's dialect of the database (`PureText.dialect`). It computes no value.
- Each of the 17 printer call sites asks it.
- A database lite has no dialect for is refused by name ("SQL dialect for database type 'DB2' is not implemented yet").
- `toSQLString` and plan text now answer on all 12 lite dialects. Before, only H2's, DB2's and Composite's legend-engine text answered. This is a listed move.
- The family's legend-engine-only questions get their plainest answers: no template parameters, no FreeMarker collection splice (refused by name), an alias as given, no separate select list, the statement as written, and the dialect's own select of a temp table. A2's second step deletes those questions.

**Kept consistent** (legend-engine's printer kept these by spelling every name bare):
- `toDDL`'s drop and create, and the printed setup's schema, table and insert statements, name a table as its store declared it.
- A seed's date and timestamp cell is a string the dialect escapes, as legend-engine's text quoted it.
- A TIME cell stays refused by name.

`PureMessages.sentence` stays: B1 deletes it.

**The register:** row **S380** (CP-4, SQL text never promised) is DRAFTED for the user's word. The id is provisional until landing, coordinated with the product track's rows. S4 is marked superseded by it.

### Guard files moved, for the user's word
Each carries its dated note: "A2 of the compatibility program, stage A ratified by the user 2026-11-19".
1. **ArchitectureTest:**
   - Invariant 4d's two quarantine rules go with their subjects. `PRODUCT_REACHES_NO_EDGE` still holds the product off the edge, and `LiteSqlText` is package-private.
   - `upstreamSidesOutsideTheEdge` drops its `EngineTextDialect` clause.
   - The planner entries lose `EngineShape`'s six calls: Styled's constructor, forms, lists, pureOnly, the edge's dateTimes, and SubselectPrune's union-frame prune.
   - The temporal-literal makers lose `EngineSeedText`.
2. **CodeShapeGuardrailTest:** the mutable-field allowlist loses `EngineStyleH2.frameDepth` and `anonDistinctDepth`.
3. **DialectBoundaryTest:**
   - `PureText.java`'s dialect constructions: **17 -> 12**.
   - `PureText.java`'s switch arms on a database type: 20 -> 13.
4. **DialectOwnerExclusivityTest:** the `EngineStyleH2.java` DuckDB crossing leaves. The extends-chain check is retargeted from EngineStyleComposite to MariaDb.
5. **DialectVocabularyTest:** the concrete-dialect floor goes 15 -> 12. It counted the three printers.
6. **JavaEvalLedgerTest:**
   - `EnginePlans.java` goes 760 -> 757.
   - The edge's closed file list loses the four printer files and **gains `LiteSqlText.java`**. FLAGGED: a new entry in a closed list.
7. **ParkedWorkLedgerTest:** PARK-18 closes with its file, and `docs/PARKED_WORK_LEDGER.md` says so.
8. **PlannerSurfaceTest:** the two `Compiler#lowerResolved` signatures lose the `boolean`.
9. **CompatSpellingGuardTest:** its plant moves from EngineStyleH2 to EnginePlans, because GuardNamesExistTest refuses a stale name. Its forbidden names keep the deleted ones.
10. **Fixtures, rosters and goldens:**
    - `pre-quoted-names.tsv` loses its 192 printer rows.
    - `planner/PlantedHostEdgeReach` is replanted on PureText.
    - The corpus rosters and registers move (above).
    - The corpus edge net is re-recorded.
    - The capability matrix pages are regenerated without the printers' columns.
    - parser-equivalence's own corpus goes 3310 -> 3311, PureV1SqlTextTest's model, with its history line.

### Found, for later (not changed here)
- With EngineShape gone, `LoweringForms.unionFrame()` answers null in both forms. The Lowerer's union-frame branches and SubselectPrune's kept-frame parameter now do nothing. This is a core cleanup, for B5 or slice 4.
- The product dialects' hooks only the printers overrode (keyword, aggregateName and the like) are A2's second step's question.

### Proof
On the final commit 6551c2036:
- `bazel test --lockfile_mode=error //gates:local`: 558 pass and 1 skipped by design.
- The 12 targets that refuse root (the 11 Postgres targets and //datacube:verify_app_test) re-run as `ubuntu` under `tools/postgres/as_unprivileged.sh` (`--strategy=TestRunner=standalone`): 12 pass. Green.
- The corpus host passes `//spec:judge_host_{h2,duckdb}`: both verdicts SUCCESSFUL, the rosters as above.
- `//pct:edge_contract_test` (the three nets) and `//spec:lean_sql_corpus_{h2,duckdb}`: green, the nets as above.
- Not run here:
  - CI's other operating systems (Windows, macOS);
  - the lanes CI runs alone;
  - the merged batch, which the judges' session builds.
