# The edge program: follow-ups (2026-11-18)

**What this is.** Every piece of work deliberately left out of the edge program (the first total separation of product
and edge), in one list. The user asked for it on 2026-11-18 ("For all the follow-ups we should write a follow-up doc").
Nothing here blocks the program's PR. Each item names where it came from and, where known, who owned it.

Companion documents (branch `claude/total-wall-orch`):
- the handoff for the redo: `docs/records/edge-program-handoff-2026-11-18.md`;
- the plan of record: `docs/records/total-wall-edge-program-2026-11-16.md` (its §9 is copied in part B below);
- the audit list: `docs/records/edge-audit-2026-11-17.md`;
- the next program: `docs/records/edge-portability-program-2026-11-18.md`.

## A. Decisions waiting on the user

| # | Decision | Notes |
|---|---|---|
| A1 | The final audit of the branch | The user, 2026-11-18: it "might happen in another account that has token budget to do at xhigh", in parallel with the PR's CI. The handoff document is its starting point. |
| A2 | Whether the PR merges into main | Merge (main gets the separated, green branch; the redo can start from it or from the commit before), keep the PR open as a record only (main untouched), or merge the documents alone. Best decided after the audit's grades. |
| A3 | Compiler performance fixes | Ranked report delivered (`claude/compiler-perf` @ `632f57862`); that session waits for the user to choose. |
| A4 | Proposal A: drop the unread MIR `tree` from run plans | 89% of plan JSON; a plan-format change. D's readers census and a design note come first. |
| A5 | Stream S (injection safety) | Its session stopped with an error on 2026-11-15. Restart it, or fold it into the redo. |
| A6 | The portability program's open decisions (§9 of its document) | Imported plan nodes that need Java or Pure (refuse, or plan again from the Pure source); each kind-B row re-ruled under "one plan"; `EngineResultEnvelopes` (SQL) beside `PureV1Writer` (Java). |

## B. The plan of record's follow-ups F1-F52 (copied from its §9 on 2026-11-18)

Each follow-up is real, deliberately not done in this program, and has a home. When one starts, it moves into a phase
or its own plan; when it's done, it's struck through here with its commit.

### Product correctness and features
| # | Follow-up | Why later | Home |
|---|---|---|---|
| F1 | **Lists done properly everywhere.** One list design across databases, H2 included (H2 has arrays), so every list operation works on every database: `toOne` over a list, membership, a collection's reduction, and the rest. | §10 item 7: refused by name on H2 today. The general fix is its own design. | its own plan, after E-SQL's E-v5 |
| F2 | **`toOne` by counting, no list.** Exactly one value is a count plus the value, so it needs no list at all. It's leaner on every database and works on H2 at once. | a narrow, cheap step toward F1 | E-SQL, lean SQL (Phase 10) |
| F3 | **`type()` at run time, and mixed lists keeping each element's kind** (the "kinds kept" carrier, O2 step 3). It replaces S341's, R3's and R4's refusals with legend-pure's answers. | §10 item 1: refusals for now | E-SQL, Phase 10 (E-v3's "floor's real fix") |
| F4 | **The product's own `toRepresentation`**, if product users ask for it: one simple form per type, no legend-pure logic. | §10 item 8 | on demand |
| F5 | **legend-engine's answer for an unused derived column that can raise:** ask the engine runner. The edge follows it. | §10 item 2 | D, during Phase 5 |
| F6 | **The Decimal print form** (lite prints `1.100000000000000000` for `1.1->toDecimal()`, legend-pure `1.1`): a proposal with per-database spellings and costs. | outside "everything at the edge" | after D |

### Lean SQL
| # | Follow-up | Home |
|---|---|---|
| F7 | Narrow the may-skip set by per-dialect proof: the size it took from PR1, and more. | E-SQL, E-v3 |
| F8 | Classify the casts and CASEs the renderers write themselves, and make the cast-of-cast elision one lowering pass. | E-SQL, E-v4 |
| F9 | Measure MongoDB's product SQL for the first time. | E-SQL, E-v5 |
| F10 | The FULL OUTER emulation's repeated left side, after the cross-language batch. | E-SQL |

### Edge fidelity, existing differences that the golden net won't flag because they predate D
| # | Follow-up | Home |
|---|---|---|
| F11 | "Missing external parameter(s)": the edge joins with `", "`, upstream with `","`. | D, Phase 5 or after |
| F12 | The edge's main-table sentence isn't upstream's exact text (`RelationalCompilerExtension:350`). | D, Phase 5 or after |
| F13 | H2's product session's `NON_KEYWORDS` includes `SET`, so an `UPDATE T SET` with no alias fails. Latent: lite writes aliases. | with §10 item 9's row |

### Judges and Java (E-Java's v2 and v3, Phase 10)
| # | Follow-up | Home |
|---|---|---|
| F14 | No judge class over 1,000 lines (item 8, parked on `claude/total-wall-e-v3`), plus one page on how a verdict is made. | E-Java v2 |
| F15 | The judges' characters (grammar −20%, essential −10%). The rest comes with Phase 4b. | E-Java v2 |
| F16 | Retire the DATABASE-alone CI pass once it differs from BOTH nowhere. | E-Java v2 |
| F17 | Java's evaluation of values shrunk in E-Java's files (4,448 → ≤ 3,800). | E-Java v3 |

### Other programs and PRs waiting (status to confirm when each resumes)
| # | Follow-up | Home |
|---|---|---|
| F18 | The cross-language fix batch (Java, Python, TS; its scope is fixed). | after D lands |
| F19 | Landing A's two items that #111 introduced. | with F18 |
| F20 | PR #102 (Rust-port Java fixes): waiting on its author. | its author |
| F21 | Compiler performance: the ranked fixes await the user's choice. | the user |
| F22 | Stream S (injection safety): its session stopped failed; to be re-checked. | after D |
| F23 | DataCube's one engine contract: its session is ready for review. | after D |
| F24 | C5-P's follow-up (`queryTimeOutInSeconds: 0` means no limit), pushed at `6771f366` with no PR. | with F18 |
| F25 | Lineage rebuilt upstream's way. | after D |
| F26 | D3 (engine plans in and out); the layered edge design; the date-format fidelity PR; Deephaven. | after D |
| F28 | SQLite `parseFloat('NaN')` answers 0: refuse by name or answer NaN. **Done** (E-SQL, `claude/total-wall-carriers`, 2026-11-16: NaN refused by name, S138; `'abc'` → 0 is being fixed as a parse raise). | E-SQL, Phase 8 |
| F27 | DuckDB's error TEXT on PCT's date "BigNumber" adjust tests varies run to run: DuckDB's threads choose which out-of-range value is reported first. The outcome is stable and only the text varies, but G10 (determinism) wants one text. | E-Java or D, after D |
| F29 | Databricks' double-to-text cast (`Databricks.variantAwareCast`) writes `lower(CAST(x AS STRING))`, and a NaN/inf CASE around it lower-cases a second time (round 69). It is harmless but not lean. | E-SQL, lean SQL |
| F30 | pure/v1 answers a scalar `execute` (e.g. `->size()`) with 500 COMPILATION "a SCALAR result's serialization is unprobed" (found by the net's pure/v1 goldens at `26717de291`). legend-engine answers the value. Read upstream's answer and build it. | after D |
| F31 | pure/v1's error answer for a driver error carries the driver's text WITH the SQL statement (H2 `'abc'->parseInteger()`: "Data conversion error converting \"abc\"; SQL statement: SELECT CAST(...)"). Phase 2 types it (a parse raise); the edge's words follow upstream's answer, read from the engine runner. | D, Phase 2 |
| F32 | A user's fully-qualified call to an internal native RESOLVES today: `meta::legend::lite::typeRaise('a','b','c')`, `cellText('a')`, and presumably `trustOne`, which skips toOne's guard (D measured, 2026-11-16). `INTERNAL_DESUGAR` keeps out bare names only. Refuse internal natives by name to user code, with a test. | D, before landing (Phase 9) |
| F33 | The edge golden net (`//pct:edge_contract_test`: PCT's 27 diff tests, the corpus's 8, pure/v1's 1) is in `//gates:heavy` (D, `431987e1f`, after run 17's lane guard), so CI never runs it; every worker runs it by hand on every push. Before landing, it runs in a per-push CI lane. That adds the PCT SQLite recording to CI, which `gates/BUILD.bazel` marks as the user's call. | orchestrator, before landing (Phase 9) |
| F34 | Lane sessions measure the parse table only through the probe, since `core_tests_integration_<lane>` exists for sqlite and h2 alone. Decide whether the external lanes get integration targets, or whether the probe stays the one cross-database check. | after Phase 8 |
| F35 | `parser/EngineQuirks.RELATIONAL_ARITH_MISASSOCIATION`'s comment claims legend-pure gives `1 < 14`. It doesn't: legend-pure 5.99.0's `AntlrContextToM3CoreInstance.processOp` is the same algorithm as legend-engine's, so both give `1 < 20`. Correct the comment to cite both sources, and rename the flag as Pure's own rule (e.g. `PureGrammar.RELATIONAL_SNATCHES_TOP_LAST_ARGUMENT`). No behaviour change. | any worker touching the parser; before landing |
| F36 | Delete `CFloat.exact` outright once S20 is built (E-P5's 5b stops setting it on every grammar; its readers then see null). | E-P5, after 5b |
| F37 | The corpus one-pass collapse (one BOTH pass as the verdict of record) FAILED its condition: H2's fail roster would gain exactly `h2-database-lost-register`'s 66 rows (the database judge declines or leaves the shape unjudged; AgreementArm is strict). The retirement landed one-for-one instead (about +53 s DuckDB, +26 s H2 per lane). Close the 66 database-lost rows, then collapse, which saves one corpus pass per lane. | E-Java, after Phase 7 |
| F38 | At the edge, upstream H2 casts parseInteger AS INTEGER (toPostgresModel.pure:400; manifest: '9999999999999992' fails, [22018-214]), while lite's edge casts AS BIGINT, so that input passes in lite at the edge. Align the edge's parse cast type per database with upstream's. | D, the next time the edge's parse arm is touched |
| F39 | S20 on ClickHouse, BigQuery and Snowflake: their Float literal kind moves from DECIMAL to DOUBLE once their lanes measure the moves (5b's lane round). | E-P5, Phase 5b |
| F40 | The corpus measurement's ~8 possible real bugs, found when the database judge ran alone (DuckDB: golden JSON that doesn't parse, 2 test-data-generation row mismatches; H2: 3 `Invalid value ... for parameter JSON \| ROW` DataErrors, 2 golden JSON parse). Check each on the HOST road and fix or name it. | E-Java, after step 1 |
| F41 | pure/v1 generatePlan refuses at compile on H2's engine text where execute answers: LIST_FILTER (plan.raise.atOutOfBounds, toOneOverMany), LIST_GET (plan.cell.empty). Read engine's plan text for these (pinned sources or the engine runner), then write it or name the refusal. | E-SQL, Phase 6, after A-C |
| F42 | Very long values in a product test-failure message (a big table under assertEquals) are written whole; consider cutting after N elements with '... and K more', the same in the host's Java and the database's SQL. | E-Java, after step 3 |
| F43 | pure/v1: kinds that legend-engine REFUSES AT PLANNING in its own words (DATE_PART_ABSENT, INDEX_OUT_OF_BOUNDS on at(), DATE_COMPONENT_INVALID, likely USER_FAIL): match the refusals. | D, after the match-engine build |
| F44 | pure/v1: lite-only kinds (INSERT_OUT_OF_BOUNDS, INSTANCE_NOT_OF, WINDOW_FRAME_OFFSET_INVALID, LIST_LENGTHS_DIFFER, MAP_KEY_*, VALUE_NOT_HELD, FRACTION_NOT_KEPT, CSV_TOO_WIDE): ask the engine runner what it does; plain operation where engine has the function with no check, else keep, named. | D |
| F45 | MATCH_FAILURE in compat mode: legend-pure's 'Match failure: <value> instanceOf <Type>' (CompiledSupport:1588; match.pure:240); product mode keeps lite's sentence. | D |
| F46 | DIVIDE_BY_ZERO: fix the javadoc citation (divide: CompiledSupport:1485, prints the divisor; rem: CoreHelper:544 / Rem.java:64, prints the dividend); compat words = legend-pure's compiled answer per form unless a PCT test pins otherwise. | D |
| F47 | ROUND: legend-pure's round(Float, n) is HALF_UP over BigDecimal.valueOf(d), the double's shortest decimal (CoreHelper.java:500-508; RoundWithScale:58). Lite rounds half-even (round(0.25,1) gives 0.2 against 0.3; round(1.015,2) gives 1.01 against 1.02). A product value difference with no register row, so a defect. Fix per database, with tie tests; a database that can't express it comes back as a named register row for the user. | E-P5, after 5b |
| F48 | MIXED LIST: `let x = 1.5*2; [1, $x]->sort()` gives [1.0, 3.0]. A computed element takes the plain list_sort path, which unifies to DOUBLE, while legend-pure keeps the Integer 1 (a literal list does). | E-SQL, after Phase 6 |
| F49 | MUST: a Decimal COLUMN read on SQLite comes back truncated to an integer (0.10 reads 0; 370370367.3703703670 reads 370370367), a silent wrong value in the decoder/carrier path. Fix it, then add the row to FloatAndDecimalTest. | E-SQL, ahead of Phase 6's next move |
| F50 | Postgres's exact Float remainder (no native double remainder: mantissa times a power of two, an exact NUMERIC mod) writes each operand about 100 times, +28K chars on PCT's rem family. Slim it (bind the operands once where grouping allows, e.g. E-SQL's row lets) without losing exactness. | F (moved from E-SQL 2026-11-17 to balance load), after Phase 3 |
| F51 | Invariant 4 (no defaulting): an annotated #TDS Decimal column with no declared scale is typed DECIMAL(38, 0) (TdsChecker.annotatedType), a silent default scale of 0. The exact engines ignore the label; SQLite printed by it (F49, fixed at the carrier). Type it as plain Decimal; where a database needs a scale to declare it, take the literals' own scale (known at planning) or the unconstrained exact decimal, else fail by name. | E-P5, after 5b |
| F52 | Postgres float8→numeric is NOT the shortest decimal: it rounds to 15 significant digits (measured, Postgres 16.15: `123456789.123456789::float8::numeric` = 123456789.123457; `(0.1::float8+0.2::float8)::numeric` = 0.3). Census every lite cast that assumes it is exact or shortest (roundHalfEven's NUMERIC path, listReduce's accumulator, decimal scale casts, exactRemainder's comment); fix with `(x::text)::numeric` (shortest since PG12) where the shortest is meant; one value case each. | E-P5, with F47 |

## C. Deferred during the program (to do after the count reached 0)

| # | Follow-up | Owner then | Source |
|---|---|---|---|
| C1 | The VerdictSql ordering race | E-Java | log 2026-11-17 |
| C2 | Item 3: `VerdictChannelRegister` | E-Java | log 2026-11-17 |
| C3 | DuckDB Integer placeholder typing | E-SQL | log 2026-11-17 |
| C4 | pure/v1's compile parses at the LITE level with no register row (found by E-P5, 2026-11-17 23:48) | E-P5 | log |
| C5 | The Lowerer builder's configuration (dbTimeZone, features, deterministicAsOf, shape, plainRaises, styled, relationTexts) into one final record, shrinking `MUTABLE_FIELD_ALLOWLIST` by about 7; and `CodeShapeGuardrailTest.mutableInstanceStateIsExplicit` made to see multi-line field declarations (F found two evading it, 2026-11-17) | E-SQL / GUARD | log 2026-11-17 |
| C6 | The fused shared typed per-assert read for the BOTH judge: the database decides in SQL, the host compares the same typed values in Java, nothing legend-pure in product SQL; measured against the fused 892/1114; one lane round (the user ruled 2026-11-16 15:36, "do it in your order") | E-Java | log 2026-11-16 |
| C7 | Host-side batching for the isolated BOTH judge: lane round 77 measured the host judge's per-side reads at 2.55x the round trips; required before isolated BOTH lands | E-Java | lane round 77 |
| C8 | Lineage rebuilt upstream's way for everything (the user, 2026-11-14) | stream D | log |
| C9 | Landing A's two items that #111 introduced; the cross-language fix batch (Java, Python, TS); C5-P's `queryTimeOutInSeconds: 0` follow-up at `6771f366` | - | F18, F19, F24 |
| C10 | PR #102 (Rust-port Java fixes): independent review; waiting on its author | - | F20 |

## D. Found while finishing (2026-11-18)

| # | Follow-up | Source |
|---|---|---|
| D1 | **The edge still builds legend-pure's text inside SQL** (about 1,978 lines: `RelationTextLowering` 780, `PureMessages` 479, `UpstreamVerdictWords` 191, `PurePrintForms`/`TdsNullCells` 184, `EdgePureOnlyTexts` 136, `EngineResultEnvelopes` 88, `PureListTexts` 49, `PureDateTimePrint` 43, `EdgeDateTimeTexts` 28), contrary to the plan's §2.2 and §4 item 1. This is the portability program's first work. | measured at d-stage `4e8702016` |
| D2 | v57's `ExecutionPlan$DecimalDigits#precision/#scale` (+31 plan characters per Decimal(p,s) column) has no reader yet, in Java or in a host. Kept as self-describing plan data (the user, 2026-11-18: "keep it"). Give it a reader (the hosts' decimal decoding, plan export) or reconsider. | GUARD's train-19 trial |
| D3 | The wasm engine module ships as a declared opt-in (`//wasm:engine_planner` in `//:wasm`), but no shipped app wires `engine: 'legend-engine'` yet; a client opts in through engine-client's `loadEngineModule`. Wire it where a legend-engine client needs it. | the TS host, K2 |
| D4 | The K10 raise (+40,874 characters on PCT channel B's essential judge-plan pin): E-P5's trim did not take it back, as first claimed. Its real cause and whether a no-growth shape exists. (Status at PR time goes here.) | E-P5, 2026-11-18 |
| D5 | The edge golden net compares the edge's SQL text byte for byte, which rewarded copying code over redesigning it; make it compare outcomes (verdicts, answer bytes, messages). | the handoff, §4 |
| D6 | `//pct:edge_contract_test` (the edge golden net) runs in `//gates:heavy` only; CI never runs it. Give it a per-push CI lane. | F33 |
| D7 | The workers ran at the default effort level; set the effort explicitly on every session at creation. | the handoff, §4 |
