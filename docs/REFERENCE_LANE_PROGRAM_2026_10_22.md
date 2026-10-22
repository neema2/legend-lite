# The reference lane program: compile everything legend-pure compiles (2026-10-22)

**Status:** proposed to the user on 2026-10-22, after PR #66 ("The reference lane green again"). Nothing past
phase 0 is started. Each phase lands as its own PRs.

**Why.** `//spec:reference_lane` compiles `core_relational`'s closure, about 12,000 of legend-engine's real Pure
functions, with legend-pure's compiler and with ours, and compares them call by call. Each body legend-pure types and
we refuse is either a gap in lite's compiler or a construct lite chose not to read. Studio, the IDE and every
user's model meet those same gaps. This program closes them, and makes the M2M features lite refuses actually work.

## 1. Where we start (measured on PR #66's code, e0ec357b5)

| | |
|---|---|
| our bodies FAILED | 970 |
| bodies legend-pure types and we FAILED | 807 |
| functions only legend-pure has (partly the dropped files') | 481 |
| source files dropped because the model would not build | 22 |
| calls resolved as legend-pure resolves them (AGREE) | 79,436 |

The 970, by cause (`reference-lane/core_relational-failures.tsv`, grouped by message):

| family | bodies | what it is |
|---|---|---|
| A. forms that demand a written shape | ~348 | 163 spellings lite does not read (`project(col(...))`, a single `agg(...)`); 69 a parameter where a lambda is wanted (`->filter($fn)`); 116 `match` over branches in a variable, and `JSONKeyValue.key` |
| G. an element the model lacks | 103 | mostly the 22 dropped files' mappings and classes |
| B. names used as values | 74 | `system::imports::coreImport`, `new` named by its id, a profile as a value |
| C. natives left out by decision | 65 | the JSON natives whose signatures name a platform-owned type |
| I. type and multiplicity variables | 60 | unbound, or not unified |
| J. overloads | 52 | no overload, ambiguous, no structural match |
| E. the inliner | 50 | NormalizeRequired bodies: `internalize` 30, `resolveSchema` 7, `olapGroupBy` 5, `externalize` 4, `restrict` 4 |
| F. `validate` | 41 | reaches the typer before its desugar |
| D. walls by design | 32 | the engine's SQL printer 19, plan-time TDS schema inference 8 |
| H. legacy TDS against Relation | 22 | |
| K. the tail | 125 | one to six each |

Why the 22 files are dropped (none for typing): M2M per-set routing 7, the M2M enum transformer 6, M2M explosion 4,
units of measure 4, a model-join association binding 1, an embedded mapping whose owner is unknown 1. legend-pure
compiles all of them.

## 2. The rules this program keeps

1. **The language and the target are separate** (agreed with the user, 2026-10-22). Every legal Pure expression
   types, against its declared signature, whatever database runs it: the typer is the single source of truth about
   types (AGENTS invariant 1). Whether a query can run on a target is decided when it is planned, after inlining,
   and refused there by name at its call. Planning never touches a database. Showing those refusals in Studio at
   compile time comes later (phase 6; the user: "the planner errors can come later").
2. **Refuse at the query, not the model.** A model legend-pure compiles, compiles. A feature lite cannot execute
   is refused when a query reaches it, never by failing the whole model.
3. **Measure first.** Every change is proven by a judge: the reference lane for typing, the engine's own tests for
   execution (the relational corpus, PCT, and the M2M lane of phase 0).
4. **No fallbacks** (AGENTS invariant 4). Each fix makes lite do what legend-pure does. Each deliberate difference is
   a `docs/SEMANTICS_REGISTER.md` row.

## 3. The phases

Phases 2 and 3 are two tracks that can run side by side. Phase 2 is the frontend (the checkers and the typer);
phase 3 is the normalizer and lowering.

### Phase 0: the judges

- 0.1 PR #66 (open): the lane green on main, each failure's reason written, and the first cause fixed (`Class<Any>`
  meeting `Type`: 1,148 to 970).
- 0.2 Each dropped file's reason written (done, `reference-lane-2`).
- 0.3 **The M2M lane.** The engine's own M2M tests (`core/store/m2m/tests`: 254 test functions in 49 files, each with
  its expected result) run on DuckDB and H2 by the corpus runner, with committed rosters. No lane runs them today,
  so how much ordinary M2M passes has never been measured. Manual (heavy) first; a CI lane is the user's call.

**Exit:** the M2M lane's baseline recorded in `docs/GATES.md`.

### Phase 1: refuse at the query, not the model

- 1.1 The normalizer's "roadmap feature" refusals that fail the whole model move to the query: per-set routing
  (`M2mRouteGuards`), the M2M enum transformer and explosion (`MappingNormalizer`), and the model-build refusals of
  the same kind beside them (views as join targets, non-plain views, `JoinChainEmission`). The class mapping is kept,
  walled with its reason; a query routed through it is refused by that reason. Tests that expected the model-build
  refusal expect the query's.
- 1.2 The two one-offs: the model-join association binding and the embedded mapping without its owner. Each is a bug
  to fix, or a feature to refuse at the query.

**Exit:** dropped files 22 to 4 (units of measure, a decision below). Their bodies are typed and counted. The counts
in section 1 move up before they move down: bodies hidden today become visible.

### Phase 2 (track A): the language, everything legend-pure types

- 2.1 **Every spelling legend-pure reads** (~163). `project(col(...))`, a single `agg(...)`, name-less columns,
  `renameColumns`, `cast`, graph-fetch trees. These are user queries lite can already lower. Proof: the lane, and
  the relational corpus and PCT, whose rosters may gain passes.
- 2.2 **An argument not written in place** (~185). A form whose argument is a parameter, a let or computed --
  `->filter($profileFilter)`, `->match($matchClass)`, `from($mapping)`, a let-bound tree -- types against the
  form's declared signature. Lowering refuses at planning, by name, what it cannot render after inlining.
- 2.3 **Names as values** (74): import groups as elements, a function named by its id, a profile as a value.
- 2.4 **The typer's other gaps:** type and multiplicity variables (60), overloads (52), the tail (125), one cause at a
  time.
- 2.5 **`validate`** (41): typed after its desugar.

**Exit:** every failure left belongs to phase 4 or 5 or to a decision, each named in the GATES entry.

### Phase 3 (track B): M2M end to end

Proven by the M2M lane (the engine's own tests for each feature), lite integration tests per feature, and the
relational corpus and PCT unchanged.

- 3.1 **The enum transformer** (6 files): the M2M binding decodes its value through the enumeration mapping, as a
  relational property mapping already does.
- 3.2 **Explosion** (4 files, plus the relational corpus's `testCrossMappingJsonToDBWithExplosion`): one target per
  element of the source collection; several exploded properties zip by position (multi-argument `UNNEST` on DuckDB
  and Postgres; other dialects as they can, refused by name where not).
- 3.3 **Per-set routing** (7 files): a synthesized function per set id, the property calling the set it names,
  through graph fetch and unions. The largest of the three.

**Exit:** the dropped files' engine tests (about 40) pass on DuckDB and H2, or each failure is named.

### Phase 4: the system metamodel's coverage

Reflection the failing bodies read, served as the system metamodel serves classes and properties today (tables in
the graph's own database, queried by SQL): stereotypes and tagged values with their profiles, generalizations
(`getAllClassGeneralisations`), packages. Expression trees (`FunctionExpression`, `InstanceValue`, ...) stay out of
scope: lite writes protocol JSON natively (`ProtocolEmitter`), so the engine's own walkers are typed (phase 2),
never run.

### Phase 5: the legacy TDS surface and the inliner

The NormalizeRequired bodies (`resolveSchema`, `olapGroupBy`, `restrict`) and legacy TDS against Relation (22).
`internalize` and `externalize` (34) are external formats: out of scope until ruled (register S13).

### Phase 6: planner diagnostics in Studio (later)

Studio plans the project's entry points (services, executables, tests) against the project's declared target when a
file is saved, and shows a plan's refusals as compile-time errors with their positions. The project target is leg 9's
decision (`docs/PROTOCOL_PROGRAM_2026_10_05.md`).

## 4. Decisions for the user

| # | question | recommendation |
|---|---|---|
| D1 | The JSON natives whose signatures name a platform-owned type (65 bodies) | carry their signatures: typed, and a call refused at lowering as "not implemented", as the prelude does for 65 other upstream natives |
| D2 | The walls by design (32) | keep: the engine's SQL printer and plan-time schema inference are the platform's own (`docs/WORLD_MAP.md`) |
| D3 | Units of measure (4 files; `Measure`/`Unit` types lite does not have) | park as its own item; not M2M |
| D4 | External formats (`internalize`/`externalize`, 34) | keep out of scope until ruled (S13) |
| D5 | The M2M lane: manual (heavy) or a CI lane | manual first, CI once its roster is stable |

## 5. How each PR lands

- Tests for the change: a unit test of the rule, and an integration test where it executes.
- The lane re-run (about 3 minutes): the count falls and no new OVERLOAD, PACKAGE or DRIFT appears.
- The gate stages it can move: `//gates:local`, the corpus passes, PCT, stress, parser equivalence; the M2M lane
  from phase 0 on.
- The golden re-blessed, with a `docs/GATES.md` entry naming the lines that moved and why. Pins move only with dated
  reasons; deliberate differences get register rows.

## 6. Done

- The reference lane: every failure fixed, or named under a decision.
- Dropped files only by decision.
- The M2M lane: the 22 files' engine tests passing.
- The relational corpus and PCT never regress on any database.
