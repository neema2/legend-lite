# The manifest world, tested: what the default world is (2026-10-06)

Follows `docs/MANIFEST_WORLD_HOMEWORK_2026_10_05.md`. The question: what does a program **without** a manifest (a
user's model in Studio, DataCube, Query, the server) get for free, decided by upstream rather than by a list of ours,
and does it work for users, the corpus, PCT, boot time and the browser?

**Evidence:** `docs/build-inventory/manifest-world/experiments/` (its README names every script and output;
`rerun.sh` regenerates the analysis). Nothing in the product was changed: every run swapped `prelude.pure` on the
classpath of an unmodified build. So every run still booted from the prelude, as today, and still had Pure.java's
catalog: the experiments change what the prelude holds and where it comes from, not the mechanism, and none of them
removed Pure.java's signature text.

**Terminology:** "upstream native" is upstream's `native function` keyword; "platform-lowered" is what our Pure.java
declares. Never bare "native" (`docs/UPSTREAM_BOUNDARY_PROGRAM.md` section 0).

## 1. The result

**The default world is:**
1. **Upstream's own core, whole:** legend-pure `platform*` plus the engine's `core_functions_*`, the modules
   `PureRuntime.loadAndCompileCore` compiles. Tests are stripped by upstream's own markers only (test stereotypes and
   `::tests::` packages; `::test::` packages hold upstream's test infrastructure and stay).
2. **Upstream's own query surface:** every function the engine's core compiler and relational extension register as
   a handler (`Handlers.java`, `CoreCompilerExtension`, `RelationalCompilerExtension`), and every class those two
   compiler modules instantiate.
3. **Closed** over what those reference: through declarations always, and through bodies only of functions whose
   upstream body runs (not lowered, not a language form, not walled).
4. **Built in:** the m3 metamodel declarations upstream has only in `m3.pure`'s bootstrap form (87), and lite's
   stub.

Our only input is a module-level choice: which compiler modules count (core and relational). No list of names, no
pruning, no Java scan.

**Measured** (cold starts; every world uses the ownership filter of section 3):

| World | Elements | JVM boot | Browser first answer | Projects' body walls | Corpus, 6 passes | PCT, 2,737 cases |
|---|---|---|---|---|---|---|
| today's prelude | 641 | 350 ms | 1,500 ms | 146 | baseline | baseline |
| upstream core | 631 | 366 ms | 1,585 ms | 146 | | |
| + our 101 user-facing names | 652 | 364 ms | 1,604 ms | 146 | | |
| + upstream's lists, declarations only | 787 | 383 ms | 1,759 ms | **0** | changed in every lane | identical |
| **+ upstream's lists, runnable bodies** | **929** | **424 ms** | **2,088 ms** | **0** | **identical** | **identical** |

- **Identical** means every output of every pass: fail and skip rosters, both order registers, the ledgers (5,851
  assertions in the DuckDB host pass alone), the verdicts; for PCT, every test case.
- **Today's 146 body walls** in the projects are one function, called 146 times: `meta::pure::functions::lang::orElse`
  (engine `core`, `corefunctions/langExtension.pure`). It is in upstream's handler table and not in today's prelude.
  Upstream's own list found a gap our own lists missed.
- **The demos** have no walls in any world. The projects' 4 build walls are duplicate view functions inside the
  projects themselves, the same in every world.

## 2. The seven experiments

1. **Upstream's lists** (`e1.txt`): 455 handler functions (443 from the core compiler, 13 from the relational
   extension) and 156 instantiated classes. They contain 91 of the 269 engine-side names we carry today, including 58
   of our 60 date, calendar and string functions, and only 8 corpus-harness names. Not counted: the handlers of
   extensions legend-lite does not ship (data quality 19, service 5, JSON 4, external format 5, Elasticsearch 1, data
   space 2).
2. **Pure.java against upstream** (`CatalogUpstreamDiffTest`): of 837 overloads, 794 match an upstream signature id
   exactly, 0 diverge, and the 43 not upstream are all legend-lite's own `meta::legend::lite`. Rows keyed by
   `FunctionId` can replace Pure.java's signature text one for one.
3. **Loose ends** (`LOOSE_ENDS.md`):
   - `toString` is upstream (`toString_Any_1__String_1_`, `platform/pure/essential/string/toString/toString.pure:47`).
   - The 13 unexplained names (Java compiler, Postgres parser, SQL-dialect test hook, ide `debug`) come from the old
     rule "the prelude carries all upstream natives in the roots it reads". Nothing of ours uses them; they leave the
     default world by themselves.
   - Upstream's test infrastructure (PCT manifest helpers, the surveyor) is unused here but is part of upstream core;
     `TestedByResult` is required because `Extension` names it.
4. **Closure with our own resolver** (`closure.txt`): all 16,445 upstream elements of the 32 relevant modules parse and
   resolve with zero walls. Through runnable bodies: our 101 names add 121 names (68 KB); upstream's lists add 483
   names (328 KB); the 15 files holding the user-facing engine names, whole, add 928 names (1.07 MB, mostly plan and
   router machinery). Whole files are rejected.
5. **The user side** (`e5_user_side_summary.txt`): the 56 projects compiled as one graph and the 3 demos, every body
   type-checked. Results in the table above.
6. **Corpus and PCT** (`e6_lanes_*.txt`): each corpus pass's exact Bazel command rerun by hand, and the PCT targets
   through Bazel, with the world first on the classpath. Results in the table above. The corpus gets what it needs
   from three places:
   - its own program files, which the runner already loads from upstream per test: the relational tree (which holds
     `toSQLString`'s upstream body, `SQLResult`, `DbConfig` and the DDL helpers), the M2M test models,
     `LIBRARY_FILES` and the classes of `SHAPE_FILES`;
   - Pure.java's catalog, which declares the functions we implement (`toSQLString`, `executionPlan`, …);
   - the default world.

   The corpus helpers we implement, and what only they need, are not in the proposed world. Harness types that
   upstream's query surface reaches are: about 109 types and 91 functions in the plan, extension, graph-fetch,
   lineage and test-data areas (`Extension`, `ExecutionPlan`, graph-fetch tree helpers…).
7. **Browser** (`e7_*`, `bazel run //wasm:startup`, median of 5): table above. The module grows from 4.71 MB to 5.03
   MB. In every world the boot layer's resolve, normalize and index is about 55% of the first answer, and parsing the
   prelude about 13 to 17%.

## 3. What the design needs, found by the swap

1. **An ownership filter at boot.** Upstream core declares functions the platform owns. Without a filter, the boot
   fails (the system metamodel's own `_classMappingByClass` duplicates upstream's), or calls bind to upstream's
   declarations instead of our forms: `^Class(...)` resolved to upstream's `new` (901 user walls), and legacy TDS
   `project`/`groupBy` with `agg`/`col` broke 107 corpus tests. What the platform owns:
   - the functions Pure.java implements;
   - the language forms, which `CoreFn` registers by **bare name** (`new`, `project`, `groupBy`, `filter`, … 66);
   - the system metamodel's own upstream-named elements (29: `classMappingById`, `allNodes`, `inferRelationalType`, …);
   - the legacy TDS functions `TdsLegacy` implements in Java by name (17);
   - helpers our forms recognize by spelling: `meta::pure::functions::collection::agg`, `meta::pure::tds::agg`,
     `meta::pure::tds::col` (`GroupByChecker.isAggSpelling`, `ProjectChecker`).

   The experiment emulated this with today's prelude footer lists plus those (`e6_owned_final.txt`). The real filter
   is the implementation table, keyed by `FunctionId`, and the forms should match a helper by its resolved id, not
   its spelling.
2. **The boot's own demands** (`bootdemand.txt`). The system metamodel names 70 upstream names, 12 of them harness:
   `ExecutionNode`, `ExecutionPlan`, `SQLExecutionNode`, `SequenceExecutionNode`, `FunctionParameter`,
   `FunctionParametersValidationNode`, `RelationalInstantiationExecutionNode`, `AggregationAwareActivity`,
   `Extension`, `RouterExtension`, `RelationTree`, `ColumnWithContext`. Nothing boots without them, so they are in
   every world measured. Pure.java's catalog signatures (80 names) are not needed at boot.
3. **Bodies of runnable functions.** Following declarations only changes every corpus lane: 17 DuckDB and 9 H2 tests
   newly fail, and the warehouse lane's ledger changes. Their functions' upstream bodies need `removeAll`,
   `joinWithOptionalColumns` and the lineage `PropertyPathNode`, which only a body reaches. A world users can run must
   follow those bodies.

## 4. What does not work yet: the corpus loading its whole closure as its base

Swapping in `closure(core_relational)` (14,440 elements) as the corpus's prelude (`corpus_closure_build_walls.tsv`):
- **Compiler gaps:**
  - units of measure: a `Measure` is not a type to our compiler (`Mass~Kilogram`, `meta::pure::unit::Mass`; 9
    elements);
  - non-test code naming types upstream keeps in test code (12 elements);
  - one resolver miss: `meta::pure::router::routeFunction`'s bare `Extension` does not resolve though the world has
    it.
- **With those excluded it boots** (13,979 elements, 1.4 s in the JVM), but the runner fails: its T4 rule drops a
  test's copy of a class the base also holds, and `RelationReads` looks that class up in the test's graph only.

Today's runner composition by file is the working path. A true manifest load for the corpus needs those fixes first.

## 5. What this changes

- **D9's "2b"** ("whole standard library vs library only", `UPSTREAM_BOUNDARY_PROGRAM.md:273`) is answered by
  measurement: upstream core whole, plus upstream's query surface, closed through runnable bodies.
- **The default manifest** is a module-level choice: the core and relational compiler modules count.
- **The prelude generator** stops reading anything of ours: no Java scan, claims, hand enums, path lists or
  exclusion prefixes. It reads upstream (the Pure files, `Handlers.java` and the compiler sources) and the module
  choice.
- **Pure.java** becomes decision rows keyed by `FunctionId` (experiment 2), with the implementation table as the
  ownership filter (section 3).
- **The corpus helpers we implement** (SQL text and DDL helpers, plans, test-data generation, JSON checks), and what
  only they need, leave the default world. The corpus's own files supply them, and Pure.java's catalog declares them
  until its signature text goes. Harness types that upstream's query surface reaches stay in the default world.

## 6. Costs and open items

**Costs:** +74 ms cold JVM boot (350 to 424 ms), +590 ms browser first answer (1,500 to 2,088 ms, +39%), +0.33 MB
WASM module.

**Open, each with what settles it:**
1. **Browser start-up:** accept +0.6 s, or pre-bake the boot layer (resolve, normalize, index are 55% of the first
   answer in every world; `wasm/startup.mjs` exists to answer exactly this). Settled by prototyping a pre-baked boot
   layer and timing it with the same harness.
2. **The boot's demands:** keep the 12 harness types as a boot registration ("what Java needs at boot", compiler
   design section 3.1), or make the system metamodel's plan and lineage mappings conditional on the world containing
   them.
3. **Execution on the user side:** the projects and demos were type-checked, not executed; the corpus and PCT
   executed. Settled by running the demos' queries against the world.
4. **Visibility semantics** for manifest-loaded worlds (homework section 5): untested.
5. **Whole-closure loading for the corpus** (section 4): needed only if the corpus moves to a true manifest load.
6. **Other engine extensions' handlers** (data quality, service, JSON, external format, Elasticsearch, data space):
   counted only when legend-lite supports those DSLs.
7. **Pure.java without signature text:** experiment 2 shows the ids match one for one, but no run has gone without
   the catalog yet. Then the corpus would get the declarations of the functions we implement from its own files.
   Settled by switching the implementation table on and rerunning the corpus and PCT the same way.
