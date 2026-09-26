# Research line 5: the package cycles, re-measured on HEAD (origin/main 6e99cef11, 2026-09-26)

Tools: the 2026-09-22 `package-graph.py` lives only on branch `audit/standard-build-program`
(e8c7bf83d); recovered and extended with a class-level tool that includes same-package simple-name
users of a moved class (which the package tool cannot see by construction): `class-edges.py` (emits
`edges.tsv`, 7,312 class edges) and `simulate.py` (`--move`, `--cut`). `simulate.py` reproduces
`package-graph.py` on the baseline (4/208, 2/130 vs 4/209, 2/132; delta = package-info.java).
Copies: study directory `receipts/plan-audit-2026-09-26/cycles/`.

## 1. Measured numbers
| | today | + Nullable/NonNull → annot | + 5 more moves + 2 one-line cuts |
|---|---|---|---|
| files / packages | 722 / 33 | 722 / 34 | 722 / 35 |
| package edges | 233 (175 carried only by inline FQNs, 4,867 sites) | 236 | — |
| build units (SCC-collapsed) | **8** | 28 | **33 = every package** |
| largest cycle | **26 pkgs / 694 files (96%)** | 4 pkgs / 209 (compiler, .element, .spec, .spec.typed) | none across parents |
| other cycles | — | lowering↔resolver 132; protocol↔protocol.spec 49; parser↔parser.section 44 | only the 2 sanctioned parent/child pairs |
| median rebuild set | 6 (715 for the blob) | 219 | 158 |

`core/src/main/java/com/legend/Nullable.java` and `NonNull.java` still exist in HEAD. The move was
executed and validated on `experiment/move-nullable` (c4f0f3f39, 382 files, 4,467 tests green,
destination `com.legend.base`) but never landed. What DID land: `tools/untangle/move_classes.py
--group A` and `tools/untangle/groups.txt` (`A: com.legend.base <- com.legend.Nullable
com.legend.NonNull --inline`) — a one-command, rebase-safe re-run. Reference surface today: 2,895
`com.legend.Nullable|NonNull` sites in 387 files across core/spec/pct/parser-equivalence/wasm/testing;
`core/BUILD.bazel:42-43` (NullAway `CustomNullableAnnotations`/`CustomNonnullAnnotations` — the only
BUILD carrier), `ArchitectureTest.java:273-276` (`NULLNESS_ANNOTATIONS`), 4 root-package files using
bare `@Nullable`.

Delta since 2026-09-22: the residual compiler cycle is 4 packages, not 5 — resolver/lowering have
fallen out (they form their own 2-package cycle carried by one class).

## 2. core/BUILD.bazel
- 7 `java_library` (`core`, `drivers`, `duckdb_load`, `core_tests_lib`, `shadow_binding`, `core_next`,
  `core_next_prelude`); `core` is ONE target, `glob(["src/main/java/**/*.java"])`. Lexer/parser/
  protocol/model are NOT separate targets.
- Tests: `core_tests_lib` one library over all 286 test files; 4 non-manual `junit_test` lanes
  (`core_tests`, `guardrails`, `census`, `stress_suites`) + 6 `manual` scale lanes. spec 3, pct 4,
  parser-equivalence 2.
- Consumers of `//core`: spec (`//core`, `:srcs`, `:main_java`, `:core_next`, `:shadow_binding`,
  `:drivers`), pct, parser-equivalence, wasm, tools/engine-runner, tools/deps. Outside core the
  imports are mostly front-end (model 43, parser 13, protocol 18, lexer 9) and builtin/platform;
  only 4+3 imports of compiler.spec/.typed.

## 3. Back-edges (exact class pairs)
**(b) Front end = lexer + parser + parser.section + protocol + protocol.spec + model + values +
error + spi: zero references into compiler/platform/builtin/lowering/resolver/normalizer/exec/sql/
root.** Outward edges are only:

| from → to | class pairs | sites |
|---|---|---|
| protocol → com.legend.Nullable | 9 | 356 (Protocol.java 321) |
| model → Nullable | 31 | 182 |
| protocol.spec → Nullable | 29 | 95 |
| parser → Nullable | 10 | 78 |
| parser → spi.{SectionSource,ElementSink,SectionGrammar} | 6 | 17 |
| parser.section → spi | 13 | 16 |
| error/parser.section/values/lexer → Nullable | 4/3/2/1 | 17/7/4/3 |

`protocol.spec.AppliedFunction` references nothing in compiler; `model/` references neither
`platform.FunctionId` nor `builtin.Pure`; `spi` has no outgoing com.legend edges. After the
annotation move the front end is a legal Bazel target TODAY (~178 files: 49 protocol, 44 parser, 61
model, 5 lexer, 8 error, 3 values, 3 spi, +2 annot). ArchitectureTest already pins it (7a lexer
JDK-only, 7b protocol bottom, 7c parser allowlist, 6j model pure data, 6g leaves).

**(c) The compiler/typer cycle** (compiler 12, compiler.element 24, compiler.spec 82,
compiler.spec.typed 91) minority-direction edges (top 20 by sites; majority direction is spec→typed
426 pairs/1,937 sites, spec→element 48/184, spec→element.type 172/2,010):

| back-edge | sites |
|---|---|
| compiler.StatementInline → compiler.spec.SourceSubst | 11 |
| compiler.element.PureModelContext → compiler.ModelBuilder | 8 |
| compiler.element.ModelIntegrity → compiler.ModelBuilder | 8 |
| compiler.spec.Typer → compiler.ResolvedNames | 5 |
| compiler.spec.FoldChecker → compiler.ResolvedNames | 4 |
| compiler.element.FunctionCompiler → compiler.ModelBuilder | 3 |
| compiler.element.ClassCompiler → compiler.SynthFqn | 3 |
| compiler.StatementInline → compiler.element.ModelContext | 3 |
| compiler.LiteralMapUnroll → compiler.spec.SourceSubst | 3 |
| compiler.spec.typed.ContextReading → compiler.ResolvedNames | 3 |
| compiler.element.Temporal → compiler.spec.typed.TypedSpec | 2 |
| compiler.element.TypeClassifier → compiler.ModelBuilder | 2 |
| compiler.element.ModelIntegrity → compiler.SynthFqn | 2 |
| compiler.spec.{StaticFold,IfChecker,GroupLambdaAggs} → compiler.ResolvedNames | 2 each |
| compiler.spec.typed.{TypedViewRelation,TypedNativeCall} → compiler.element.TypedFunction | 2 each |
| compiler.element.Temporal → compiler.spec.typed.TypedGetAll | 1 |
| compiler.element.TypeClassifier → compiler.spec.InferenceKernel (`ENUM_METACLASS_FQN`, TypeClassifier.java:47) | 1 |
| compiler.KnowledgeLayer → compiler.element.RelationalTypeInference.infer (KnowledgeLayer.java:406) | 1 |
| compiler.ModelBuilder → compiler.element.StoreLookups | 1 |
| compiler.element.{PureModelContext,ModelContext} → compiler.NameResolver | 1 each |

`com.legend.compiler` is mixed-height: its LOW half (NameResolver 306 model refs, ResolvedNames,
BareNames, SynthFqn, SymbolTable, DerivedProps, RelationalKinds, TableIndex, ModelBuilder — used by
normalizer 100+ sites and by element) is the binder and belongs below element/spec; its HIGH members
(StatementInline, LiteralMapUnroll — used only by `Compiler`) reach into spec. **Lowering↔resolver**
is carried by ONE class: `lowering.SnapshotEnvelope → resolver.AsorRef.{SEG_LEN_WIDTH,MARKER}` (two
compile-time constants, SnapshotEnvelope.java:134,140 — invisible to ArchUnit/jdeps because javac
inlines them; Bazel strict deps would see it). AsorRef has zero com.legend out-edges.

**Validated cut set (class-level incl. same-package users): 7 moves + 2 one-line cuts → 33 units,
zero cross-parent cycles.** Moves: Nullable, NonNull → base; resolver.AsorRef → lowering;
compiler.element.StoreLookups → compiler (deps: model only); compiler.StatementInline,
compiler.LiteralMapUnroll → new `compiler.inline` (above spec; only `Compiler` uses them);
compiler.element.Temporal → compiler.spec. Cuts: hoist `ENUM_METACLASS_FQN` out of InferenceKernel;
replace `RelationalTypeInference.infer` in KnowledgeLayer.java:406. REFUTED by the same-package check:
moving TypeClassifier up (22 same-package users) and moving KnowledgeLayer up (ModelBuilder.knowledge()
constructs it → 5-package cycle).

## 4. Timing answer
**A1 as worded in the homework would create the edge protocol.spec → platform, and platform depends
back**: platform → model (11 pairs/33 sites: DeclarationTable/ImplementationTable → model.Function;
`FunctionId.of(Function)` → model.SignatureMangle) and platform → builtin (4/7) → parser (Pure/
SystemMetamodel/Prelude → ElementParser/Dialect, 10 sites); model → protocol.spec (27 sites). Result: a
NEW 7-package/171-file cycle {protocol, protocol.spec, model, platform, builtin, parser, parser.section}
and three ENFORCED tests red (`protocolIsTheBottomLayer` 7b, `packageDependenciesAreAcyclic`,
and after the annotation move the parser target itself). Contradicts COMPILER_DESIGN §3.2 and §3.3.

**The binder produces its own artifact.** (i) a Bound tree package (`com.legend.bind`, later the
renamed low half of `com.legend.compiler`) above protocol+model+platform and below compiler.spec; or
(ii) the low-churn seed for A1: an immutable per-compilation `Bindings` table (identity-keyed: syntax
call node → `List<FunctionId>`, later variable → symbol) produced by NameResolver and read by the
typer's `candidatesOf`/`FunctionCompiler.functionsAt`, with `candidateFqns` deleted from
AppliedFunction in the same commit. (ii) grows into (i) at step G. Do NOT strip `FunctionId` to a
model-free record in `values` to carry it on the syntax node.

**The annotation move: NOW, before A1**, in its own commit, at a quiet moment, with the NullAway gate
re-proven by injecting a violation. Settle the name (groups.txt says `com.legend.base`). Use
`--inline` (keeps line counts for scalar pins).

**Recommended sequence**
- NOW (~1 day): (1) `--group A` + BUILD flags + `NULLNESS_ANNOTATIONS` + 4 bare root users; (2)
  AsorRef and StoreLookups moves as groups B/C; (3) split `//core` into: `annot`, `values`, `error`,
  `spi`, `lexer`, `protocol` (+spec), `model`, `parser` (+section), `builtin`, `platform`,
  `element.type`, ONE `compiler_mid` for {compiler, element, spec, spec.typed} + `lowering`, `resolver`,
  `sql`, `sql.dialect`, `exec`, `normalizer`, `plan`, `lineage`, root, `server`, `ide`, `cache`, `probe`.
  8 → ~25 units. Do NOT touch Typer/InferenceKernel/NameResolver/FunctionCompiler/BareNames/
  StatementInline placement now.
- At step G: the 3 remaining moves + 2 cuts, then split `compiler_mid` into bind / element / typer /
  typed-HIR targets. Re-run simulate.py on that tree first.
- NEVER: the full per-depth re-layering (284 of 679 classes, 166 packages); never a `resideInAPackage`
  exemption or a "temporary" back-edge to get a step green.

## 5. Test isolation (286 test files)
| reaches at most | files | notes |
|---|---|---|
| front end only | ~24 behaviour files (+ ~10 root-package parser/protocol tests) | `//core:parser_tests` cached against the front-end target |
| typer or below | 21 | `//core:typer_tests` once compiler_mid exists |
| full pipeline (Compiler.execute / JDBC) | ~207 (integration 65, resolver 35, lowering 20, exec 19, …) | stay in `core_tests` |
| source checks (guardrail/census) | ~20 | already their own lanes |

~45 of 286 (~16%) become per-target; 84% execute end to end. The compile-side win is asymmetric:
front-end edits still recompile ~560–670 files; the payoff is for edits ABOVE the front end —
lowering 206, sql.dialect 100, resolver 95, exec 73, normalizer 65, root 36, server 7 files instead
of 722 — exactly where A–F work happens. wasm/planner builds stop carrying exec/server.

## Risks
- `datacube/dual-plane` (125 commits ahead) touches 32 core main files; `bazel/e2e` 9. The
  annotation move is `--inline` and re-runnable: rebase recipe = "drop the move commit, re-run
  `--group A`". Coordinate only AsorRef (resolver) with the DataCube branch.
- The class-level graph is regex-based (90% recall/93% precision vs jdeps on 2026-09-22); javac is
  the authority — a move can still fail on package-private access.
- Bazel strict deps will surface the constant-inlined edges (AsorRef, ENUM_METACLASS_FQN); expect
  those two when `compiler_mid`/`lowering`/`resolver` become targets.
