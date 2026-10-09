# D24, "one substitution engine first": the homework (2026-10-08)

The compiler plan's Now line (`docs/EXECUTION_PLAN_2026_09_26.md` §0, D24 in §2): after W0.8, the cleanup phase,
"one substitution engine (six today)" first, self-contained in `compiler/spec`; the budget is fourteen sessions for the
whole cleanup; every cleanup push ends net-negative on product lines and changes no SQL. This note is the homework before
code (rule 0b.1): what the six are, from the code, what "one" can mean, the probe, and the first push.

## 1. The engines, from the code

Substitution of a variable by a term, with binders handled, exists in seven places over two trees.

| engine | tree | lines | what it does | binder policy | callers |
|---|---|---|---|---|---|
| `compiler/spec/SourceSubst` | syntax (`ValueSpecification`) | 487 | `substitute(v, env)`, `inlineLets` (a multi-statement lambda folded), `freeVars`; capture-avoiding since W0.6 push 1 | a binder renamed only on a real hazard, to `b_<k>`, the smallest `k` free nowhere: deterministic, no state | 16 files (the checkers, `Overloads`, `SpecCompiler`, `StaticFold`, the front door) |
| `compiler/spec/AlphaRename` | syntax | 56 | renames EVERY lambda binder of an inlined helper body to `_nr<N>` before the typer re-types it (the must-inline path, `Overloads.java:438`) | all binders, a per-typer counter | `Overloads` (and `StaticFold` through `typer.alphaRename`) |
| `compiler/StatementInline` | syntax, statements | 293 | statement-level β of program calls at the front door (`Compiler.resolveQuery`, `:570`); substitutes through `SourceSubst` | every callee let renamed to `_s<N>_<name>` | `Compiler.resolveQuery` |
| `compiler/LiteralMapUnroll` | syntax, statements | 92 | a statement-root `map` over spelled variables unrolled, one copy per element (`Compiler.java:580`); substitutes through `SourceSubst` | each element's lets renamed `<name>_<i>` | `Compiler.resolveQuery` |
| `compiler/spec/StaticFold.inlineUserCall` | syntax | (845 in all) | β-inlines a bodied callee inside a normalize-required body before folding (`:225-266`): `SourceSubst.substitute(typer.alphaRename(body), subst)` | AlphaRename's | `StaticFold.fold` |
| `compiler/spec/UserCallInliner` | typed (`TypedSpec`) | 1,625 | G½: every `TypedUserCall` β-reduced, callee lets reduce forward, static `match` dispatched, `eval` of a literal lambda reduced; its OWN capture machinery (`captureRisk` stack, `underSubst`/`underNames`/`pushNames`/`popRisk`, `bind`, `namesIn`: `:1474-1560`), plus the re-unification, `redispatch` and `resolveStamps` W3.4 replaces (`:389-560`) | a binder renamed only when a term substituted beneath it mentions its name, to `_i<N>` with a counter (`:1530`); the capture set is the names an argument list MENTIONS, free or bound (`namesIn`), a conservative over-approximation | `TypedQuery.lower`, the executor |
| `compiler/spec/typed/TypedSubst` | typed | 293 | `apply(body, env)`: capture-avoiding substitution with exact free variables (`FreeVars`), W0.6 push 1 | a binder renamed only on a real hazard, to `b_<k>`: deterministic, no state | `lowering/MatchFold`, `resolver/StoreResolver` |

Beside them, `resolver/Substitution` (3,479 lines) is H's rewriting of a user lambda over class instances into the
mapping pipeline's row (`RowScope`: `userVar` to `freshRowVar`): a different job (binding a variable to a row, not a
term to a term) that W4.3 owns; D24 names it only as the third re-derivation of mapping elaboration.

The typed tree binds variables by NAME (`TypedVariable(String name)`, `TypedLambda(List<String> parameters)`,
`TypedLet(String name)`); W0.6 push 2 put the Barendregt convention at the lowering and resolution boundaries, with
`VarUse`, `FreeVars` and `Lets` as the one occurrence probe, the one free-variable function and the one let lookup.
"Unique variable ids on the typed tree" (D24's first item, W2.5's `VarId`) is what would make every capture check
unnecessary; the Now line keeps it for the middle because thirty of its thirty-five touched files are the store
resolver's.

## 2. What "one engine" can mean

Two trees, two engines, one policy: over syntax `SourceSubst` is already the one engine (every other syntax-level site
calls it; `AlphaRename` is a policy on top of it; `StatementInline` and `LiteralMapUnroll` are statement splicers that
call it). Over the typed tree there are two: `TypedSubst` (push 1's, deterministic, exact) and `UserCallInliner`'s own
(a counter, a conservative capture set). "One substitution engine first" therefore means, concretely:

1. **The typed tree substitutes through `TypedSubst` only.** `UserCallInliner` keeps what is its own — the call stack
   and the recursion wall, lets reducing forward, static `match` dispatch, `eval` of a literal lambda, the hook, the
   query-level lets left alone — and loses its capture machinery: the `captureRisk` stack, `underSubst`, `underNames`,
   `pushNames`, `popRisk`, `pushRisk`, `bind`, `namesIn`, the `fresh` counter and the `_i<N>` namespace scan at
   `:158-218` (about 150 lines). A β-step becomes `TypedSubst.apply(calleeBody, params -> args)`; a let reduction
   becomes `TypedSubst.apply(rest, name -> value)`. The renaming policy becomes push 1's deterministic `b_<k>`.
2. **The syntax level's three policies become one**: `AlphaRename`'s rename-everything (`_nr<N>`) is a stronger rule
   than hygiene needs; `StatementInline`'s `_s<N>_` and `LiteralMapUnroll`'s `<name>_<i>` exist so that single-assignment
   scopes do not collide when statements are spliced, which `SourceSubst`'s hazard rule already guarantees for reads
   but not for re-binding. Whether a spliced let may keep its name when nothing later rebinds it is a probe question
   (below); if yes, the three policies collapse into `SourceSubst.bind`'s. This slice is smaller and later: the
   must-inline path that `AlphaRename` serves is deleted by W3.4/W4.2 anyway ("source-level β-expansion during typing
   ends"), so only `StatementInline` and `LiteralMapUnroll` would move.

So the first push is (1). It is self-contained in `compiler/spec` (as the Now line says), net-negative, and it gives
the middle one substitution semantics over the typed tree before W3.4 and W4.2 build on it.

## 3. The probe, before the switch (rule 0b.2)

Binder names reach two surfaces: the plan's printed lambdas (`functionParameters = [optionalID:String[0..1]]`, the
inliner's own comment at `:1521-1526`) and SQL lambda text where a lambda is rendered. A policy change from `_i<N>`
to `b_<k>` can therefore move bytes. The probe, with no code change:

- count, over the corpus's committed results and the stress corpus's plans, every binder name an inlining renamed
  (`_i[0-9]+` in plan text and SQL text): the committed `spec/src/test/resources` hold none today (grep, 2026-10-08),
  so the question is the live surfaces: run the render census (`tools/census`, the execution census of every statement
  the lanes send, and the render census) at base and at head of the switch and diff;
- count the inliner's renames per corpus run (a counter printed once, under a local-only switch) against `TypedSubst`'s
  on the same trees: how often the conservative `namesIn` set renames where the exact `FreeVars` set would not.

**The probe's first receipt (2026-10-08, a throwaway branch, never landed):** counters in `UserCallInliner.bind` and in
`//core:compile_latency`'s render step, over the stress corpus's 4,736 service tests (4,723 planned): the inliner renamed
**0** binders (the capture hazard never fired), **0** rendered SQL texts contain an `_i<N>` name; 48 contain some
`<name>_<k>` identifier, which the loose pattern cannot tell from SQL aliases such as `t_1`. On this population the
switch to `TypedSubst` moves no name. The relational corpus (the corpus lanes) is the other population; the same two
counters run there in the push itself before the switch.

If the diff is empty, the switch is a pure deletion. If names move only where no SQL moves (plan surfaces), the GATES
entry says so and the affected goldens are re-blessed once with the policy named. If SQL text moves, the slice stops
and the policy is decided first (a deterministic name is the better one: the same input gives the same bytes).

## 3b. The shape of the switch, from reading the inliner (2026-10-08, evening)

`UserCallInliner.rewrite(node, env)` walks the typed tree carrying the substitution (`env`: a name to its typed term)
and reduces as it goes: a `TypedUserCall` is opened (`inlineCall`, the callee's parameters bound to the rewritten
arguments, its lets reduced forward in `reduceStatements`), a static `match` is dispatched (`dispatchArm`), an `eval` of
a literal lambda is β-reduced (`reduceEval`), query-level lets substitute into the rest (`inlineBody`). The capture
machinery exists because substitution and reduction are interleaved: every binder met under a non-empty `env`
(`lambda`, the lets inside it, the match arms) asks `bind` whether a term substituted beneath it mentions its name and
renames it `_i<N>` from a counter that first scans the query for user-written `_i` names (`reserveFreshNames`,
`bumpPast`); the hazard set is the names the arguments MENTION (`namesIn`), pushed and popped around every β site
(`pushRisk`, `underSubst`, `underNames`, `pushNames`, `popRisk`).

The switch is "substitute, then reduce". At each β site the substitution happens once, up front, through
`TypedSubst.apply(body, env)` (exact free variables, a binder renamed only on a real hazard, the deterministic
`b_<k>` names), and the walk continues over the substituted tree with no environment: `rewrite(node)` keeps the
reductions (open a call, dispatch a match, reduce an eval, the hook, the recursion wall, the unroll budget, the
`bound` bookkeeping the hook's shadow guard reads) and loses every `env` parameter. Deleted: `captureRisk`,
`pushRisk`, `pushNames`, `popRisk`, `underSubst`, `underNames`, `bind`, `namesIn`, `fresh`, `reserveFreshNames`,
`bumpPast`, and the `env` arms of `lambda` and `TypedLet` (about 200 lines). Two consequences to judge, not assume:
a binder that today is renamed only on the conservative hazard (a name mentioned anywhere in an argument) is renamed
only on the exact one, so some `_i<N>` names in plan surfaces may become their source names; and the names that do
change spell `b_<k>` instead of `_i<N>`. The probe over the stress corpus saw no rename at all (§3); the render census
before and after, with the scope ids checked, is the judge for the corpus lanes.

## 4. The first push, as a slice

- **Homework** (this note) and the probe's receipt in GATES.
- **Switch**: `UserCallInliner` over `TypedSubst`; the capture machinery deleted; the inliner's unit tests and
  `InlinerMatchCaptureTest` (push 1's permanent correctness test) unchanged.
- **Gates** (D24's): the corpus rosters LOST 0 by the roster files; the wrong-rows tool's rows equal to the engine's
  on the seeds (`plan-audit-2026-09-26/wrongrows/`); the reference lane untouched (G½ is after typing); the render
  census diff empty or explained; the local gate; one CI run.
- **Number**: product lines in `core/src/main` down by the deleted machinery (rule 0b.17); the inliner below 1,500
  lines.
- **Size**: one to two sessions. The syntax-level policies (§2, item 2) are a second slice if the budget allows, after
  the probe on spliced lets; W3.4 (recorded instantiations replace re-unification) and W4.2 (one G½) follow in Phase 3
  on the one engine.

## 5. What stays out

`resolver/Substitution` (W4.3's), `StaticFold`'s folding (D8's evaluator, W4.2), the must-inline path's re-typing
(W3.4), unique variable ids (with the middle, per the Now line).
