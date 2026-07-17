# B2 — the platform Result / execute() value

Status: DESIGN (audit 19d finding B2 — the deepest harness compensation).
Every passing corpus test currently rides `TestBody`'s host-side envelope
machinery (~400 lines: `ExecHandle`, the `.values` peels, `substitute`'s
splice rules). Three audits of scar tissue (9, 16-F1, the `at(k>0)` guard)
came from host-encoding envelope semantics the platform should own.

## Ground truth

Real pure (platform_dsl_mapping/result.pure):

```
Class meta::pure::mapping::Result<T|m> { values:T[m]; activities:Activity[*]; }
```

`execute(|query, mapping, runtime, extensions)` returns `Result` and runs
EAGERLY; the engine's interpreter then navigates the in-memory value.

## The constraint that shapes everything

**legend-lite has no interpreter, by design** (tenet #1: Java orchestrates,
the database executes). A materialized host-side object graph for `Result`
would be an interpreter by stealth. Therefore:

> `Result` is a TYPING surface plus an ORCHESTRATION handle.
> Reads over it are REWRITTEN into SQL-bound typed queries — the same
> splice semantics `TestBody` performs today, moved into the platform's
> statement orchestrator, where they are typed instead of textual.

Rejected alternatives:
- Host object-graph Result (interpreter-by-stealth; breaks the tenet).
- Keeping the harness splice alongside a platform one (two substitution
  mechanisms — the exact failure mode B4 just removed).

## Design

### G surface (B2a)
- Native class `meta::pure::mapping::Result<T|m>` with `values:T[m]`,
  plus nominal `Activity`.
- Native `execute` overloads (context params `Any` per the from()-convention):
  `execute<T|m>(f:Function<{->T[m]}>[1], mapping:Any[1], runtime:Any[1],
  extensions:Any[*]):Result<T|m>[1]` (+ the debug-context arity).
- Result binds `T|m` from the query lambda, so `$r.values` types `T[m]`
  and downstream property/relation ops type NATURALLY — the pre-typing
  rewrites in the harness stop being necessary for G.
- The corpus's own `execute` definitions (plan-generation internals) are
  suppressed via `isPlatformOwnedFunction`, exactly like toSQLString/toDDL.

### K orchestration (B2b) — the splice moves platform-side
`StatementExecutor`'s statement loop gains a RESULT FRAME:
- `TypedLet(r, TypedNativeCall(execute, [lambda, mapping, runtime, ext]))`
  → EAGER run (engine parity; audit 16 F1): inline → resolve against the
  EXPLICIT mapping argument (the toSQLString arm's exact shape) → lower →
  execute; the frame stores the TYPED query, the plan, and the
  `ExecutionResult`.
- Downstream statements pass through a TYPED rewriter before their own
  resolve/lower: reads over the frame splice per the rules TestBody owns
  today — these rules move VERBATIM, they are the audited semantics:
  * relation-rooted query: `.values` = the ONE-TDS envelope; `->at(0)` /
    `->toOne()` over it collapse; `at(k>0)` stays loud.
  * class/scalar root: `.values` IS the collection; `at`/`toOne` are REAL
    selections spliced over the chain (slice BEFORE the implicit
    serialize — this is what cures the `TypedSerializeGraph`-in-scalar
    bucket B7's honesty trade surfaced).
  * `->sqlRemoveFormatting()` / `.sql` = the stored plan's SQL text (the
    harness's advisory-compare POLICY stays in the harness; the platform
    merely answers the question honestly).
- The frame is the platform `ExecHandle`; `relationRooted` becomes a typed
  fact (the resolved root's shape), not a probe.

### Harness after (B2c)
`TestBody` keeps ONLY class-A orchestration: statement sequencing
delegation, assert interception + comparators, outcome scoring, the
emptiness/seed-failure guards. `ExecHandle`, `splice`, `toHandle`, the
`.values` substitution arms, `relationRooted`, `envelopeIndexError` are
DELETED as each shape moves.

## Migration (each stage full-sweep gated; no dual-path shapes)
1. **B2a** — G surface only. TestBody's interception fires before typing
   matters, so the sweep must hold EXACT. Immediate payoff: `execute`
   inside HELPER bodies (B1-expanded) and non-intercepted positions types.
2. **B2b** — the result frame + typed splice, shape by shape (let-bound
   `.values` first, then `at`/`toOne`, then scalar tails). Each shape's
   TestBody arm is deleted IN THE SAME COMMIT its platform twin lands —
   never two owners for one shape.
3. **B2c** — delete the residual machinery; move the `TestBody`
   5-way seam split if file sizes demand.

## What it unlocks
- The `scalar lowering not yet implemented for TypedSerializeGraph`
  bucket (8+ tests incl. the B7 trio).
- The 11 `execute() whose query argument is not a lambda` SHAPEs.
- Honest typing for the whole `$r.values` read surface (no silent
  envelope-shape decisions in the harness).
