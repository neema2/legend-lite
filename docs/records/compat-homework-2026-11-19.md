# The compatibility homework spike (2026-11-19)

The user's go, 2026-11-19 (~03:05 UTC): "start the homework spike". Its purpose: decide, with evidence, whether the
compatibility architecture below is tractable, BEFORE the presentation program's remaining slices (B1, paused after
S1 on `claude/presentation-b1`) are re-cut on it. The orchestrator is session `session_0195rQEoRb1AGCHxtZChjWsg`.

## 1. The architecture under test

The product is one meaning with one output form. Compatibility is translation at two boundaries, never a parameter
of the core.

```
upstream request -> INPUT ADAPTER -> lite program -> [compiler -> planner -> plan -> exec] -> typed result -> OUTPUT PRESENTER -> upstream bytes
                    (rewrite or refuse)               one meaning, no modes, no hooks                         (spell it out)
```

1. **The core knows nothing about upstream:** no modes, no style objects, no two-form hooks or facets in the compiler,
   planner, plan or exec.
2. **The plan is SQL plus typed data.** An error site is its kind, its location and its structured literal operands;
   never words (the user's A3: no run-time operand values in messages).
3. **Presentation** is a pure function from typed results and error records to bytes, after exec, in the host. Never
   built in SQL, never stored in the plan. One presenter per audience: lite, legend-engine, legend-pure.
4. **Upstream's SQL text and internal shapes are never promised**, only answers and envelopes. Code that reproduces
   upstream's SQL shapes is deleted, unless it changes an answer, in which case it is a meaning question (rule 5).
5. **Meaning differences** are input rewrites: the input adapter rewrites an upstream program into the lite program
   that computes upstream's answer. Where no exact rewrite exists, it refuses by name, and the case is recorded.
   Upstream's bugs are never copied.
6. **Some compatibility cannot happen after results** and must be an input rewrite: text built inside a query and
   then used (`filter($x.price->toString() == '2.5E7')`, a group key from `toString`, `joinStrings` over Floats),
   meaning differences that change values or rows, and upstream's planning refusals. For these the core may need a
   small set of **explicitly named capabilities** (for example "print a Float the way legend-pure does"), which only
   the edge's rewrite calls: no mode parameter, no hook.
7. **Accepting upstream input** (grammar, protocol, project files) stays in the product; speaking upstream's words
   does not (error kinds in the core, words in the presenter).
8. **The library holds meaning only;** upstream's messages and runner helpers are not library.
9. **Judges (the user's amendment):** the product (LITE) keeps BOTH judges, host and database, with parity.
   Compatibility judging (legend-pure's comparison rules and words, PURE and ENGINE) is host-only.
10. **Data delivery** is chosen by the target's capability; a remote target gets an upload or stream route.

## 2. The homework

Each item is one session at xhigh effort, measure-and-record (H3 builds a prototype on its own branch only). Each
writes `docs/records/compat-homework/H<n>-<topic>.md` on its own branch `claude/compat-homework-h<n>`, pushes it, and
sends the orchestrator the counts first, then the record.

| | Question | Done when |
|---|---|---|
| **H1** | Can every known upstream difference be handled by the architecture, and how? | Every known difference classed into exactly one of: **P** presenter (after results), **R0** input rewrite using what the core already has, **R1** input rewrite needing a new named core capability (name it, say what it computes, which databases), **X** refuse by name, **S** same in every audience (not a difference after all). Counts per class; every R1 listed with its capability; the five largest risks named |
| **H2** | How common is text built inside a query and then used (the R1 driver)? | Counts, per source, of expressions whose text output (`toString`, `format`, `joinStrings`, `toRepresentation`, a date's text, `plus` on strings over a non-string) feeds further computation (a filter, a sort or group key, a join key, a comparison, an aggregate), versus text that is only returned. Sources: PCT's suites (essential, grammar, standard, relation, unclassified), the relational corpus (`spec/` rcorpus), lite's own tests of real projects, and the DataCube and timeseries libraries |
| **H3** | Does the pattern work end to end? | Three prototypes on a scratch branch, each checked against upstream's own answer (the engine runner on `claude/presentation-r`, PCT's expected values or the manifests): (a) `substring`/`indexOf` as an ENGINE input rewrite over LITE's Pure meaning; (b) `toString` of a Float inside a filter, as an input rewrite calling one named capability; (c) one legend-engine envelope (a TDS result) produced by a presenter over typed results, byte-identical to the golden. For each: what it took (files, lines), whether it matched upstream, and what broke |
| **H4** | How do three host languages (Java server, TypeScript tab, Python) present upstream's bytes without drifting? | The options compared with measurements: one presenter implementation shared (Java through WASM in the tab; Python through which binding?) versus per-language presenters held to shared fixtures; today's copies counted (HOSTS-9's four store copies, the raise readers, the plan readers); a recommendation with its cost and its guard |

## 3. Sources (read; do not recall)

- `docs/SEMANTICS_REGISTER.md` on `main` (every deliberate difference, with evidence).
- The presentation design, `docs/records/presentation-program-design-2026-11-18.md` on `origin/claude/presentation-design`
  (§4 (b): the per-mode form rows; §2: where presentation happens today; §3.5: raises per mode).
- The census, `docs/records/census-b2-inventory-2026-11-19.md` on `origin/claude/census-b2` (the 142 remaining hits).
- The legend-engine oracle, `docs/records/engine-oracle-pure-v1-2026-11-18.md` on `origin/claude/presentation-r`
  (the five answer-level differences; the tooling).
- `docs/records/pr118-audit-2026-11-18/FOLLOW_UPS.md` and its `findings/` on `origin/claude/refactor-code-quality-review-9fqhug`.
- The pinned upstream sources (`@legend_engine_src`, `@legend_pure_src`; the PCT manifests under
  `legend-engine-xts-relationalStore/.../pct-manifests/`): upstream's answer is read from them or asked of the engine
  runner, never recalled (AGENTS.md's upstream-fidelity tenet).

## 4. Rules

- AGENTS.md at `main` is binding. Measure and record; H3's prototype lives on its branch and is never proposed for
  merge as is.
- No pin, ceiling, ledger or allowlist moves. No PR. Push only to your own `claude/compat-homework-h<n>` branch.
- Never read, print or copy a credential.
- Time box: about one working day each. Send early counts as soon as you have them; the orchestrator may narrow
  scope.
