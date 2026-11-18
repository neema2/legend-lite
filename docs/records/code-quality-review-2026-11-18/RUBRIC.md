# Code-quality review rubric: legend-lite's plan/exec separation (main @ 4dfbb5b7)

## What this review is for

The owner of this repository wants to know whether the plan/exec separation that landed on `main` (PRs #92, #95,
#101, #103, #106, #110, #111, #105, 2026-11-09 to 2026-11-15) is **expert-written, maintainable, well-designed code**.
It was written by AI agent sessions running at a low reasoning effort, and the owner suspects its quality.

**This review is NOT about correctness and NOT about whether tests pass.** They pass. The repository has a very large
layer of guard tests (ArchUnit rules, censuses, ledgers, ratchets). A guard passing proves only what that guard checks
(mostly where code lives and what it depends on). It is not evidence that the code is well designed. Do not cite a
passing guard, a javadoc claim, or a design doc as proof of quality: verify by reading the code.

The bar, in one question: **would a strong senior engineer who owns this code for years have written it this way, and
could a capable new engineer be productive in it within a week?**

## Context to read first (skim, do not get lost in it)

- `AGENTS.md` (repository root): the goals of record G1-G11, the pipeline, the layer contracts, the invariants.
- `docs/GOALS_OF_RECORD_2026_11_07.md`: the goals in full.
- Your unit's file list (given in your brief): column 1 is lines added by the series, column 2 is lines now.
  Prioritise files with many added lines; the rest are context.

Understand the real constraints (for example: the plan is portable data read by Java, TypeScript and Python hosts;
parts compile to WebAssembly/JS via TeaVM; upstream byte-fidelity with legend-engine; no Java value computation at
query time). When a choice looks odd, check whether a constraint forces it. If one does, say which, and judge whether
the code meets that constraint in the best available way. A constraint stated in prose is not proof the code honours
it well. Do not reopen the goals themselves; judge the code that implements them.

## Lenses

1. **Responsibility and cohesion.** One reason to change per class. God classes, very long methods, feature envy,
   layering leaks, classes that are bags of unrelated statics.
2. **Type design.** Illegal states unrepresentable. Sum types vs records with many nullable fields whose valid
   combinations live in prose. Stringly-typed dispatch (type names, kinds, modes, arms passed as `String` and matched
   with `case "..."` or `equals`). Boolean flag parameters. Primitive obsession. Invariants enforced by types vs by
   runtime checks vs only in comments.
3. **Duplication, textual and structural.** Copy-paste, and more importantly: parallel hierarchies, near-identical
   code paths for two roads (product vs test), the same concept implemented in several places (codecs, parsers,
   runners, decoders), the same logic hand-re-implemented in Java, Python and TypeScript where it could be shared or
   generated.
4. **Abstraction fitness.** Is each abstraction the simplest that meets the goals? Over-engineering (indirection with
   one implementation, layers that only forward, wrappers of wrappers) and under-engineering (giant switch ladders
   that want a table or polymorphism). Accidental complexity.
5. **Naming and idiom.** Names say what a thing is or does. Consistent vocabulary. No process history in identifiers
   (audit round numbers, ticket codes, dates). Idiomatic language use: imports rather than inline fully qualified
   names, standard library where it fits, idiomatic Python and TypeScript (not Java transliterated).
6. **Comments and docs in code.** They should explain what the present code does and why. History and process
   narrative (dates, who ruled what, which program step or doc section) belongs in git history and design docs, not
   in javadoc. Flag stale or wrong comments, and the signal-to-noise ratio.
7. **Error handling.** Consistent exception types, useful messages, no swallowed errors, no catch-alls. Impossible
   states should be impossible by type rather than thrown at run time.
8. **Control flow and state.** Mutable holders (one-element arrays), static or thread-local state, hidden temporal
   coupling, deep nesting, long parameter lists, out-parameters, callbacks of callbacks.
9. **Tests.** Do they test behaviour or pin structure and text? Brittle string or allowlist assertions. Duplicated
   test helpers. Test code quality.
10. **Guard proportionality** (guards unit mainly, but anyone may comment). Does each guard protect a real invariant
    at a reasonable maintenance cost? Would a simpler mechanism (build-level module boundaries, package-private
    visibility, the type system) do the same job? Are allowlists and ledgers maintainable?
11. **Dead code, residue, half-migrations.** Leftovers of removed designs, two ways of doing the same thing,
    transitional shims that stayed.
12. **Cross-language consistency** (host units). The same concept with the same name and shape across hosts;
    idiomatic code in each language.

## Severity

- **S1 (design).** A wrong or excessive abstraction, a type model that broadly admits invalid states, a duplicated
  subsystem, a structure that makes every future change in the area expensive. Fixing it means redesigning a unit.
- **S2 (local).** A class or method that needs refactoring, a specific stringly-typed dispatch, a smell confined to a
  few files.
- **S3 (sweep).** Style, naming, comments, imports. Individually trivial: report these as **patterns with a count and
  three representative examples**, never instance by instance.

## Evidence rules

- Every finding names `path:line` (repository-relative, at `4dfbb5b7`), says what is there (a short quote or an exact
  description), why it costs a maintainer, and **what an expert would write instead** (sketch the type, signature or
  structure concretely).
- Mark each finding's confidence: **verified** (you read the code and confirmed it) or **suspected** (a lead you
  could not fully confirm).
- No vague findings ("could be cleaner"). If you cannot say what better looks like, it is not a finding.
- **Report strengths too**, with evidence. The owner wants a calibrated verdict, not a list of complaints. If an area
  is genuinely good, say so plainly.
- Rank findings by how much they cost a maintainer, most costly first.

## Verdict per unit

One of:

- **EXPERT**: would pass a demanding senior review as it stands.
- **SOLID**: good design and code, minor cleanups only.
- **MIXED**: sound design, notable quality debt.
- **WEAK**: design or quality problems that will slow every change in the area.
- **REWORK**: needs redesign.

Give a 3-5 sentence justification.

## Output

Write your report to `/tmp/claude-0/-home-user-legend-lite/e46cefdc-2e0e-59c4-80ed-3422bd6cbcd6/scratchpad/review/findings/<UNIT>.md`
in exactly this shape:

```
# <UNIT>: verdict <EXPERT|SOLID|MIXED|WEAK|REWORK>

## Summary
(at most 10 lines)

## Strengths
- ... (with path:line evidence)

## Findings
### <UNIT>-<n> [S1|S2|S3] <short title>
- Where: path:line[, path:line ...]
- What: ...
- Why it matters: ...
- Expert alternative: ...
- Confidence: verified | suspected

## Cross-unit observations
(things you noticed outside your unit, briefly)

## Measurements
(any numbers you gathered, with the command)
```

Your final message back should be a short summary (verdict, the top 5 findings by title and severity, the file path of
your report). The report file is the deliverable.

## Working rules

- **Read-only.** Do not edit, create or delete files in the repository, do not commit, do not push, do not switch
  branches. Write only under the scratchpad `review/` directory.
- Do not run Bazel builds or tests (slow and not needed). `git`, `grep`, `rg`, reading files, and small scripts are
  fine. Tools installed in the scratchpad: `jscpd` (copy-paste detection) at
  `/tmp/claude-0/-home-user-legend-lite/e46cefdc-2e0e-59c4-80ed-3422bd6cbcd6/scratchpad/cpd/node_modules/.bin/jscpd`,
  and `lizard` (function length and cyclomatic complexity for Java, TypeScript and Python) via
  `python3 -m lizard` if installed (`pip install --user lizard` if not).
- Judge the code as it stands at `4dfbb5b7` (the current checkout). You may use `git log -p` to understand intent, but
  judge the result.
- Do not spawn subagents. Do the work yourself, thoroughly.
