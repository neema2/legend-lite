# Generator dossiers: the brief

## Why
The user will decide, generator by generator, what runs in the build, what is manual, what belongs to the
upstream-bump procedure, and what is really a test. Before any of that, every generator must be understood
completely: what it does and why, everything it reads and writes, who uses its output, and what should make it run.
No change is made until this is done. Be exhaustive and exact. A missing or wrong fact here becomes a wrong
decision later.

## Where
- The repo (read it, never change it): the build/rebuild checkout, branch build/rebuild.
- Your generators: runs/generators/G<n>.txt (one label per line). Every one gets a full dossier.
- Context:
  - the design: docs/ (plan branch) BUILD_REBUILD_DESIGN_2026_10_05.md;
  - the earlier per-area audits: docs/ (plan branch) build-inventory/area*_report.md
    (they are a starting point; verify, don't copy);
  - the plan and log beside it (BAZEL_FIRST_CLASS_WORKPLAN_2026_10_03.md, BAZEL_EXECUTION_LOG.md);
  - git history (`git -C <repo> log --follow`, `git log -S`).
- The CI lanes: .github/workflows/gates-run.yml (the lane matrix; the build lane runs `bazel build //...`). The
  local gate: gates/BUILD.bazel. The upstream bump: tools/bump/Bump.java. The root: BUILD.bazel (//:generated,
  //:update_generated).

## Rules
- Read-only. Never edit, commit, or run `bazel build`, `bazel test` or `bazel run`. You MAY run `bazel query`,
  `bazel cquery` and `bazel aquery` (always `env -C <repo> bazel ...`, never `cd`), git, grep, and read any file.
- Read the generator's PROGRAM: the Java main class, Python script or JS file, and what it calls. Its declared
  inputs (BUILD, aquery) are only half the story. What it actually opens and reads is the other half.
- Every claim cites evidence: `path:line`, a query you ran (give it), or a commit. Write OPEN with what would settle
  it, never a guess.
- Write runs/generators/G<n>_dossiers.md. Return to the caller only a 10-line summary.

## The dossier: one per generator, every field
1. **Identity:** label, rule kind, `BUILD file:line`, the program (main class or script, `path:line` of its entry).
2. **What it computes,** in plain words: a short paragraph a newcomer understands.
3. **Why it exists:** the commit or plan item that introduced it, and what it protects or feeds. Say if the reason
   is gone.
4. **Inputs, declared:** grouped (our sources and which; upstream archives; Maven pools; other generators'
   outputs; tools), with counts. Use `bazel aquery` for the real action inputs, and say how many core jars it takes.
5. **Inputs, actually read:** what the program opens (files, directories, system properties, environment), from the
   source. Name every over-declared input (declared, never read) and under-declared one (read, not declared).
6. **Outputs:** each file, with what it contains in one line.
7. **Committed?** If so, where in the tree, its writer target, its diff test, and which suite (`//:generated`,
   `//:update_generated`, another) includes them. If not, who consumes it as a build output.
8. **Who consumes it:** tests (name them), other generators, product code at run time (search the product's
   sources and resources for the file name), docs or humans (READMEs, GATES.md, error messages that tell someone to
   run it).
9. **Determinism:** same inputs, same bytes? Evidence: a comment, the code (time, randomness, hash order, absolute
   paths), or the plan's records.
10. **Cost:** memory and resource settings, whether it starts a database, server or JVM, how much it computes (by
    what it does; no timing runs).
11. **What reruns it today:** which source changes invalidate it (its closure: core libraries, upstream, the spec, the
    corpus, test trees). Show one `bazel query 'somepath(...)'` for each surprising one.
12. **What SHOULD rerun it (the true trigger):** an upstream bump, a change to which of our files, a human request,
    or "engine behaviour" (which makes it a test).
13. **Who runs it today:** is it built by `bazel build //...` (not manual, or pulled in by something that is not)? By
    `//gates:local` (through which suite)? By which CI lane? By the bump (tools/bump/Bump.java)? By hand, and which
    doc says so?
14. **Recommendation,** with its reason. Use exactly one of:
    - BUILD-OUTPUT: not committed; something in the build or a test consumes it. Say whether it should be testonly.
    - COMMITTED-UPSTREAM: committed; its true trigger is an upstream bump (plus hand edits to its own spliced file).
      It belongs to the bump's update.
    - COMMITTED-SOURCE: committed; its true trigger is a change to named files of ours. Its diff test belongs where
      those files' changes are checked.
    - TEST-IN-DISGUISE: it judges engine behaviour (a verdict, a golden, a check that fails), so it should be a test.
    - DRAFT-MANUAL: a human runs it on purpose; its output is a draft or a report.
    - DEAD: nothing uses it (prove it).

    Then say: should the generator be `manual`? Should its writer be in `//:update_generated`, in a bump-only update,
    or neither? Should its diff test run in the everyday gate, the bump's check, or its own lane? And what dependency
    narrowing would make it rerun only on its true trigger?
15. **Open questions,** each with what would settle it.

End with a table of all your generators: label | recommendation | manual? | update group | true trigger.
