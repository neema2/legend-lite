# G6: the machinery around the generators

Follow runs/generators/BRIEF.md's Where and Rules (read-only, evidence for everything, OPEN when unsure). Instead
of per-generator dossiers, you document the MACHINERY. Write runs/generators/G6_machinery.md and return a 10-line
summary.

1. **Every writer and diff test.**
   - The 71 `_write_source_file` targets and 60 `_diff_test` targets (runs/generators/all.txt).
   - For each package's `write_source_files` call: the files it writes, the generator behind each, its suite, and
     whether that suite is in `//:generated` (BUILD.bazel) and/or `//:update_generated`.
   - Name every writer or diff test in no suite, and every suite in neither root target.
2. **`//:update_generated` exactly.** What `bazel run //:update_generated` runs, in what order. Which generators it
   builds (cquery its runfiles), including manual ones it pulls in (for example `//pct:ratchets`). Whether one run
   reaches a fixed point: does any generator read another generator's committed output instead of its build
   output? Trace the spec chain, gen_engine_handlers and the prelude's Claims step.
3. **`//:generated` exactly.** Every diff test it groups. What builds when it runs, and so what `//gates:local` and
   the CI checks lane pay on an engine edit.
4. **The bump** (tools/bump/Bump.java, line by line). Every step, every bazel command, which writers it reaches, and
   what it assumes about a fixed point. Which generators truly depend on the upstream release (release.MODULE.bazel,
   @legend_engine_src, @legend_pure_src, @maven_upstream, @maven_runner, @oracle_pins)?
5. **Humans.** Every doc, README, error message and diff-test failure message that tells a person to run a writer
   or generator. Quote and cite each. Which are stale?
6. **What runs where today.** For each CI lane and `//gates:local`: which generators execute (built, not just
   analysed). Use the lane targets and cquery/aquery.
7. **The diff tests' own behaviour:** what a failure says, and whether it names the right command to fix it.
8. **Problems,** with evidence: writers outside suites, a suite in neither root, a manual target pulled into the
   build, a missing fixed point, stale instructions, writers that would re-bless a test's expected results.
