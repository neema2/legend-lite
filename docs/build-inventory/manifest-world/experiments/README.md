# Manifest-world experiments (2026-10-06): the evidence

Read with `docs/MANIFEST_WORLD_EXPERIMENTS_2026_10_06.md`. Everything here is homework: scripts, probes and their
outputs. Nothing in the product was changed to produce it.

## Setup to rerun

- Write the paths of the two pinned upstream trees into `et` (legend-engine) and `pt` (legend-pure) next to the
  scripts. They are Bazel's `@legend_engine_src` and `@legend_pure_src` repositories.
- Build `//:java` and write its runtime classpath into `probe/cp2` (from `bazel cquery //:java --output=files`,
  prefixed with the execution root). Compile `probe/*.java` against it into `probe/classes` with a JDK 25.
- `JDK=<jdk 25 home> rerun.sh <repo> <this directory>` regenerates the analysis (steps 1 to 4 below).

## Scripts, in pipeline order

| Script | What it does | Output |
|---|---|---|
| `docstart.py` | element segmentation of upstream Pure files: doc strings stay with their element, keywords inside doc strings and block comments are prose, names read after stereotypes and tagged values | (library) |
| `enginepat.py` | every upstream element; which carried names lie outside legend-pure `platform*` + engine `core_functions_*`; by package, file, folder | `enginepat.txt` |
| `englist.py`, `enggroups.py` | the 269 engine-side names, why each is carried (lowered, named by our Java, closure), grouped by purpose | `englist.tsv`, `enggroups.txt` |
| `coverage.py` | what upstream core plus the user-facing engine files cover | `coverage.txt` |
| `e1.py` | experiment 1: upstream's own lists (handler registrations, classes the compiler instantiates) | `e1.txt` |
| `modworld.py` | whole-module worlds, tests stripped by upstream's markers; also writes the universe | (synth files) |
| `probe/ClosureProbe.java` | our own parser and resolver over the universe: every element and every full name it references, split declaration/body | (edges, not committed: regenerable) |
| `closure.py` | experiment 4: the closure from three starting sets, three modes | `closure.txt` |
| `probe/BootDemandProbe.java`, `bootdemand.py` | what the boot itself references (system metamodel, Pure.java's catalog) | `bootdemand.txt` |
| `probe/SystemFqns.java` | the system metamodel's own element names | `system_fqns.txt` |
| `synth_prelude.py` | a candidate world written as a `prelude.pure`, with an ownership filter | (override directories) |
| `probe/UserSideProbe.java` | experiment 5: boot, then parse, build and type-check every body of the 56 projects (one graph) and the 3 demos | `e5_user_side_summary.txt` |
| `probe/BootProbe.java` | cold boot timing | `boottimes.txt` |
| `e6_lanes.py` | experiment 6: each corpus pass's exact Bazel command rerun by hand with a world first on the classpath, every output compared with the Bazel baseline | `e6_lanes_*.txt` |
| `usage.py`, `usage2.py` | the earlier usage analysis (what our programs name) | `usage*.txt` |

PCT (experiment 6) ran through Bazel with `--test_env=JAVA_TOOL_OPTIONS=-Xbootclasspath/a:<world>`: resource lookup is
parent-first, so a `prelude.pure` on the boot class path wins over core's jar. The browser timing (experiment 7) is
`bazel run //wasm:startup` with the world copied over `prelude.pure` for the build and restored after.

## Other outputs

- `catalog-upstream-diff.tsv`: experiment 2, `CatalogUpstreamDiffTest`'s rows.
- `LOOSE_ENDS.md`: experiment 3.
- `test_only_prelude.txt`: prelude names upstream defines only in test code (under the earlier, too broad test rule).
- `e6_owned_final.txt`: the names added to today's footer lists to emulate the ownership filter.
- `corpus_closure_build_walls.tsv`: the walls of `closure(core_relational)` built as one module.
- `e7_wasm_first_answer.txt`, `e7_wasm_*_phases.txt`: experiment 7.

`e6_lanes.py` names the Bazel configuration directory `darwin_arm64-fastbuild`; adjust it on another platform.
