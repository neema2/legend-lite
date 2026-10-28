# Running the corpus

Two bodies of work live here and they run differently.

| | what it is | how it is checked | where |
| --- | --- | --- | --- |
| the **corpus** | ~4690 executable services with seeded data | run against legend-engine, every answer compared to an independently computed expectation | `core/src/test/resources/stress/` |
| the **project graph** | 56 Legend projects that depend on each other | compiled only — no data, no runtimes, no services | `projects/` |

Some of the 56 are also pulled into the corpus and executed; see
[the boundary](#the-boundary) below.

## Prerequisites

**Bazel.** Since 2026-10-21 the corpus commands are Bazel targets: the engine runner, its JDK
and its jars (legend-engine at the release `release.MODULE.bazel` pins), and the Python the
scripts run on all come from the build. Nothing is built by hand, and there is no `cp.txt`.

The probes (`scripts/corpus/probe_*.py`) and `scripts/projects/{check,loadtime}.py` still read
the Maven-era runner (`cp.txt`, `target/classes`, `run.py`'s old `RUNNER` and `JAVA_HOME`),
so they don't run. Moving them onto `run.py`'s `Engine.launch` is the Bazel workplan's P7-03.

## The commands

Everything is driven from the repository root.

```
bazel run //core:update_stress_corpus                    # regenerate the corpus from the generators
bazel run //scripts/corpus:changed -- --out changed.txt  # the testables that changed against HEAD
bazel run //scripts/corpus:run -- --only changed.txt     # run those on legend-engine and judge them
bazel run //scripts/corpus:run -- --jobs 4               # the whole corpus, four JVMs at once
bazel test //scripts/corpus:engine_stress                # the whole corpus as a sharded test (manual)
```

`changed` lists every testable when anything other than a service changed: a seed, a mapping
or a class can change any service's answer. `run` writes `engine-pass.txt` (lite's pass-list
form), `engine-verdicts.tsv`, `engine-report.tsv` and each JVM's log into `--out`, or into a
temporary directory it names.

The generators are the part that matters. Thirteen files under `stress/` are **generated wholesale**
and must never be hand-edited, because the next build overwrites them (`core/stress.bzl`'s
`STRESS_GENERATED` lists them all); the main ones:

- `94-fanout-services.pure` — every generated service and its expectation (134k lines)
- `93-testdata.pure` — the `###Data` element, i.e. all seeded rows
- `98-combination-execution.pure` — the combination matrix's services
- `99-typed-services.pure` — typed constants, predicate trees and parameters (`literals.py`, the
  improvement plan's Phase 1)
- `99-intraday-services.pure` — milestoning at time of day: versions read at a DateTime, a StrictDate's
  midnight, a parameter, every version or a range, and dated navigations (`intraday.py`, the plan's Phase 3)
- `92-services.pure` — the hand-written battery, rendered

Everything else under `stress/` is hand-written and safe to edit. If you want to change what
is generated, change the generator in `scripts/corpus/`, not its output.

`build.py` also runs every consistency check before it writes anything — a class whose
mapping the runtime cannot resolve, a duplicate set id, a seeded value too wide for its
column, a service that sorts on an unprojected alias, a seed spine that lost an adversarial
shape or emptied a to-many a service counts, and an expected result that is empty or
hollow outside a declared emptiness family
(`emptiness.py`). It prints them all and writes nothing if any fires. A clean build ends with
`wrote` or `no changes`.

The whole corpus takes **88 minutes** with `--jobs 4` on a 20-core Windows desk (measured
2026-10-21 at 4,735 testables). The run's last line is the summary, which on that day read:

```
4685 passed, 32 known-fail (quarantined), 1 not run (hangs), 18 unexpected, 4736 total, in 5291 s
```

The 18 were 14 JVMs killed by a concurrent Bazel build (all passed alone), legend-engine's fix
of F27 (`FIXED`; its quarantine entry is now gone) and F60's three `cbrt` cells (the
platform's last bit; since 2026-10-23 those cells read perfect cubes, the improvement plan's
D6). **`0 unexpected` is the only acceptable result.** The other numbers are allowed to move.

### Where the time actually goes

Measured 2026-10-21, four JVMs at once:

| | |
| --- | --- |
| per testable | about 3 s; a graph fetch up to 24 s (`DSTree_*`), the slowest family |
| per JVM | 25 s alone, about 50 s beside three others, to parse and compile the corpus (parse is twice compile) |
| per batch of 200 | 650 to 1,130 s, by family |
| memory | about 4 GB resident per JVM, under a 6 GB heap |

Until 2026-10-21 each test also paid for every test before it in its JVM: legend-engine's
service test runner kept every test's pooled DuckDB connection, and each later test's data
setup slowed (`docs/UPSTREAM_FINDINGS.md`, F59). Nine services sharing `stress::RT` took 2.4 s
to 25 s, one after another, and a batch of 40 ran for over 40 minutes. The runner now closes
the pools after every testable, which is also what makes batches of 200 work.

## The two generated layers

The services fall into two layers, and the split is deliberate.

**Bare projections**, one per subtype set, ~1830 of them. A wrong `~filter` fails by returning
the WHOLE table rather than by erroring, and a bare projection over the subtype is the only
thing that makes that visible. These are the control. About 570 expect no rows: the seed holds
no row of that subtype, and the empty answer is the assertion (`emptiness.py`'s families).

**Stacked queries** from `scripts/corpus/deepstack.py`, ~2440 of them: four-hop chains
filtered on a NAVIGATED column, `groupBy` on a joined key, `graphFetch` three and four levels
deep, subtype roots navigating inherited edges, qualified properties taking arguments. Since the
improvement plan's Phase 2 (2026-10-27) every one of them reaches a value: the deep variants root
only at classes the seed holds rows of, a seed pass (`spines.py`) relinks or clones the fewest
rows that give each chain complete paths to two distinct values, and each filter compares
against a value of its column (`selective.py`) instead of the `> ' '` guard, which a tenth keep.

A stacked query is only interpretable if a simple one over the same rows passes, which is why
the bare ones were kept rather than upgraded. The same split already pays around F6
(`aggregates.py` against `tomany.py`) and F28 (`CB_NotEqualsNull`).

One pair is a true INVARIANCE. `DSDeep_X` filters on a model path before `project`, where the
predicate can be pushed into the WHERE beside the join; `DSPost_X` applies the identical
predicates to the projected ALIASES afterwards. Each pair's expectations are computed
independently from the seed and `build.py` requires them equal -- so if the engine ever places
one of them wrongly, exactly one side of the pair fails and names itself.

## Reading a failure

`run.py` classifies every testable, and the classes mean different things:

| verdict | meaning |
| --- | --- |
| `PASS` | the engine's answer matched the oracle's |
| `KNOWN-FAIL <Fnn>` | quarantined against a recorded finding in `docs/UPSTREAM_FINDINGS.md` |
| `REGRESSION` | it failed and is not quarantined — **this is the one to care about** |
| `ERROR` | it threw rather than returning a wrong answer |
| `FIXED <Fnn>` | a quarantined service now passes; remove it from `quarantine.py` |
| `HUNG` | it was still running after `--limit` (600 s); it needs a repro and a `HANGS` entry |
| `DIED` | its JVM exited while it ran |
| `MISSING` | it ran and reported no test |

Since 2026-10-21 the runner takes one testable at a time and reports each as it finishes, so
one service costs only itself. A service that throws during test-suite *initialisation* is
an `ERROR` of its own; before, it killed its batch's JVM and left a wall of `MISSING`. After
a hang or a crash, `run.py` starts a new JVM on the testables after it.

## Running one service

The full suite is too slow for a debugging loop. To run a handful, list them in a file and:

```
bazel run //scripts/corpus:run -- --only mine.txt -v      # -v prints every verdict and the JVM's log
```

or run the runner directly on any files, the linked projects first:

```
bazel run //tools/engine-runner:testable -- <files...> --testable=stress::PL2_PillarTenorBand --dump=<dir>
```

Two things that are easy to get wrong and fail anonymously:

- **the linked project files must come first.** A `.pure` file with no `###Section` header
  inherits whatever section the previous file left open, so order is load-bearing. Omit them
  entirely and the corpus fails with a bare `Unexpected token` naming no file.
- **`perf.TestableMain` runs only what is NAMED.** Passing the files with no `--testable=`
  compiles everything and runs nothing, reporting `0 total` rather than an error.

`--dump=<dir>` writes the expected and actual JSON per suite, which is the only way to see a
diff the console truncates.

## The project graph

```
python3 scripts/projects/check.py              # each project alone, then all 56 together
python3 scripts/projects/check.py core-fx      # just one
python3 scripts/projects/check.py --graph      # just the whole-graph compile
python3 scripts/projects/loadtime.py           # parse and compile time by layer and closure
```

`check.py` asks two different questions. **Alone**: does a project compile with exactly its
declared dependency closure — a project needing an undeclared one fails here, which is the
defect the graph exists to find. **Together**: does the whole graph compile at once, which is
where set-id and filter-name collisions appear, both being global namespaces in Legend.

It also checks that a project is *substantively* what it was meant to be — the size band from
`scripts/projects/spec.py` and the presence of a `MANIFEST.md`. A three-file placeholder
compiles perfectly, and without that check the graph looks complete when it is not.

`projects/CONTRACT.md` is binding on anything added to `projects/`. Its "learned the hard
way" list is eleven items long and every one of them cost a project a failed compile.

## The boundary

`projects/` is compile-only by design, which proves a cross-project reference *compiles* and
nothing more — and several recorded findings compile perfectly and fail at execution. So a
slice of the graph is pulled into the executable corpus:

```python
# scripts/corpus/model.py
LINKED_PROJECTS = ["core-types", "core-tenor", "core-fx", "core-ratings",
                   "core-instrument", "core-calendar", "core-units", "fee-core"]
```

**Order is load-bearing: dependencies before dependents.** `fee-core` depends on the two
before it, and `core-types` is in the list only because fee-core needs it — it exports no
store and no mapping at all.

Adding a project to that list is not enough on its own. It also needs:

1. **rows** in `scripts/corpus/seed.py` for each of its tables. Leave one unseeded and the
   corpus's expansion ring will invent placeholder rows for it, which surface as
   width-violation errors rather than as "nobody seeded this".
2. `include <project>::Store` in `store::DB` — **first in the body**, which is a grammar
   requirement, not tidiness.
3. `include <project>::Mapping` in `projectlink::ProjectLinkMapping` — likewise first.
4. an association in `8111-projectlink.pure` if the corpus is to navigate into it, with the
   set ids named on **both** ends. The project side names its ids explicitly; the corpus side
   mostly does not, so the two halves of one association follow opposite conventions and
   neither is guessable from the class name.

Two things that a project brings and the corpus does not have, which cost a build each:

- **a class declared over several lines**, with stereotypes and tagged values above its name,
  and **a qualified property whose body wraps**. The reader is line-based and folds both, but
  only because it RAISED rather than skipping them — an unparsed class has no properties, no
  mapping and no service, and nothing would have said so.
- **arithmetic follows the COLUMN type, not the property type.** A `DECIMAL` column adds and
  subtracts exactly -- `108.7500 - 107.9000` is `0.85` in the database and
  `0.8499999999999943` in binary -- while a `DOUBLE` column computes in binary and a 4-byte
  `FLOAT` computes narrowed. The oracle carries a tag per width (`flat.F32`, `flat.F64`);
  a value that loses its tag silently gets the wrong arithmetic.
- **a graphFetch ENFORCES multiplicity where a projection does not.** A `[1]` property whose
  column holds a NULL returns the null happily in a TDS projection and fails a tree with
  "Property of multiplicity [1] can not be null". The seed nulls every nullable column on
  purpose, so a generator building trees has to CHOOSE a null-free leaf rather than take the
  first one and reject the tree.
- **a wrapped `Join` condition.** A two-column join written over two lines is valid grammar
  and the reader now folds it on parenthesis balance. A comment in fee-core says a wrapped
  condition "is not parsed" -- that was true of the READER, not of Legend, and core-account
  wraps all four of its two-column joins.
- **a decimal column.** Seed it with a FLOAT, not a quoted string. The emitter writes
  `repr()` and the oracle's exact-decimal path goes through `Decimal(str(v))`, so every digit
  survives either way -- but a string is COMPARED as a string, and
  `$this.factorToBase == 1.0` is false for `"1.00000000"`. The engine returns decimals padded
  to the column's scale; that is not a mismatch, because `EqualToJson` compares numbers
  numerically and only the value has to agree.
- **a value containing a comma.** Allowed: the data element is CSV and RFC4180 quoting works,
  including `""` for an embedded quote. A NEWLINE is still refused, because the element is a
  concatenation of one Pure string literal per row and quoting cannot save that.
- **an enum over a string column.** A generator that picks a filter column by the column's
  type will pick it and emit `> ' '`, which fails to compile and takes the whole file with
  it. Choose by the DECLARED property type.
- **a mapping set that `extends` another.** It inherits the parent's main table, its column
  mappings AND their transformers — copying the columns without the enum transformers read
  `'ZC'` off the row where the engine returns `ZERO`, and only a subtype whose enum property
  is declared on an ANCESTOR set shows it. It does NOT inherit the parent's `~filter`; that
  is F56, and it is the engine's behaviour rather than the reader's choice.

A project whose value is its SUBTYPES rather than its navigation will generate almost
nothing on its own: `stacks.py` wants two distinct navigation targets and `graphs.py` wants
to-one branches, and a wide flat master table has neither. Register it in
`taxonomy.py`'s `TAXONOMIES` and `IDENT` instead — one entry per LEVEL, since each level is
told apart by a different column — and one service per subtype falls out.

## What the reader refuses, and what it records

The reader RAISES on a form it cannot model, and records what it can see but not resolve in
`c.unparsed`. Generators skip the latter, so a gap costs COVERAGE and never correctness -- a
service using an unresolved property fails with a reason rather than returning a wrong answer.

Five entries remain, all in single-construct mapping fixtures. That number was eleven until a
one-character asymmetry was found: a nested embedded block closes with a bare `)` and its
parent with `),`, and the generic closing-punctuation skip swallowed the bare one. So
`ownership ( ... taxResidence ( ... ) )` popped once instead of twice and stayed open, and
every property declared AFTER it -- a sibling embedded block and six outbound joins -- was
attributed to the inner class and resolved against nothing. One level of nesting worked; a
sibling after it did not, and `core_account::Account` read as having no navigation at all.

## The rule the whole thing rests on

**Every expectation is computed from the seed by `scripts/corpus/oracle.py`, never read back
from the engine.** An expectation copied from the engine's output asserts that the engine
agrees with itself, which is always true and worth nothing.

This is why the oracle refuses an unknown function rather than guessing at it, why it raises
on an aggregate it does not implement rather than returning null, and why a cross-project
function had to be reimplemented here from the project's own source. If you add a construct
the oracle cannot evaluate, teach the oracle — do not relax the assertion.
