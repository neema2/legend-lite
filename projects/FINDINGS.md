# What the project graph found

A single flat namespace cannot have a cross-project defect, so none of this was visible
before. These are grouped by whether they are about the ENGINE, about DEPENDENCY structure,
or about what a graph does to a MODEL — the last group being the one a single project cannot
produce at all.

## Engine and grammar

Each of these cost one project a failed compile, and each is now in `CONTRACT.md`.

| what | detail |
| --- | --- |
| boolean literal in a `Filter` | `Filter X(T.IS_ACTIVE = true)` → `Unexpected token 'true'`. A null test works. |
| `Integer * Float` | types as `Number`, not a subtype of `Float`. Needs `->toFloat()`. NOT symmetrical with `/`, which widens on its own. |
| `~filter` through a join | needs the database pointer on BOTH sides: `~filter [db]@Join \| [db]FilterRows`. |
| property on a subclass | cannot be mapped on the parent's set. The error names the property, not the set it was wrongly written in. |
| `~distinct` without a key | needs an explicit `~primaryKey` on the column being deduplicated. Keyed on the table's own key it collapses nothing, silently. |
| `~groupBy` | takes column REFERENCES, not expressions. A banded value must be stamped on the row to be grouped on. |
| `sum()` in a view | is nullable, so aggregate totals are `Float[0..1]` and arithmetic over them needs `->orElse(0.0)`. |
| an empty group | produces NO ROW at all, not a zero. A completeness report has to read that case from the parent. |
| an association end declared twice | as an end AND as a class property is a duplicate-property failure. Two projects wrote it that way first, having copied one that maps its edges as plain properties instead. |
| a constraint over `[0..1]` | does not type-check. The "is it set" rule has to be a derived property using `->isEmpty()`. |
| infix inside a list literal | `[0.0, $this.a - $this.b]->max()` is a parse error; parenthesising the element fixes it. Verified directly: the bare form fails, `($this.a - $this.b)` and a bare `$this.a` both compile. |

Two things reported as engine limits turned out not to be, and are worth recording as
corrections rather than quietly dropping:

* `||` compiles. One project reported it failing; the failure was elsewhere in the same
  expression.
* A wrapped join condition compiles. I told the projects it did not — I was wrong, and the
  one-line rule is house style because the CORPUS's own reader refuses one.

## Dependency structure

* **The include diamond closes.** Reaching one store or mapping by two routes — core-party
  arriving via both credit-core and collateral-core, core-instrument via both position-keeping
  and reference-data — resolves to one. It does not duplicate and does not error. Three
  separate projects verified this independently before relying on it, because no project had
  done it before and the failure mode would have been discovered thirty classes in.
* **Explicit set ids delete the default ones.** A project that writes
  `core_party::LegalEntity[cpLegalEntity]` no longer has `core_party_LegalEntity`, so every
  downstream reference must name the explicit id. This makes each project's MANIFEST
  load-bearing rather than documentation: an id is not derivable from the class name.
* **Depth is nearly free, size is not.** Over the finished 56-project graph, 3MB of Pure:

      through L0   12 projects   226 KB   parse  316ms   compile 2363ms
      through L1   32 projects  1028 KB   parse  759ms   compile 2512ms
      through L2   48 projects  2238 KB   parse 1126ms   compile 2850ms
      through L3   56 projects  3078 KB   parse 1353ms   compile 2987ms

  Parse tracks bytes almost exactly — 13.6x the source, 4.3x the parse. Compile does not:
  the same 13.6x costs 1.26x. And a project's own closure barely matters — the deepest,
  `regulatory-capital` with 18 projects and 963KB behind it, compiles in 2373ms against
  1698ms for `core-account`, which has no dependencies at all.

  These are the BEST of three runs each. A single-run version of this series produced
  incremental deltas that went NEGATIVE -- 300KB of extra Pure appearing to compile faster --
  which is impossible, and is what contention looks like when it is reported as data.
* **A transitive dependency is visible and must not be named.** Everything in the closure is
  on the classpath, so referring to a project you did not declare COMPILES. Several projects
  reported deliberately not naming `core_types::CtCurrency` and using a String instead, for
  exactly this reason. `check.py` catches it by compiling each project against its declared
  closure and nothing more — which is the only reason the discipline is checkable at all.

## What a graph does to a model, which one project cannot show

* **Two projects independently modelled a netting set.** `exposure-agg` has `ExaNettingSet`
  as the measurement-side grouping key; `legal-netting` has `NetSet` as the closure of one
  master agreement. They meet on `nettingSetId` and neither restates the other's numbers, so
  both compile and both are defensible. Nothing in the graph forces a decision, and nothing
  would have raised the question inside a single project — the concept only splits once two
  teams own two halves of it. Recorded rather than resolved: picking an owner is a modelling
  decision, not a compile error.
* **A project adds properties to classes it does not own.** A cross-project association end
  puts a property on the dependency's class, so `trading::Trade` now carries `amendments` from
  trade-capture, `lifecycleAmendments` from trade-lifecycle and `executionBookings` from
  order-execution. Those names are a shared namespace nobody owns. Three projects reported
  checking the others' MANIFESTs before naming an end, which works only because they thought
  to look.
* **Two valid styles for the same edge, and they cannot be mixed.** A project may model a
  relationship as an `Association` with mapped ends, or as a plain property over a join in the
  set implementation. Both compile and both are used here. What does not compile is declaring
  one property BOTH ways — which is what happens when a project copies the shape of a
  dependency that chose the other style. Two projects hit this, and neither could have known
  without reading the other's source rather than its manifest.
* **`Void not supported!` when linking a project — cause still unknown, and my first
  explanation was wrong.** Linking core-calendar and core-units into the executable corpus
  and navigating across failed at test-suite initialisation with
  `meta::pure::router::store::routing::Void not supported!`, naming nothing. I recorded that
  as hop count or a schema. `scripts/corpus/probe_boundary_navigation.py` rules out both,
  and two more besides — fourteen cases over an upstream/downstream pair, every one PASSING:

  | ruled out | cases |
  | --- | --- |
  | hop count | one hop and two hops across the boundary, all four edge-style combinations |
  | schema | a dependency table inside a `Schema` block, reached at one hop and at two |
  | edge style | `Association` with mapped ends, and a class-typed property over a join |
  | a missing `~primaryKey` | a downstream set declaring only `~mainTable`, as much of the corpus does |

  So it is none of those ALONE. What is left is something about the corpus specifically —
  its scale, or an interaction with one of its ~170 included mappings — and that is not
  something a small model reproduces by construction.

  The probe is kept because ruling four things out is most of the work of finding the fifth,
  and because the next person will otherwise start from the same four guesses. Not reported
  upstream: an unreproducible error with a wrong first explanation is not a finding.

* **The same column is two widths on either side of a join.** `INSTRUMENT_ID` is `VARCHAR(60)`
  in position-keeping and `VARCHAR(20)` in valuation-core; the join across them compiles.
  Whichever side is narrower is the one that truncates, and nothing says so at compile time.

## legend-lite, found by `bazel test //projects:tests` (Bazel workplan P3-23)

Every project is compiled by legend-lite alone with its declared closure, and the graph together
(`tools/legend/defs.bzl`). A failure is quarantined in `projects/BUILD.bazel` (`QUARANTINE`) with
its row here; the quarantined test holds the project to the recorded failure and turns red when
legend-lite compiles it, so the row comes out then.

* **F-L1 (2026-10-05): a view declared inside a `Schema` is lifted twice.** Quarantined:
  `firm-balance-sheet` (its views are in `Schema fbs`), which the graph test leaves out until then. The smallest model that
  fails:

      ###Relational
      Database r::Store
      (
        Schema s
        (
          Table L (K VARCHAR(10) PRIMARY KEY, V INTEGER)
          View V ( K: s.L.K PRIMARY KEY, N: s.L.V )
        )
      )

  `function 'r::Store$view$s.V' is defined more than once with the same signature`. The same
  view outside a schema compiles. Cause: `DatabaseDefinition.views()` is the flat mirror of
  EVERY schema's views, and `ModelBuilder`'s view index walks it and then each schema's views,
  so a schema view is declared twice (`LiftedViews` then lifts it twice).
  `DatabaseDefinition.defaultSchemaViews()` exists for exactly this walk; the index should read
  it. Not fixed here: it is a compiler change outside the Bazel program's files.
