# Mapping-normalizer audit — research detail (2026-09-15)

Supporting material for [`docs/MAPPING_NORMALIZER_AUDIT_2026_09_15.md`](../MAPPING_NORMALIZER_AUDIT_2026_09_15.md).

**Start at [`findings/FIXLIST.md`](findings/FIXLIST.md)** — the ordered, actionable list.
This directory is the homework behind it.

---

## What was audited

The arc `d3cd6efc9..ae16e5c46` (56 commits touching `src/main`, 209 commits total from the
worktree's prior HEAD), comprising:

- **T4.1 steps 2, 3a–3d, 4a, 4b, 5–6** — the "knowledge kernel" / stamped-facts work
- **Clean-sheet B1, B3.1b, B3.2, B3.3**
- **"Legacy routes as composition" legs 1, 1b, 2, 2a, 3a, 3b, 4a–4d, 5a, 5b, 6a–6g**

Scope read end to end: `core/src/main/java/com/legend/normalizer/` (30 files, 11,194 lines), the
`com.legend.resolver` package (35,517 lines), `com.legend.lineage` (3,456), plus
`model/MappingDefinition.java`, `model/NormalizedModel.java`, `compiler/ModelBuilder.java`,
`compiler/element/PureModelContext.java`, and the relevant guard tests.

## Method

Sixteen parallel audits, one per dimension, each given the same discipline:

1. **Read whole files end to end.** No sampling.
2. **Docs and commit messages are claims, not evidence.** Every claim checked against the code;
   divergences reported as findings in their own right.
3. **Cite `file:line` for everything.** A finding without a line is not a finding.
4. **Don't pad.** Mark legitimate occurrences clearly so the SUSPECT/BUG list stays credible.

Findings were then re-checked by the lead auditor where they were load-bearing, and where two
audits disagreed the source decided it.

## Evidence grades

| grade | meaning |
|---|---|
| **PROVEN** | demonstrated by executing the code — a probe was written and run against `core/target/classes` |
| **VERIFIED** | re-checked in the source by the lead auditor, independently of the agent that found it |
| REPORTED | one audit's finding, not independently re-checked |

Ungraded statements in these files are that audit's own reporting. Treat them as REPORTED.

## Reproducing the build

```
JAVA_HOME=/Users/neemsandv/jdk/jdk-21.0.11+10/Contents/Home \
/Users/neemsandv/jdk/apache-maven-3.9.9/bin/mvn -pl core test -Denforcer.skip=true
```

`mvn` is **not** on `PATH`. `-o` (offline) fails on a missing `maven-enforcer-plugin:3.3.0`.
Heavy tests are excluded by default (`surefire.excludedGroups=heavy` in `core/pom.xml`).

For gates, use `tools/allgates.sh` — **never** a hand `mvn`. The hand path silently reads the
default oracle checkout instead of the pinned one (`docs/GATES.md` documents this; it caught this
auditor, and a second guard inside `MinimalCorpusTest` caught the bypass).

## A standing caveat on engine citations

Both oracle checkouts are behind the pins in `tools/oracle-pins.env`:

| | pinned | on disk |
|---|---|---|
| `legend-engine` | 4.145.0 | 4.137.1-SNAPSHOT (`943d38b3`, 2026-08-06) |
| `legend-pure` | 5.99.0 | 5.92.0-3 (`d00cfd5b`, 2026-08-05) |

**Any finding of the form "citation line N is wrong" that compares against the local checkout is
unsound.** Two audits made this error before it was caught. `findings/10-engine-citations.md`
explains why the citations are nonetheless judged honest — the offsets grow monotonically while
internal gaps are preserved to the line, which a clerical error cannot produce.

## File index

| file | covers |
|---|---|
| `findings/FIXLIST.md` | ordered actionable fixes |
| `findings/01-mapping-normalizer-core.md` | `MappingNormalizer`, `Pipeline`, `ModelNormalizer`; four executed probes |
| `findings/02-stamped-facts.md` | stamped-facts pipeline, fact census, order-independence |
| `findings/03-join-chain-emission.md` | `JoinChainEmission`, slot minting, `innerFilteredSource` |
| `findings/04-union-and-routes.md` | `UnionSynthesis`, route classification, leg-6g verdict |
| `findings/05-views-and-groupby.md` | `ViewRelation`, `GroupBySynthesis`, the flattening-fallback question |
| `findings/06-associations-and-xstore.md` | `AssociationSynthesis`, `XStorePureEnds`, Phase-D/E layering |
| `findings/07-downstream-consumers.md` | resolver: consumed vs re-derived |
| `findings/08-lineage-rediscovery.md` | `ScanRelations` duplication on a shipped surface |
| `findings/09-test-quality.md` | test substance census; which rules would go red |
| `findings/10-engine-citations.md` | citation census and the stale-checkout adjudication |
| `findings/11-sql-leanness.md` | emitted SQL, probes, engine comparison, DuckDB plans |
| `findings/12-git-history-and-ratchets.md` | arc map, deletion claims, ratchet table |
| `findings/13-dyna-and-coercions.md` | `RelOpTranslator`, `DeclaredCoercions`, enum decode |
| `findings/14-guards-fallbacks-census.md` | exhaustive census of silent defaults / first-wins / swallows |
| `findings/15-m2m-json-enum.md` | M2M, JSON-source, embedded, clean-sheet bypass |
| `findings/16-architecture-review.md` | decomposition, noun test, one-owner table, proposed guards |
