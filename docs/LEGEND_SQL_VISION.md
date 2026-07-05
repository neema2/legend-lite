# legend-sql — the vision (governs all SQL-layer design from 2026-07-05)

The SQL layer (`com.legend.sql` + dialects + exec normalization) is built to
become a STANDALONE world-class SQL library — a typed, execution-verified,
N-in/N-out SQL metamodel rivaling SQLGlot/Calcite/Trino — with Legend's Pure
surfaces (Relation pipeline, Class+Mapping) as *frontends* over it. Real
Legend corroborates the shape: its `legend-engine-xts-sql` module accepts
Postgres-grammar SQL into the same relational model.

```
READERS (N-in)                THE PRODUCT                    WRITERS (N-out)
SQL text, any dialect  ─┐   ┌───────────────────────────┐  ┌─ SQL text, any dialect
 (ONE parser core,      ├──▶│ typed AST (SqlType, not   │──┤   (AnsiSqlRenderer + overrides)
  per-dialect overrides)│   │  Pure types)              │  ├─ Relation-pipeline text
Pure Relation pipeline ─┤   │ SqlFn semantic vocabulary │  │   (SQL → Pure — the adoption
 (the Lowerer)          │   │ rewrite engine (fold      │  │    weapon; falls out of
Pure Class + Mapping   ─┘   │  policy generalized,      │  │    dialect symmetry)
 (Phase H)                  │  capability downgrades)   │  └─ …
                            │ differential conformance  │
                            └───────────────────────────┘
```

## Principles

1. **N-in is architecture, not roadmap.** ONE dialect-parameterized parser
   core (tokenizer + Pratt parser; dialects override token sets, precedence,
   function tables, extensions — the SQLGlot shape). Postgres grammar is
   dialect #1 (the center of gravity; real Legend's choice), never "the"
   input.
2. **A dialect is a symmetric (reader, writer) pair** + capability set +
   value normalizer. Implementing either side is valid. Pure surfaces are
   READERS that are compilers instead of text parsers — same output
   contract: produce the typed AST. Relation-pipeline gets a WRITER too:
   `parse(warehouse SQL) → AST → idiomatic Pure`.
3. **The canonical center is the whole product**: typed AST + SqlFn + SqlType
   + rewrite rules. Everything else is pluggable periphery that round-trips
   through it.
4. **Typed end-to-end is the moat.** Schema-aware AST (outputs carry types)
   enables correctness no untyped transpiler can offer; execution-verified
   development (every construct runs on real engines) is the second moat;
   leanness-as-a-spec (flatness goldens) the third.
5. **Grammar-first node design.** Every new AST node is modeled as the SQL
   standard/dialects have it, not as a Legend lowering minimally needs
   (CTEs incl. recursive, INTERSECT/EXCEPT, IN(SELECT)/ANY/ALL, LATERAL,
   GROUPING SETS, DML for write(), WINDOW clause, intervals — the known
   completeness gaps).
6. **Capability statements, never approximations** (already enforced): a
   dialect that cannot express a construct throws at render; downgrades
   (QUALIFY self-wrap) are explicit rewrites.

## Test discipline (generalizing what the repo already does)

- Per-dialect IDENTITY tests: parse → render fixpoint.
- Cross-dialect TRANSPILE matrices.
- DIFFERENTIAL execution: same AST on DuckDB/SQLite/Postgres, results
  diffed; sqllogictest + TPC corpora as completeness forcing functions.
- Pure round-trips: Relation → AST → SQL → AST → Relation fixpoint.

## Immediate prerequisites (cheap now, brutal later)

1. **SqlType**: the SQL layer's OWN type vocabulary; Pure→SqlType mapping at
   the lowering boundary. Today OutputCol/Cast import Pure types — the one
   dependency that kills standalone-ness.
2. **The wall**: ArchUnit — `com.legend.sql..` imports nothing from
   `com.legend.compiler..` (build failure, like the core/engine wall).
3. From here on: rule 5 for every node we touch.

## Sequencing honesty

The Legend arc (Phase H, corpus scoreboard) proceeds on the current seam —
it is compatible. The parser core, rewrite engine, and second/third backends
grow corpus-driven without blocking Legend milestones.
