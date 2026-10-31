"""
Emits the artefacts for cross-engine differential testing.

The same corpus, the same seed, two independent implementations. legend-engine executes
the .pure testSuites directly; legend-lite generates SQL that has to be run against a
database holding the identical rows. This writes what the legend-lite side needs:

  <out>/seed.json             every table's DECLARED columns, each with the type the seed gives it, and its rows,
                              each cell the text the database casts to its column (null: SQL NULL); the test
                              creates and fills the tables through the dialect of the database it runs on
  <out>/expected/<Svc>.txt    the oracle's answer in a normalised form

Run by Bazel (//scripts/corpus:gen_differential, `--out DIR --inputs FILE...`): its tree is the data of
//core:corpus_differential_test (Bazel workplan P3-18, D3 row 4).

Both sides are compared in a normalised text form rather than as JSON, so that neither
side's JSON library, float formatter or key ordering can influence the verdict. The first
line of each expectation names the column kinds; the reader uses them to normalise, which
is what keeps `190500.0` on one side and `190500` on the other from being a false
difference.

This is the third assertion mechanism from the plan. Unlike the oracle it cannot be wrong
about semantics — it only reports that two engines disagree — and unlike a metamorphic
relation it cannot be satisfied vacuously. Its blind spot is equally plain: a defect both
engines share is invisible to it.
"""
from __future__ import annotations

import decimal
import json
import sys
from pathlib import Path

import battery
import flat
import literals
import model
import stacks
from quarantine import LITE_QUARANTINE
import oracle
import query
import seed
from model import STRESS


# Declared kind -> the seed's column type, a database-neutral DDL kind (sql.SqlDdl.ColumnType.Kind) that the test's
# dialect spells: BIT is Legend's boolean.
_KIND = {"string": "VARCHAR", "int": "INTEGER", "float": "DOUBLE",
         "bool": "BIT", "date": "DATE", "timestamp": "TIMESTAMP"}

NULL = "~"


def _column_type(col) -> str:
    """A column's type in the seed. A declared FLOAT or REAL is four bytes (REAL), as the oracle models it (flat.F32:
    29.8 is stored as 29.799999237...); creating it DOUBLE made the two halves of the harness disagree in the
    seventh decimal (Bazel workplan P3-18)."""
    base = str(col.type or "").upper().split("(")[0]
    return "REAL" if base in ("FLOAT", "REAL") else _KIND[col.kind]


def _cell(v, kind: str):
    """A value as the text its column takes, or None for SQL NULL: the database does the typing (setup.RowLoad)."""
    if v is None:
        return None
    if kind == "bool":
        return "true" if v else "false"
    if isinstance(v, decimal.Decimal):
        return format(v, "f")         # a DECIMAL column's seed value, digit for digit (never `Decimal('...')`)
    if kind in ("int", "float"):
        return repr(v)
    return str(v)


def seed_json(c: model.Corpus, TABLES) -> str:
    """The seed as data, no SQL (the dialect program's Phase 6, 2026-10-17: it was DuckDB DDL and INSERTs): one table
    per line. DDL comes from the DECLARED store schema, not from CSV type inference, so a column that happens to be
    all-NULL in the seed still gets its real type. A table in a named Schema is in it: legend-lite's SQL names it
    `schema.TABLE`, as the mapping does (an unqualified seed left those queries reading no table; Bazel workplan
    P3-18). Views are excluded: the engine inlines their GROUP BY, so creating a real table of the same name would
    shadow the view and stop testing the aggregation."""
    lines = []
    for name, rows in TABLES.items():
        if name in c.views:
            continue
        table = c.tables[name]
        cols = table.columns
        lines.append(json.dumps({
            "schema": None if table.schema == "default" else table.schema,
            "table": name,
            "columns": [[n, _column_type(col)] for n, col in cols.items()],
            "rows": [[_cell(r.get(n), col.kind) for n, col in cols.items()] for r in rows],
        }, ensure_ascii=False))
    return '{"tables": [\n' + ",\n".join(lines) + "\n]}\n"


def normalise(v, kind: str) -> str:
    if v is None:
        return NULL
    if kind == "float":
        return f"{float(v):.6f}"
    if kind == "bool":
        return "true" if v else "false"
    return str(v)


def expected_text(c: model.Corpus, spec, TABLES) -> str:
    rows = oracle.evaluate(c, spec, TABLES)
    kinds = oracle.kinds(c, spec)
    # After a groupBy only the keys and aggregates survive; the other projected columns
    # are gone, so the header must follow the RESULT shape, not the projection list.
    aliases = ([*spec.group_by, *(name for name, _, _ in spec.aggs)]
               if spec.group_by else [p.alias for p in spec.projections])
    head = "|".join(f"{a}:{kinds[a]}" for a in aliases)
    # Sorted, because EqualToJson compares unordered and the SQL carries no ORDER BY
    # unless the query asked for one; row order is not part of the claim.
    body = sorted("|".join(normalise(r[a], kinds[a]) for a in aliases) for r in rows)
    return "\n".join([head, *body]) + "\n"


def main() -> None:
    model.take_inputs(sys.argv)
    if "--out" not in sys.argv:
        raise SystemExit("usage: differential.py --out DIR [--inputs FILE...]")
    out = Path(sys.argv[sys.argv.index("--out") + 1])
    c = model.load()
    TABLES = flat.all_tables(c)
    # Clear first. A service that stops being emitted — because it moved out of scope, or
    # was renamed — otherwise leaves its old expectation behind, and the Java side keeps
    # comparing against a file nothing generates any more.
    exp_dir = out / "expected"
    if exp_dir.is_dir():
        for stale in exp_dir.glob("*.txt"):
            stale.unlink()
    exp_dir.mkdir(parents=True, exist_ok=True)
    (out / "seed.json").write_text(seed_json(c, TABLES), encoding="utf-8", newline="\n")

    # Graph-fetch services are OUT OF SCOPE for the differential.
    #
    # The comparison here is a normalised FLAT row — `col|col|...` with a header of column
    # kinds — which cannot represent a tree. Emitting an expectation for a graph spec
    # produced an empty body and every actual row reported as a disagreement: noise that
    # looks exactly like a real finding.
    #
    # Covering them would mean either teaching this harness tree comparison or having
    # legend-lite produce the same serialization. Neither is done, so they are skipped
    # HERE and remain fully asserted against legend-engine by run.py.
    # Also out of scope: any spec whose expectation is MIRRORED. Those roots have no
    # table — an M2M target is not read from a store — so there is nothing for the oracle
    # to evaluate and nothing for legend-lite to generate SQL against.
    # The walking generators' view of the seed, as build.py hands it them (literals.FIXTURE_TABLES): with every
    # seeded table these specs would reach the typed-values pack and no longer be the services emitted.
    generated = stacks.build(c, literals.walked(TABLES), TABLES)
    # Services legend-lite cannot even load the model for (Measure) are out of scope on
    # this side; run.py still asserts them against legend-engine.
    lite_blind = {
        "MU0_MonetaryTrade",
        # its provisioning spans two stores bound to two connections; legend-lite binds one session connection per
        # query (multi-connection execution is not modeled), so //core:stress_suites SKIPS it too (a registered skip)
        "X1_ExternalEntityProjection",
        # 2026-10-30, the stress corpus plan's Phase 6 (relmap.py; the user's choice): the binding transformer's
        # relational twins over hier::IssuerMapping. legend-lite stops before it runs on any Binding transformer
        # ("expected hier::Profile, got String"), which the differential cannot hold as a known divergence (a crash is
        # a new failure). //core:stress_suites counts them and their relation-mapped services as failing, and
        # legend-engine passes all four.
        "RMR_IssuerFlags",
        "RMR_IssuersFlagged",
    }
    specs = [s for s in query.load() + list(battery.SPECS) + generated
             if s.graph is None and getattr(s, "mirrors", None) is None
             and s.short not in lite_blind]
    for spec in specs:
        # The expectation is written WITHOUT the timestamp reformatting the JSON path
        # applies -- that is a legend-engine serialisation detail, not a value difference,
        # and the SQL side never sees it.
        (out / "expected" / f"{spec.short}.txt").write_text(expected_text(c, spec, TABLES), encoding="utf-8",
                                                             newline="\n")
    # legend-lite's OWN known divergences from the reference — not legend-engine's. The
    # two lists are different by construction: the F6 fan-out services fail in
    # legend-engine and agree here.
    (out / "expected" / "QUARANTINE.txt").write_text(
        "".join(f"{n.split('::')[-1]}\t{q[0]}\t{q[1]}\n"
                for n, q in sorted(LITE_QUARANTINE.items())), encoding="utf-8", newline="\n")

    print(f"seed.json: {sum(1 for n in TABLES if n not in c.views)} tables, "
          f"{sum(len(v) for v in TABLES.values())} rows")
    skipped = sum(1 for s in battery.SPECS
                  if s.graph is not None or getattr(s, "mirrors", None) is not None)
    print(f"expectations: {len(specs)} services -> {out / 'expected'}"
          + (f"  ({skipped} graph-fetch services skipped: flat comparison cannot "
             f"represent a tree; M2M targets have no table)" if skipped else ""))


if __name__ == "__main__":
    main()
