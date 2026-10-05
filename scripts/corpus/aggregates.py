"""
Generates aggregate instances over to-many associations.

The hand-written F-series probes (F0-F6) each pin ONE property: `count()` over an empty
to-many must return 0. They are quarantined against F6, which returns 1, and they are the
right shape for that job -- six minimal probes, each attributable to one association.

They also mean the NON-empty case has never been tested at scale. Counting children where
children exist is the ordinary case, it is correct today, and two hand-written specs cover
it. The model offers 146 to-many ends.

So the property is split, one level finer than graphs.py splits graph fetch:

    "count over an EMPTY to-many is 0"      -- hand-written, pinned 6x, quarantined (F6)
    "count over a NON-EMPTY to-many is N"   -- generated here, unblocked

Splitting them is what makes the volume reachable. Tangled together, F6 blocks everything;
separated, it blocks only the half it actually affects.

WHY ONLY SOME ENDS QUALIFY. F6 fails a whole service if any one row hits the empty case, so
an end is usable here only when EVERY seeded parent has children. Measured over the current
seed:

    146 usable to-many ends
      9  every parent childless    -- pure F6, nothing to test yet
    115  some parents childless    -- F6 hits those rows, blocked
     22  no childless parent       -- generated here

That 22 is not a permanent ceiling, it is a fact about today's data, and it is the honest
number rather than the impressive one. When F6 is fixed the filter below drops and all 146
become available in one line -- which is the same note graphs.py carries about to-many
navigation, for the same reason.
"""
from __future__ import annotations

import model
import oracle
import query
from query import Proj, Spec

# Bounded like stacks.MAX_ROOTS and graphs.MAX_ROOTS: the ranking decides which, so a wider
# seed widens the corpus without a new entry here.
MAX_ROOTS = 40

# One aggregate is a probe. The point of generating is to stack several counts on one root
# so the outer joins interact, which is where the corpus has previously found defects that
# single-aggregate probes missed.
MAX_AGGS = 3


def _child_counts(c: model.Corpus, tables: dict[str, list[dict]],
                  owner: str, prop: str) -> list[int] | None:
    """How many children each seeded parent has, or None if the path does not resolve."""
    rows = tables.get(c.main_table.get(owner, ""), [])
    if not rows:
        return None
    try:
        hops, _target = c.resolve_assoc(owner, [prop])
    except Exception:
        return None
    return [len(oracle.walk_many(c, tables, r, hops)) for r in rows]


# usable_ends per (corpus, tables, seeded): spread.py asks once per candidate root (about 2,000
# times a build) with the same three, and each answer walks every seeded parent's children
# (99% of the executed gate's time before this). The entry keeps the objects its key's ids name,
# so an id cannot be reused while the entry stands.
_USABLE: dict[tuple, tuple] = {}


def usable_ends(c: model.Corpus, tables: dict[str, list[dict]],
                seeded: set[str]) -> dict[str, list[str]]:
    """owner class -> to-many properties whose every seeded parent HAS children.

    The childless-parent test is done against the oracle, not against the engine, which
    matters: asking the engine which parents are empty would use the very behaviour F6 makes
    wrong, and the generator would then exclude exactly the wrong ends.
    """
    key = (id(c), id(tables), frozenset(seeded))
    if key not in _USABLE:
        _USABLE[key] = (c, tables, _usable_ends(c, tables, seeded))
    return {owner: list(props) for owner, props in _USABLE[key][2].items()}


def _usable_ends(c: model.Corpus, tables: dict[str, list[dict]],
                 seeded: set[str]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for (owner, prop), end in sorted(c.ends.items()):
        if not end.to_many or not end.join:
            continue
        if c.main_table.get(owner) not in seeded or c.main_table.get(end.target) not in seeded:
            continue
        counts = _child_counts(c, tables, owner, prop)
        if not counts or any(n == 0 for n in counts):
            continue
        out.setdefault(owner, []).append(prop)
    return out


def _identifier(c: model.Corpus, root: str) -> str | None:
    table = c.tables.get(c.main_table.get(root, ""))
    if table is None or not table.pk:
        return None
    pk = table.pk[0]
    return next((p for p, col in c.columns.get(root, {}).items() if col == pk), None)


# See graphs.FIXTURE_DOMAINS: the combination matrix and the hier:: feature domain are
# fixtures for mapping constructs, with their own generators, runtimes and data elements.
# Ranking over them couples every matrix change to the domain service set.
def _is_fixture(cls: str) -> bool:
    return cls.startswith(("combo::", "hier::"))


def build(c: model.Corpus, seeded: set[str],
          tables: dict[str, list[dict]]) -> list[Spec]:
    """One aggregate service per qualifying root, richest first."""
    ends = {k: v for k, v in usable_ends(c, tables, seeded).items() if not _is_fixture(k)}
    ranked = sorted(ends, key=lambda cls: (-len(ends[cls]), cls))[:MAX_ROOTS]

    specs, seen = [], set()
    for n, root in enumerate(ranked):
        ident = _identifier(c, root)
        if ident is None:
            continue
        props = ends[root][:MAX_AGGS]
        projections = [Proj(ident, [ident])]
        for prop in props:
            projections.append(Proj(f"{prop}Count", [prop], agg="count"))

        signature = (root, tuple(props))
        if signature in seen:
            continue
        seen.add(signature)

        short = query.short_name(c, root)
        # Named from the CLASS, not from a loop index. An index renumbers every
        # service downstream of it the moment the ranking changes -- and the
        # ranking is derived from the seed, so a data change silently repoints
        # every quarantine entry that keys on the name. That has already happened
        # once (F26); the quarantine below keys on ten of these names.
        spec = Spec(f"stress::AA_{short}Counts", f"/stress/aa_{short.lower()}",
                    f"Generated aggregate on {root}: {len(props)} count(s) over to-many "
                    f"associations, every parent of which HAS children. This is the "
                    f"non-empty half of the property the F-series pins on the empty half -- "
                    f"F6 makes count() over an empty to-many return 1, so an end with any "
                    f"childless parent is excluded here rather than quarantined. Several "
                    f"counts on one root so the outer joins interact; a single-aggregate "
                    f"probe cannot show that. Generated by scripts/corpus/aggregates.py.",
                    root)
        spec.projections = projections
        spec.sort = (ident, False)
        query.apply_temporal(c, spec)
        specs.append(spec)
    return specs


if __name__ == "__main__":
    import flat

    c = model.load()
    tables = flat.all_tables(c)
    seeded = {t for t, rows in tables.items() if rows}
    ends = usable_ends(c, tables, seeded)
    total = sum(len(v) for v in ends.values())
    specs = build(c, seeded, tables)
    print(f"roots with fully-populated to-many ends: {len(ends)} "
          f"({total} ends)")
    print(f"specs: {len(specs)}")
    for s in specs[:6]:
        aggs = sum(1 for p in s.projections if p.agg)
        print(f"  {s.name.split('::')[-1]:<36}{aggs} count(s)")
