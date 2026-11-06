"""
Generates EMPTINESS predicates over to-many associations, at the scale the model has.

The corpus declares 177 to-many association ends and traverses TWELVE. Everything else is
to-one navigation, which is a different semantic entirely: a to-one hop lands on at most one
row and a broken hop yields NULL, where a to-many hop lands on a SET that may be empty. The
model has always had the shape and the queries have never used it.

`count()` is the obvious way to touch it and is the wrong one to build volume on: F6 makes
`count()` over an EMPTY to-many return 1 rather than 0, so any service whose row set includes
a childless parent fails -- which is why aggregates.py can only use the 22 ends where every
parent has children, and why that number collapsed to 8 once the seeder started linking
foreign keys properly.

`isEmpty` and `isNotEmpty` do NOT have that problem. Verified before any of this was
generated: a parent with no children returns isEmpty=true and isNotEmpty=false, correctly,
on the same data where count() returns 1 instead of 0. So the empty case -- the interesting
one, the one the adversarial seed guarantees exists -- is reachable after all, just not
through the function the corpus reached for first.

`forAll` is NOT generated: it has no SQL translation at all
("No SQL translation exists for the PURE function 'forAll_T_MANY__Function_1__Boolean_1_'"),
which is recorded as a finding rather than worked around.

Both predicates go on the SAME end in one service. Asserting only one of them would let a
degenerate implementation pass by returning a constant; asserting both means a service is
green only if the two disagree row by row, exactly where the data says they should.
"""
from __future__ import annotations

import model
import query
from query import Proj, Spec

# Bounded like the other generators: the ranking decides which ends, so a wider seed widens
# the corpus without a new entry here.
MAX_ROOTS = 40
MAX_ENDS = 4


def _is_fixture(cls: str) -> bool:
    return cls.startswith(("combo::", "hier::"))


def usable_ends(c: model.Corpus, tables: dict[str, list[dict]],
                seeded: set[str]) -> dict[str, list[str]]:
    """class -> its to-many ends whose target is seeded and whose join resolves.

    Unlike aggregates.py this does NOT require every parent to have children. The childless
    parent is the case worth having, and these predicates are correct on it.
    """
    out: dict[str, list[str]] = {}
    for (owner, prop), end in sorted(c.ends.items()):
        if not end.to_many or _is_fixture(owner) or not model.walked(c, end):
            continue
        if c.main_table.get(owner) not in seeded:
            continue
        if c.main_table.get(end.target) not in seeded:
            continue
        try:
            hops, _t = c.resolve_assoc(owner, [prop])
        except Exception:
            continue
        if not hops:
            continue
        # Not over a join the planner cannot index on. F51: `isEmpty()` over a to-many whose
        # join is anything other than a single key equality returns the source row once per
        # joined row -- correct booleans, wrong number of rows. Every such end generated here
        # would fail for the one reason, so they are excluded and CV6_PillarEmptiness pins it
        # once.
        #
        # A general join records its columns as empty strings, which is exactly what "no key
        # to index on" looks like in this reader.
        if any(not fcol for _j, _ft, fcol, _tt, _tc in hops):
            continue
        out.setdefault(owner, []).append(prop)
    return out


def _identifier(c: model.Corpus, root: str) -> str | None:
    table = c.tables.get(c.main_table.get(root, ""))
    if table is None or not table.pk:
        return None
    pk = table.pk[0]
    return next((p for p, col in c.columns.get(root, {}).items() if col == pk), None)


def build(c: model.Corpus, seeded: set[str],
          tables: dict[str, list[dict]]) -> list[Spec]:
    ends = usable_ends(c, tables, seeded)
    ranked = sorted(ends, key=lambda cls: (-len(ends[cls]), cls))[:MAX_ROOTS]

    out, seen = [], set()
    for root in ranked:
        # Subtypes are skipped. A subclass inherits its supertype's associations -- the
        # reader propagates them, correctly -- so all 58 derivative subtypes suddenly
        # offered `legs` and `optionTerms` and this generator produced a service for each.
        # They add nothing: it is the SAME association over the same table, and the base
        # already has a service for it.
        #
        # They also do not run. Navigating an inherited association from a SUBTYPE set fails
        # with "Void not supported!" -- the association mapping names the BASE's set id, and
        # a query rooted at a subtype set is not it. The taxonomy services reach the same
        # ends without trouble, so this is a property of these generated shapes rather than
        # of the model.
        if c.classes.get(root) is not None and c.classes[root].supertype:
            continue
        ident = _identifier(c, root)
        if ident is None:
            continue
        props = ends[root][:MAX_ENDS]
        signature = (root, tuple(props))
        if signature in seen:
            continue
        seen.add(signature)

        short = query.short_name(c, root)
        spec = Spec(f"stress::TM_{short}Empty", f"/stress/tm_{short.lower()}",
                    f"Emptiness of {len(props)} to-many association(s) on {root}. Both "
                    f"`isEmpty` and `isNotEmpty` are projected for each end: asserting one "
                    f"alone would let a constant answer pass, and asserting both means the "
                    f"service is green only where the two disagree row by row. The corpus "
                    f"declares 177 to-many ends and traversed twelve; `count()` cannot carry "
                    f"the volume because F6 makes it return 1 over an empty set, and these "
                    f"predicates are correct there. Generated by scripts/corpus/tomany.py.",
                    root)
        projs = [Proj(ident, [ident])]
        for prop in props:
            projs.append(Proj(f"{prop}Empty", [prop], agg="isEmpty"))
            projs.append(Proj(f"{prop}Any", [prop], agg="isNotEmpty"))
        spec.projections = projs
        spec.sort = (ident, False)
        out.append(spec)
    return out


if __name__ == "__main__":
    import flat

    c = model.load()
    tables = flat.all_tables(c)
    seeded = {k for k, v in tables.items() if v}
    ends = usable_ends(c, tables, seeded)
    specs = build(c, seeded, tables)
    total = sum(len(v) for v in ends.values())
    print(f"usable to-many ends: {total} over {len(ends)} classes")
    print(f"services: {len(specs)}, covering "
          f"{sum((len(s.projections) - 1) // 2 for s in specs)} ends")
    import oracle
    empties = 0
    for s in specs:
        for row in oracle.evaluate(c, s, tables):
            empties += sum(1 for k, v in row.items() if k.endswith("Empty") and v is True)
    print(f"EMPTY to-many cells asserted: {empties}  <- the case F6 makes count() get wrong")
