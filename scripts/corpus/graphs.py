"""
Generates graph-fetch instances across the whole model.

The four hand-written GRAPH specs in battery.py each pin a PROPERTY -- nested to-one
navigation, an Operation union with an empty leg, an enum that raises on one execution path
and returns NULL on another, a milestoned class at %latest. Those stay hand-written: each
encodes a piece of knowledge about what graph fetch is supposed to do, and no generator
knows any of it.

What they do not do is scale. They cover four roots out of 209 seeded, because a human
picked four. This generates the INSTANCES those properties should hold over -- tree-shaped
reads across every root the model offers -- while battery.py keeps the properties.

The division is deliberate and is the same one stacks.py makes for projections: hand-write
the judgement, generate the volume. A generated tree asserts only that graph fetch agrees
with the oracle for that shape; it cannot tell you WHY a disagreement matters. When one
fails, the hand-written probe next to it is what explains it.

Two exclusions, both because generating a test that is known to fail buys nothing:

  * ENUM-MAPPED properties. F10: graph fetch RAISES on a source code with no
    EnumerationMapping entry where a TDS projection returns NULL. Every expanded table
    carries such a row by construction, so including enums would generate a corpus of
    uniform, already-understood failures. G3 pins that deliberately, once.

  * TO-MANY navigation. F6: count() over an empty to-many returns 1 rather than 0, and A3
    guarantees a childless parent in every expanded table. Trees stay to-one until F6 is
    fixed; the moment it is, lifting this restriction is a one-line change and the fan-out
    is large.
"""
from __future__ import annotations

import model
import query
from query import Spec

# Bounded like stacks.MAX_ROOTS: every root costs a service and the suite runs per commit.
# The ranking decides which roots, so widening the seed widens the corpus rather than
# needing a new entry here.
MAX_ROOTS = 60

# A tree with one scalar is a projection wearing a tree's clothes -- it exercises none of
# the sub-object assembly that makes graph fetch different from a flat read.
MIN_SCALARS = 2
MAX_SCALARS = 4
MAX_BRANCHES = 2


def _scalars(c: model.Corpus, cls: str, limit: int,
             tables: dict[str, list[dict]] | None = None) -> list[str]:
    """Scalar properties of `cls`, identifier first, enum-mapped ones excluded.

    Identifier first so a failure diff names the row a human can find, and so the tree reads
    like one somebody would write.

    Also excluded: a property declared `[1]` whose column actually holds a NULL in the
    seeded rows. Graph fetch ENFORCES multiplicity -- "Property of multiplicity [1] can not
    be null" -- where a TDS projection of the same column returns the null happily. The
    engine is right to complain; the model and the data disagree, and generating a tree over
    that combination tests the disagreement rather than graph fetch. The adversarial nulls
    are deliberate, so the generator avoids the required properties instead of the seed
    avoiding the nulls.
    """
    cols = c.columns.get(cls, {})
    table = c.tables.get(c.main_table.get(cls, ""))
    if table is None:
        return []
    enum_props = {p for (owner, p) in c.enum_props if owner == cls}
    declared = c.classes[cls].props if cls in c.classes else {}

    def nullable_conflict(prop: str, col: str) -> bool:
        d = declared.get(prop)
        if d is None or d.lower < 1 or tables is None:
            return False
        return any(r.get(col) is None for r in tables.get(table.name, []))

    pk = table.pk[0] if table.pk else None
    ident = next((p for p, col in cols.items() if col == pk), None)
    rest = sorted(p for p, col in cols.items()
                  if p != ident and p not in enum_props and col in table.columns
                  and not nullable_conflict(p, col))
    out = ([ident] if ident else []) + rest
    return out[:limit]


def _branches(c: model.Corpus, root: str, seeded: set[str]) -> list[tuple[str, str]]:
    """(property, target class) for each to-one navigation from `root` onto a seeded table.

    to-one only -- see the module docstring on F6.
    """
    out = []
    for (owner, name), end in sorted(c.ends.items()):
        if owner != root or end.to_many or not model.walked(c, end):
            continue
        if c.main_table.get(end.target) not in seeded:
            continue
        # F55: a graphFetch sub-object whose target set was contributed by an INCLUDED
        # mapping dies at test-suite initialisation with "RelationalPropertyMapping cannot
        # be cast to XStorePropertyMapping", taking its whole batch with it -- so this is
        # removed from the corpus rather than quarantined, which is what a fatal-at-init
        # defect always costs. The same edge PROJECTS correctly, and D_FeeScheduleDense
        # reads two hops of it, so only the tree form is given up.
        if c.declared_in.get(end.target) in model.mapping_closure(
                c, c.declared_in.get(root, "")):
            continue
        out.append((name, end.target))
    return out


# The generated services below build over the trading DOMAIN. The combination matrix and the
# hier:: feature domain are FIXTURES for mapping constructs -- they carry their own
# generators, their own runtimes and their own data elements -- so sweeping them in here
# couples every matrix change to the domain service set, and did: adding an association to
# the matrix made a graph fetch over combo::C0 a generated root, which fails plan generation
# with "Only one return type should be selected during Serialization Class generation" and
# blocks its whole batch.
FIXTURE_DOMAINS = ("combo::", "hier::")


def _is_fixture(cls: str) -> bool:
    return cls.startswith(FIXTURE_DOMAINS)


def roots(c: model.Corpus, seeded: set[str],
          tables: dict[str, list[dict]] | None = None) -> list[str]:
    """Every class worth fetching, richest first.

    Ranked by how much TREE the class can produce -- branches first, then scalars -- so the
    generated corpus leads with the deepest object graphs rather than with whatever sorts
    first alphabetically.
    """
    scored = []
    for cls, table in c.main_table.items():
        if table not in seeded or cls in c.views or _is_fixture(cls):
            continue
        scalars = _scalars(c, cls, MAX_SCALARS, tables)
        if len(scalars) < MIN_SCALARS:
            continue
        branches = _branches(c, cls, seeded)
        scored.append(((len(branches), len(scalars)), cls))
    scored.sort(key=lambda s: (-s[0][0], -s[0][1], s[1]))
    return [cls for _, cls in scored[:MAX_ROOTS]]


def build(c: model.Corpus, seeded: set[str],
          tables: dict[str, list[dict]] | None = None) -> list[Spec]:
    """One graph-fetch spec per root, deduplicated by tree SHAPE.

    Two roots that reach the same targets by the same property names produce the same test
    twice, which is the failure mode stacks.py had to fix after emitting four
    interchangeable services. The signature here is the full nested key structure, so a tree
    counts as new when it reads something new -- not merely when it starts somewhere new.
    """
    specs, seen = [], set()
    for n, root in enumerate(roots(c, seeded, tables)):
        # Subtypes are skipped. A subclass inherits its supertype's associations -- the
        # reader propagates them, correctly -- so every one of the 58 derivative subtypes
        # suddenly offered `legs` and `optionTerms` and this generator produced a service
        # for each. They add nothing: the association is the SAME association over the same
        # table, and the base already has a service for it.
        #
        # They also do not run. Navigating an inherited association from a subtype set fails
        # with "Void not supported!" -- the association mapping names the BASE's set id, and
        # a query rooted at a subtype set is not it. The taxonomy services reach the same
        # ends without trouble, so this is a property of these generated shapes rather than
        # of the model.
        if c.classes.get(root) is not None and c.classes[root].supertype:
            continue
        tree: dict = {p: None for p in _scalars(c, root, MAX_SCALARS, tables)}
        if len(tree) < MIN_SCALARS:
            continue
        for prop, target in _branches(c, root, seeded)[:MAX_BRANCHES]:
            sub = _scalars(c, target, 3, tables)
            if len(sub) >= 2:
                tree[prop] = {p: None for p in sub}
                # A THIRD level, where the model offers one. Every generated tree was two
                # deep, which is the shallowest shape that still counts as a tree -- and the
                # question graph fetch actually raises is what happens to a sub-object of a
                # sub-object when the middle one is absent. Two levels cannot ask it: there
                # is nothing under the missing object to be missing.
                for sub_prop, sub_target in _branches(c, target, seeded)[:1]:
                    leaves = _scalars(c, sub_target, 2, tables)
                    if len(leaves) >= 2:
                        tree[prop][sub_prop] = {q: None for q in leaves}
        if not any(isinstance(v, dict) for v in tree.values()):
            # No sub-object: a flat read, already covered far better by the projection
            # corpus. Graph fetch earns its place only where a tree exists.
            continue

        signature = _signature(tree)
        if signature in seen:
            continue
        seen.add(signature)

        short = query.short_name(c, root)
        # Named from the CLASS, not from a loop index. An index renumbers every
        # service downstream of it the moment the ranking changes -- and the
        # ranking is derived from the seed, so a data change silently repoints
        # every quarantine entry that keys on the name. That has already happened
        # once (F26); the quarantine below keys on ten of these names.
        spec = Spec(f"stress::GG_{short}Tree", f"/stress/gg_{short.lower()}",
                    f"Generated graph fetch on {root}: {len(tree)} keys, "
                    f"{sum(1 for v in tree.values() if isinstance(v, dict))} sub-object(s). "
                    f"Tree-shaped read of the same rows the projection corpus reads flat -- "
                    f"the two must agree with the oracle independently, because a sub-object "
                    f"that is wholly absent and one whose every field is null are the same "
                    f"row in a projection and different objects in a tree. Generated by "
                    f"scripts/corpus/graphs.py; the PROPERTIES it is instantiating are the "
                    f"hand-written G0-G3 in battery.py.", root)
        spec.graph = tree
        query.apply_temporal(c, spec)
        specs.append(spec)
    return specs


def spine_targets(c: model.Corpus, seeded: set[str], tables) -> list:
    """Every sub-object a generated tree fetches, down to its deepest: spines.py gives each roots
    whose object there exists, so no tree is hollow at the bottom."""
    import spines
    import stacks

    out = []
    for s in build(c, seeded, tables):
        def walk(node, path, cls):
            for prop, sub in node.items():
                if not isinstance(sub, dict):
                    continue
                tgt = c.ends[(cls, prop)].target
                leaf = stacks._identifier(c, tgt) or next(
                    (k for k, v in sub.items() if v is None), None)
                if leaf is not None:
                    out.append(spines.Target(s.root, tuple(path + [prop, leaf]), s.as_of, s.short,
                                              filtered=False))
                walk(sub, path + [prop], tgt)
        walk(s.graph, [], s.root)
    return out


def _signature(tree: dict) -> tuple:
    return tuple(sorted((k, _signature(v) if isinstance(v, dict) else None)
                        for k, v in tree.items()))


if __name__ == "__main__":
    import flat

    c = model.load()
    tables = flat.all_tables(c)
    seeded = {t for t, rows in tables.items() if rows}
    all_roots = roots(c, seeded, tables)
    specs = build(c, seeded, tables)
    print(f"roots ranked: {len(all_roots)} (cap {MAX_ROOTS})")
    print(f"specs after shape dedupe: {len(specs)}")
    for s in specs[:5]:
        branches = sum(1 for v in s.graph.values() if isinstance(v, dict))
        print(f"  {s.name.split('::')[-1]:<34}{len(s.graph)} keys, {branches} sub-object(s)")
