"""
Generates DENSE services — many features in one query — instead of one probe per feature.

The corpus grew as a set of narrow probes: median 2 features per service. That is right
for ATTRIBUTION (F10, F12, F13 and F15 are each reportable because they were isolated) and
wrong as the whole corpus, because a defect that only appears when several features
interact is invisible to a probe by construction. F14 is the proof: groupBy over a
many-to-one enum mapping, findable only by stacking.

So this generates the other layer. For each root class it takes what the MODEL actually
offers — the deepest navigation chains, any enum-mapped column, any derived property —
and stacks them into one query with a filter, a sort and a limit on top. Generated from
the model rather than hand-written, so it grows as the model does instead of needing a new
hand-authored service per domain.

Two rules keep the output meaningful:

  * Nothing quarantined goes in. A stack containing a known defect is red for a reason
    nobody has to look for, and masks every interaction it exists to find. Aggregates over
    a to-many are therefore excluded while F6 stands, and groupBy over an enum while F14
    does.
  * A service must reach MIN_FEATURES or it is not emitted at all. A "stack" of three
    features is a probe wearing a bigger name.
"""
from __future__ import annotations

import model
import query
from query import Pred, Proj, Spec

MIN_FEATURES = 5

# How many roots to stack on. Ranked by what the model offers from each, so widening the
# seed automatically widens the corpus instead of needing a new entry here.
# Bounded, not because more would be wrong, but because every root costs a service and
# the suite is run per commit. Raised as the seed widens; the ranking decides which.
# 45 -> 120 when the expansion reached all 210 tables: 86 roots now qualify, and the cap
# was silently discarding the bottom 41. Output SATURATES at 45 services well before the
# cap binds -- the signature dedupe and MIN_FEATURES do the real limiting -- so the number
# here is headroom rather than a target, and raising it further changes nothing until the
# model grows again.
MAX_ROOTS = 120


# A literal per parameter type, for a generated qualified-property call. Deliberately not a
# default-with-fallback for unknown types: an unknown type gets 0.5 and will fail loudly in
# the oracle, which is better than a plausible value that makes a wrong call look right.
_ARG_FOR = {"String": "BBG", "Float": 0.5, "Integer": 2, "Boolean": True}


def _identifier(c: model.Corpus, root: str) -> str | None:
    """The property mapped to the root table's primary key — used as the stable sort key
    and the first projected column, so a generated service reads like one a person would
    write."""
    ids = _identifiers(c, root)
    return ids[0] if ids else None


def _identifiers(c: model.Corpus, root: str) -> list[str]:
    """Every property mapped to a primary-key column, in key order.

    The first column alone was the sort key until a class with a THREE-column key arrived.
    A curve pillar is keyed by curve, date and tenor, so ordering by the curve leaves 24
    rows tied and `->limit(25)` cuts through the middle of a tie -- which the oracle refuses
    outright rather than picking a winner the database has not promised.
    """
    table = c.tables.get(c.main_table.get(root, ""))
    if table is None or not table.pk:
        return []
    cols = c.columns.get(root, {})
    out = []
    for pk in table.pk:
        prop = next((p for p, col in cols.items() if col == pk), None)
        if prop is None:
            # A key column with no property is not orderable from a query, so the key
            # cannot be reconstructed and this root has no stable order.
            return []
        out.append(prop)
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


def roots(c: model.Corpus, seeded: set[str]) -> list[tuple[str, str]]:
    """Every class worth stacking on, richest first.

    Derived rather than listed: the ranking is how many DISTINCT navigation targets the
    class can reach, so seeding another domain promotes its classes automatically.
    """
    scored = []
    for cls, table in c.main_table.items():
        if table not in seeded or cls in c.views or _is_fixture(cls):
            continue
        ident = _identifier(c, cls)
        if ident is None:
            continue
        chains = _chains(c, cls, seeded)
        targets = {t for _, t in chains}
        deep = sum(1 for p, _ in chains if len(p) >= 3)
        if len(targets) < 2:
            continue                      # nothing to stack
        scored.append(((len(targets), deep), cls, ident))
    scored.sort(key=lambda s: (-s[0][0], -s[0][1], s[1]))
    return [(cls, ident) for _, cls, ident in scored[:MAX_ROOTS]]

_LABELLISH = ("name", "legalname", "region", "status", "currency", "jurisdiction")


def _inherited_ends(c: model.Corpus, cls: str) -> set[str]:
    """End names `cls` gets from a supertype rather than declaring itself, and CANNOT use.

    The reader propagates a supertype's ends onto every subclass, correctly -- Pure does the
    same. What it cannot propagate is an ASSOCIATION's mapping: there is one association, its
    ends already name a source and a target set, and a query rooted at the subtype is not
    that set. That is F49, and it is fatal at test-suite initialisation.

    A property mapped over a JOIN is different and is NOT excluded. The subtype's set
    `extends` the parent's and inherits its property mappings, and navigating one from the
    subtype works -- proved directly against the engine before relying on it. The two are
    told apart by `assoc`, which a property-over-join end leaves None.

    Excluding both was costing 1968 of 2215 seeded roots their entire reach: the taxonomies
    carry their edge on the base class, so every subtype gets it by inheritance and nothing
    else.
    """
    out, parent = set(), c.classes[cls].supertype if cls in c.classes else None
    while parent:
        out |= {n for (owner, n), e in c.ends.items() if owner == parent and e.assoc}
        parent = c.classes[parent].supertype if parent in c.classes else None
    return out


def _chains(c: model.Corpus, root: str, seeded: set[str], depth: int = 3):
    """Every to-one navigation path from `root`, deepest first, whose every hop lands on a
    seeded table. Deepest-first so the densest service takes the longest chains."""
    out, frontier = [], [([], root)]
    for _ in range(depth):
        nxt = []
        for path, cls in frontier:
            # An end INHERITED from a supertype cannot be navigated from a subtype set: the
            # association names the base's set id and a query rooted at the subtype is not it
            # (F49). That is fatal at test-suite INITIALISATION rather than a failure, so it
            # kills the whole batch -- forty unrelated services reported MISSING when a
            # generated dense service over `curves::QuotedPillar` first reached one.
            #
            # graphs.py and tomany.py skip subtype ROOTS entirely for this reason. That is too
            # blunt here: a subtype's OWN ends are fine, and this generator's whole value is
            # the depth it reaches. So the exclusion is per-END rather than per-root.
            inherited = _inherited_ends(c, cls)
            for (owner, name), end in c.ends.items():
                if owner != cls or end.to_many or not end.join:
                    continue
                if name in inherited:
                    continue
                if c.main_table.get(end.target) not in seeded:
                    continue
                out.append((path + [name], end.target))
                nxt.append((path + [name], end.target))
        frontier = nxt
    return sorted(out, key=lambda p: -len(p[0]))


def _leaf(c: model.Corpus, cls: str, kind: str = "string", not_null: bool = False):
    """A scalar property on `cls`, preferring one that reads like a label so the generated
    query stays legible to a human reviewing the diff.

    `not_null` restricts the choice to columns the DDL forbids a NULL in. Only 2 of this
    model's 86 roots have one, so it is not usable for choosing a filter column -- see the
    comment at the filter itself.
    """
    cols = c.columns.get(cls, {})
    table = c.tables.get(c.main_table.get(cls, ""))
    if table is None:
        return None
    fallback = None
    props = c.classes[cls].props if cls in c.classes else {}
    for prop, col in sorted(cols.items()):
        if table.columns[col].kind != kind:
            continue
        # The COLUMN being a string does not make the PROPERTY one. An EnumerationMapping
        # reads an enum off a CHAR(3), so `feeCurrency` is a `CtCurrency` over a string
        # column -- and filtering it with `> ' '` fails to compile with
        # `Can't find a match for function 'greaterThan(CtCurrency[1],String[1])'`, which
        # takes the whole file down before any service runs. spread.py already chooses by
        # DECLARED type for the same reason; this is the other half of that lesson, found
        # when a linked project first brought an enum over a string column into range.
        pr = props.get(prop)
        if pr is not None and pr.type in c.enums:
            continue
        if not_null and not table.columns[col].not_null:
            continue
        if prop.lower() in _LABELLISH:
            return prop
        fallback = fallback or prop
    return fallback


def _alias(path: list[str], leaf: str) -> str:
    parts = path + [leaf]
    head, *rest = parts
    return head + "".join(p[:1].upper() + p[1:] for p in rest)


def _unique_aliases(projections: list[Proj]) -> list[Proj]:
    """Make every alias in one projection list distinct.

    `_alias` builds a name from the path, which is unique per path -- but two DIFFERENT paths
    can flatten to the same name, and the engine rejects the result at compile with
    "The relation contains duplicates: [bucketBand]", naming the alias and neither path.
    It surfaced the moment the linked project put two routes to the same leaf in range.

    Renaming the later one rather than dropping it: both paths are worth projecting, and a
    generated service that quietly reads one of two chains is the shape this corpus exists to
    avoid.
    """
    seen, out = {}, []
    for p in projections:
        n = seen.get(p.alias, 0)
        seen[p.alias] = n + 1
        out.append(p if n == 0 else Proj(f"{p.alias}{n + 1}", p.path, p.agg, p.args, p.func))
    return out


def build(c: model.Corpus, seeded: set[str], tables) -> list[Spec]:
    """The dense services, each filter comparing against a value of its column and each limit
    cutting rows (the improvement plan's Phase 2, selective.py)."""
    import selective

    specs = _build(c, seeded)
    for spec in specs:
        selective.tune(c, spec, tables)
    return specs


def spine_targets(c: model.Corpus, seeded: set[str], tables) -> list:
    """Every navigated column a dense service projects: spines.py gives each paths that arrive,
    so a service that reads four chains reads four values."""
    import spines

    return [spines.Target(s.root, tuple(p.path), s.as_of, s.short, filtered=False)
            for s in _build(c, seeded)
            for p in s.projections if len(p.path) > 1 and not p.agg and not p.func]


def _build(c: model.Corpus, seeded: set[str]) -> list[Spec]:
    """Generated services must DIFFER from one another, not merely exist.

    The first version of this emitted four services whose navigation was identical --
    every root reaches `trade`, and the deepest chains from trade are the same, so
    Settlement, Confirmation, TradeReport and SalesCredit all became
    trade.instrument.sector.name and friends. That is precisely the failure mode of the
    v1 corpus (10,800 cases, ~75% interchangeable), reproduced by a generator instead of
    by hand.

    Two guards: chains already used by an earlier service are DEPRIORITISED so each root
    reaches somewhere new, and a service whose complete path-set duplicates an earlier one
    is dropped rather than emitted.
    """
    specs = []
    seen_signatures: set[tuple] = set()
    used_chains: set[tuple] = set()
    for n, (root, ident) in enumerate(roots(c, seeded)):
        if c.main_table.get(root) not in seeded:
            continue
        chains = _chains(c, root, seeded)
        if not chains:
            continue
        # Prefer chains no earlier service has taken; keep depth as the tiebreak.
        chains.sort(key=lambda pc: (tuple(pc[0]) in used_chains, -len(pc[0])))

        # Every key column, not just the first: they are what the sort orders by, and a
        # sort on an unprojected column is not expressible.
        key = _identifiers(c, root)
        projections = [Proj(k, [k]) for k in key]
        used_targets: set[str] = set()
        features: set[str] = set()

        # Up to three DISTINCT chains — distinct targets, so the service exercises several
        # join trees rather than the same tree three times.
        for path, target in chains:
            if len(projections) > 3 or target in used_targets:
                continue
            leaf = _leaf(c, target)
            if leaf is None:
                continue
            projections.append(Proj(_alias(path, leaf), path + [leaf]))
            used_targets.add(target)
            used_chains.add(tuple(path))
            features.add("navigation")
            if len(path) >= 3:
                features.add("deepNavigation")

        # An enum-mapped column anywhere along the way, if one is reachable.
        for path, target in chains:
            enum_prop = next((p for (cls_, p) in c.enum_props if cls_ == target), None)
            if enum_prop and len(projections) < 8:
                projections.append(Proj(_alias(path, enum_prop), path + [enum_prop]))
                features.add("enum")
                break

        # A derived property, on the root or on anything it reaches.
        for path, target in [([], root)] + chains:
            derived = c.classes.get(target)
            name = next((n for n, d in derived.derived.items() if not d.params), None) \
                if derived else None
            if name and len(projections) < 9:
                projections.append(Proj(_alias(path, name), path + [name]))
                features.add("derivedProperty")
                break

        # A QUALIFIED property -- derived AND taking a parameter -- on the root or anything
        # it reaches. Projected alongside the plain derived one rather than instead of it:
        # they are different constructs, and the scoreboard counts them separately because
        # the engine plans them differently.
        for path, tgt in [([], root)] + chains:
            k = c.classes.get(tgt)
            qual = next((d for d in (k.derived.values() if k else []) if d.params), None)
            if qual and len(projections) < 10:
                # An argument of the DECLARED type. This was 0.5 for everything, which is
                # right for the Float parameters that were the only ones the generator could
                # reach until a String one came into range -- and then it produced
                # `tickerOn(0.5)`, which the oracle hit as `float + str` inside expression
                # evaluation, naming neither the property nor this line.
                args = [_ARG_FOR.get(ptype, 0.5) for _pname, ptype in qual.params]
                projections.append(Proj(_alias(path, qual.name) + "Q",
                                        path + [qual.name], args=args))
                features.add("qualifiedProperty")
                break

        # Named from the CLASS, not the loop index -- the ranking is derived from the seed,
        # so a data change renumbers every service after the one that moved and silently
        # repoints any quarantine entry keyed on the name. The graph and aggregate
        # generators carried the same bug; this was the last one.
        short = query.short_name(c, root)
        spec = Spec(f"stress::D_{short}Dense", f"/stress/d_{short.lower()}",
                    f"Dense stack on {root}: several distinct navigation chains plus "
                    f"whatever the model offers along them, with a filter, a sort and a "
                    f"limit. Generated from the model by scripts/corpus/stacks.py, so it "
                    f"deepens as the model does rather than needing a new hand-written "
                    f"service per domain.", root)
        spec.projections = _unique_aliases(projections)

        scalar = _leaf(c, root)
        if scalar:
            # `> ' '` rather than `!= ' none'`, for two reasons.
            #
            # It is a REAL filter: every seeded string sorts above a single space, so it
            # keeps the non-null rows and EXCLUDES the null ones. The old predicate was
            # chosen to exclude nothing, which exercises the predicate path but asserts
            # nothing about what a predicate does.
            #
            # And it drops the NULL rows, where `!=` would keep them: Pure compares an
            # absent value as false and `!=` as true (oracle._cmp). Once the seeder nulled
            # every nullable column, thirty of these services diverged at once on that rule
            # while the oracle read `!=` as SQL does (F28, withdrawn 2026-10-23). The rule is
            # pinned by CB_NotEqualsNull and its two companions and by the typed services
            # (literals.py); these keep to `>`, the split aggregates.py makes around F6.
            #
            # Written as the guard; build() then compares against a value of the column
            # instead, so the filter also drops some present rows (selective.py, decision D5).
            spec.filters = [Pred([scalar], ">", " ")]
            features.add("queryFilter")
        query.apply_temporal(c, spec)
        spec.sort = [(k, False) for k in key]
        spec.limit = 25
        features.update({"sort", "limit"})

        signature = tuple(sorted(".".join(p.path) for p in projections[1:]))
        if signature in seen_signatures:
            continue        # an interchangeable service is not a second test
        if len(features) >= MIN_FEATURES:
            seen_signatures.add(signature)
            specs.append(spec)
    return specs
