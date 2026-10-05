"""
Coverage-directed generation: services chosen for the COMBINATIONS they close.

Every other generator here is driven by the model -- one service per root, per to-many end,
per graph shape. That fills breadth and saturates: once each construct has a service, adding
more of the same moves nothing. The stacking scoreboard is where that shows up, and it read

    PAIR COVERAGE    175 of 820   (21%)
    TRIPLE COVERAGE  422 of 10660  (4%)

with sixteen constructs appearing in one or two services apiece. A construct in one service
can participate in at most `depth - 1` pairs however important it is, so `View`, `XStore`,
`Otherwise`, `Inline`, `~distinct` and the rest were each contributing two or three of a
possible forty.

This generator is driven by the SCOREBOARD instead. It computes what the corpus already
covers, enumerates candidate services, scores each by how many uncovered pairs and triples it
would close, and takes the best one repeatedly. That is greedy set-cover, which is not optimal
and is within a log factor of optimal, and the difference does not matter next to the gap it
is closing.

Two design points worth stating, because both were mistakes first.

Candidates are built by taking an EXISTING service as a template and changing only the query
shape. The rare constructs are not reachable from a bare root class -- `XStore` needs
`external::EntityMapping` with `stress::XStoreRT`, `Otherwise` needs the combo mapping with
its own data element and connection id -- and rediscovering that wiring per construct would
be guesswork. The wiring is already correct in the service that first exercised it, so it is
inherited wholesale and only the shape varies.

And a candidate is scored on pairs AND triples, weighted. Scoring on pairs alone produced a
run of services that each closed two or three pairs and no triples, because the cheapest way
to close a pair is a shallow service; the triple term is what pushes the selection toward
deep stacks, which is the thing the scoreboard was built to measure in the first place.
"""
from __future__ import annotations

import itertools
from collections import Counter
from dataclasses import replace

import executed
import model
import stacking
import stacks
from query import Pred, Proj, Spec

# How many services to emit. Every one costs suite time, so this is a budget rather than a
# target -- the loop stops early when nothing left to add closes anything.
BUDGET = 90

# A candidate must close at least this many new combinations to be worth a service. One new
# pair for one more service is a bad trade; the scoreboard would rise and the suite would get
# slower for a case nobody chose deliberately.
MIN_GAIN = 1

# Triples are worth less than pairs individually and there are thirteen times as many, so
# without a weight the triple term swamps the pair term and the selection stops caring about
# pairs at all. A third was picked by trying 1, 1/3 and 1/10 and keeping the one whose
# selections closed the most of both.
TRIPLE_WEIGHT = 1 / 3

# (mapping short name, shape tag) -> why the engine will not run it.
#
# Established by generating the shape, running it, and reading what came back -- not by
# guessing which combinations look risky. The oracle validation in _evaluable catches what
# the CORPUS cannot express; these are what the ENGINE cannot execute, and the two are
# different lists. Left here with their messages rather than silently skipped, because
# "graph fetch does not work over a relation-function set" is a fact about the engine worth
# keeping even though the corpus's response to it is to stop asking.
EXCLUDE = {
    ("Join", "Graph"):
        "graph fetch over a set backed by a Relation ~func: 'Relation function for set "
        "mjBook in mapping modeljoin::JoinMapping'",
    ("Otherwise", "Graph"):
        "graph fetch over a set with an Otherwise fallback errors during plan generation",
    # Two hier entries remain, with the reason the engine actually gives:
    #
    #   No test data provided for connection type 'RelationalDatabaseConnection'
    #
    # The Filter shape reaches a connection the template's ###Data element does not cover.
    # The IssuerProd GRAPH shape passes and is no longer excluded; the Issuer one does not,
    # so the exclusion is per (mapping, shape) rather than per mapping.
    ("Issuer", "Filter"): "the shape reaches a connection the template's data element does "
                          "not seed: 'No test data provided for connection type'",
    ("Issuer", "Graph"): "as Issuer/Filter -- the same uncovered connection",
    ("IssuerProd", "Filter"): "as Issuer",

    # The other two entries that used to sit here are GONE, and their reason was wrong.
    # "The hier region's test data does not survive this shape" was a description of the
    # symptom; the cause was HIER_ISSUER.COUNTRY_CODE declared VARCHAR(8) while the seed's
    # adversarial dangling-FK marker 'HC-0001-GONE' is twelve characters. Any shape that
    # drove the insert hit it, and the shapes that did not simply never inserted that row.
    # Widening the column fixed all four. model.check now catches this class of thing at the
    # seed rather than as a JDBC error inside an unrelated service.
}


def _templates(c: model.Corpus, specs) -> list[Spec]:
    """One representative service per (root, mapping), preferring the deepest.

    Deduped because the combination matrix alone contributes sixteen services over one
    mapping, and sixteen templates that differ only in root class would generate sixteen
    near-identical candidates and crowd the selection.
    """
    byclass = executed.class_features()
    bymapping = executed.mapping_features()
    closure = executed.include_closure()
    best: dict[tuple, tuple[int, Spec]] = {}
    for s in specs:
        key = (s.root, getattr(s, "mapping", None))
        depth = len(stacking.spec_features(c, s, byclass, bymapping, closure))
        if key not in best or depth > best[key][0]:
            best[key] = (depth, s)
    return [s for _d, s in best.values()]


def _mapped_classes(mapping: str | None) -> set[str]:
    """Every class the mapping maps, following includes. None means the corpus default."""
    byclass = executed.class_features()
    closure = executed.include_closure()
    mp = mapping or executed.DEFAULT_MAPPING
    ms = closure.get(mp, {mp})
    return {k for (m, k) in byclass if m in ms}


def _path_mapped(c: model.Corpus, root: str, path: list[str], mapped: set[str]) -> bool:
    """Every INTERMEDIATE class on the way is mapped too, not just the destination."""
    cls = root
    for step in path:
        end = c.ends.get((cls, step))
        nxt = end.target if end else c.embedded.get((cls, step))
        if nxt is None:
            return False
        if nxt not in mapped:
            return False
        cls = nxt
    return True


def _shape_variants(c: model.Corpus, t: Spec, seeded: set[str],
                    tables_for_ends=None) -> list[Spec]:
    """Query-shape variants of one template, built ONLY from paths the template proves.

    The first two versions took the template's mapping and then chose properties and chains
    from the MODEL. Both failed at execution, one level apart:

        The class 'Sector' can't be found in the mapping 'InlineFlatMapping'
        The system can't find a mapping for the property 'trading::Trade.commission'
            in the mapping 'reporting::InlineFlatMapping'

    A narrow mapping maps a subset of classes AND a subset of each class's properties, and
    the corpus reader keys a property's column by CLASS rather than by (mapping, class), so
    it cannot answer which subset. The template can: every path it already projects is one
    that mapping demonstrably resolves. So the reach is INHERITED wholesale and only the
    SHAPE varies, which is what the module docstring claimed all along and what neither
    earlier version actually did.
    """
    root = t.root
    tproj = [p for p in getattr(t, "projections", []) if p.path and not p.agg]
    if not tproj:
        return []
    ident = tproj[0].alias
    base_kw = dict(mapping=getattr(t, "mapping", None), runtime=getattr(t, "runtime", None),
                   data_element=getattr(t, "data_element", None),
                   connection=getattr(t, "connection", None))
    out: list[Spec] = []

    def mk(tag: str, **kw) -> Spec:
        # The name carries the MAPPING as well as the root: trading::Trade is mapped by
        # external::EntityMapping (XStore), reporting::InlineFlatMapping (Inline) and the
        # default one, so keying on the root alone collided exactly the rare-construct
        # candidates this exists to reach.
        short = root.split("::")[-1]
        mshort = (base_kw["mapping"] or "def").split("::")[-1].replace("Mapping", "")
        if (mshort, tag) in EXCLUDE:
            return None
        s = Spec(f"stress::SP_{mshort}{short}{tag}",
                 f"/stress/sp_{mshort.lower()}{short.lower()}{tag.lower()}",
                 f"Coverage-directed stack on {root} ({tag}), reusing the reach of "
                 f"{t.name.split('::')[-1]} and changing the query SHAPE. Generated by "
                 f"scripts/corpus/spread.py, which picks services by the feature "
                 f"combinations they close rather than by walking the model.", root,
                 **base_kw)
        for k, v in kw.items():
            setattr(s, k, v)
        return s

    def add(tag, **kw):
        s = mk(tag, **kw)
        if s is not None:
            out.append(s)

    def clone(ps):
        return [Proj(p.alias, list(p.path), p.agg, list(p.args), p.func) for p in ps]

    def enriched():
        """The template's reach, widened with more of the model where that is SAFE.

        Safe means the default mapping, which maps every class and every property, so the
        reader's column table is authoritative there. For a narrow mapping it is not -- the
        reader keys columns by class rather than by (mapping, class) -- and reaching beyond
        the template is what produced "can't be found in the mapping" twice.

        This is what unlocks the aggregate shapes. tomany.py emits forty services carrying
        `emptiness over to-many`, and each projects an identifier and the aggregate and
        nothing else, so the construct that appears in forty services was paired with almost
        nothing. Widening the reach around it is the difference between a construct being
        present and a construct being COMBINED.
        """
        # The template's NON-aggregate reach. Cloning its aggregates too and then widening
        # the reach around them changes what they group over: adding a to-many chain fans the
        # rows out, and a count that was safe over the template's row set meets an empty
        # parent over the widened one -- which is F6, arriving as a failure of a service that
        # was not trying to test F6. Each variant adds the ONE aggregate it is about.
        ps = [p for p in clone(getattr(t, "projections", [])) if not p.agg]
        if getattr(t, "mapping", None) not in (None, executed.DEFAULT_MAPPING):
            return ps
        have = {tuple(p.path) for p in ps}
        targets = set()
        for path, tgt in stacks._chains(c, root, seeded):
            if len(ps) >= 6 or tuple(path) in have or tgt in targets:
                continue
            leaf = stacks._leaf(c, tgt)
            if not leaf or tuple(path + [leaf]) in have:
                continue
            ps.append(Proj(stacks._alias(path, leaf), path + [leaf]))
            targets.add(tgt)
        for path, tgt in [([], root)] + stacks._chains(c, root, seeded):
            # Plain derived properties only. A QUALIFIED one takes a parameter, and
            # projecting it with none fails the build -- "valuedAt takes 1 argument(s),
            # given 0". stacks.py projects those deliberately, with an argument.
            names = [n for n, d in c.classes[tgt].derived.items() if not d.params] \
                if tgt in c.classes else []
            if names and len(ps) < 8 and not any(p.path[-1] == names[0] for p in ps):
                ps.append(Proj(stacks._alias(path, names[0]), path + [names[0]]))
                break
        return ps

    # A string-typed leaf the template already projects, for the filter. `> ' '` keeps the
    # non-null rows and drops the nulls -- a real predicate -- and agrees with the engine
    # over NULL where `!=` does not (F28).
    def _is(kind, p):
        """The DECLARED type of the property a projection lands on.

        By declared type, not by the column's: the combo classes map a Boolean property from
        a VARCHAR source, so a column-kind test picked a Boolean for a string filter and the
        oracle compared a bool with ' '.
        """
        cls = c.owner_of(root, p.path)
        pr = c.classes[cls].props.get(p.path[-1]) if cls in c.classes else None
        return pr is not None and pr.type == kind

    strs = [p for p in tproj if len(p.path) == 1 and _is("String", p)]
    nums = [p for p in tproj if len(p.path) == 1 and (_is("Integer", p) or _is("Float", p))]
    filt = [Pred(list(strs[0].path), ">", " ")] if strs else []

    if filt:
        add("Filter", projections=enriched(), filters=filt,
            sort=(ident, False), limit=25)
    if getattr(t, "filters", None):
        # The template already filters; the variant is the SAME reach unfiltered, which is a
        # different shape and a different row set.
        add("Open", projections=enriched(), sort=(ident, False), limit=25)

    # groupBy. This shape had ONE service in the corpus and was missing 37 of its 40 possible
    # pairs -- more than any other construct -- for want of a second service.
    if nums:
        gb = mk("Group", projections=enriched(), filters=filt)
        if gb is not None:
            gb.group_by = [ident]
            gb.aggs = [("n", nums[0].alias, "count"), ("tot", nums[0].alias, "sum")]
            out.append(gb)

    # as-of, for a milestoned root. `latest` rather than a date, so the expectation does not
    # depend on which dates the seeder happened to choose.
    if root in c.classes and getattr(c.classes[root], "temporal", None) \
            and not getattr(t, "as_of", None):
        add("AsOf", projections=enriched(), sort=(ident, False), limit=25,
            as_of="latest")

    # Emptiness over a to-many end, stacked on the DEEP reach.
    #
    # This is the variant that matters most. `emptiness over to-many` appears in forty
    # services and was still missing 37 of its 40 pairs, because the forty are tomany.py's
    # and each projects an identifier and the aggregate and nothing else. Enriching those
    # services does not help either: _templates keeps the deepest service per root, so a
    # narrow aggregate service is never the template. Adding the aggregate to the deep
    # template is the way round -- it pairs emptiness with everything the deep stack already
    # touches, in one service.
    #
    # isEmpty and not count(): F6 makes count() return 1 over an empty set, so a count here
    # would put a known defect inside every stack this generator builds.
    # Default mapping only, for the same reason enriched() is: the association end exists in
    # the MODEL, and a narrow mapping need not map it. Taking it from c.ends and handing it to
    # InlineFlatMapping is the third time this exact mistake has failed the suite, so the
    # guard now sits on every place that reaches past the template.
    # And not over a join the planner cannot index on. F51: `isEmpty()` over a to-many whose
    # join is anything other than a single key equality returns the source row once per joined
    # row. tomany.py excludes the same shape; this generator reached the REVERSE end of the
    # same `or` join and produced the identical failure one commit later, which is why the
    # guard belongs on both rather than on whichever one failed first.
    default_map = getattr(t, "mapping", None) in (None, executed.DEFAULT_MAPPING)
    tm_end = next((prop for (cls_, prop), end in c.ends.items()
                   if cls_ == root and end.to_many and _key_joined(c, root, prop)),
                  None) if default_map else None
    if tm_end:
        add("Empty", projections=enriched() + [Proj("hasNone", [tm_end], agg="isEmpty")],
            filters=filt, sort=(ident, False), limit=25)

    # count() over a to-many end, stacked on the same deep reach -- but ONLY over an end
    # where every parent has children. aggregates.usable_ends is the existing answer to that
    # question and is reused rather than re-derived: F6 makes count() return 1 over an empty
    # set, so one childless parent fails the whole service, and the F-series pins that on the
    # empty half deliberately. `count over to-many` is the construct with the most open pairs
    # in the corpus, and it is open for exactly this reason -- the nine services that carry it
    # are narrow ones.
    import aggregates as _agg
    safe = (_agg.usable_ends(c, tables_for_ends, seeded).get(root, [])
            if tables_for_ends and default_map else [])
    if safe:
        add("Count", projections=enriched() + [Proj(f"{safe[0]}Count", [safe[0]],
                                                    agg="count")],
            filters=filt, sort=(ident, False), limit=25)

    # Graph fetch over the same reach: a different retrieval path entirely, and one no
    # rare-construct service uses.
    if not getattr(t, "graph", None) and root not in c.distinct_sets:
        # Not over a ~distinct set: graph fetch refuses one outright with "Store distinct not
        # allowed in graph fetch flow", which is a stated restriction rather than a defect --
        # there is nothing to pin and a generated service asserting it would only re-state
        # the assertion the engine already makes.
        #
        # It is worth excluding rather than quarantining because the failure is fatal to the
        # PROCESS: the exception leaves the test session half-initialised and the batch runs
        # to its one-hour timeout instead of reporting an error, taking forty unrelated
        # services with it.
        #
        # Only real ASSOCIATION hops become branches. An embedded property is a sub-object
        # on the same row, and graph fetch treats a branch as an association -- feeding it an
        # embedded hop fails the build with "combo::C9.nested is not an association".
        # No DateTime leaves. F24 makes a DateTime serialise WITH a UTC offset through a TDS
        # projection and WITHOUT one through graph fetch, so any generated tree containing
        # one fails for a defect that is already pinned by name elsewhere. Excluding it keeps
        # these services green and the coverage they report real; F24 loses nothing, since
        # the services that pin it are unaffected.
        tree = {}
        for p in tproj:
            if _is("DateTime", p):
                continue
            if len(p.path) == 1:
                tree[p.path[0]] = None
            elif len(p.path) == 2 and c.ends.get((root, p.path[0])) is not None:
                tree.setdefault(p.path[0], {})[p.path[1]] = None
        if len(tree) > 1 and all(isinstance(v, (dict, type(None))) for v in tree.values()):
            add("Graph", graph=tree)
    return out


def _key_joined(c, root: str, prop: str) -> bool:
    """True if every hop to `prop` is a single key equality the planner can index on.

    A general condition -- an inequality, a disjunction, a multi-column predicate, a {target}
    self-join -- records its columns as empty strings in this reader, which is exactly what
    "no key" looks like here.
    """
    try:
        hops, _t = c.resolve_assoc(root, [prop])
    except Exception:
        return False
    return bool(hops) and all(fcol for _j, _ft, fcol, _tt, _tc in hops)


def _score(feats: set[str], pairs: set, triples: set) -> float:
    p = sum(1 for x in itertools.combinations(sorted(feats), 2) if x not in pairs)
    t = sum(1 for x in itertools.combinations(sorted(feats), 3) if x not in triples)
    return p + TRIPLE_WEIGHT * t


def _evaluable(c: model.Corpus, spec: Spec, tables) -> bool:
    """Can the oracle produce an expectation for this candidate?

    Checked here rather than trusted, because a candidate the oracle cannot evaluate fails
    the BUILD -- not the suite -- and takes every other generated file down with it. Three
    separate shapes did exactly that in turn: a graph tree through an embedded hop, a graph
    leaf on a join-mapped column, and a filter on a computed Boolean. Each was a one-line
    guard, and each was found by the build dying rather than by the guard being anticipated.

    Validating instead makes the generator safe to widen: a shape that does not work is
    dropped silently, so adding a variant can only ever add coverage.
    """
    import oracle
    try:
        oracle.as_json_rows(c, spec, oracle.evaluate(c, spec, tables))
        return True
    except Exception:
        return False


# build.py and executed.all_specs ask for the same cover (Bazel workplan P2-05): computed once per corpus, seed set
# and baseline.
_BUILT: dict[tuple, list] = {}


def build(c: model.Corpus, seeded: set[str], existing, tables=None) -> list[Spec]:
    """Greedy set-cover over the pairs and triples the corpus does not yet execute."""
    key = (id(c), frozenset(seeded), id(existing), id(tables))
    if key not in _BUILT:
        _BUILT[key] = _build(c, seeded, existing, tables)
    return _BUILT[key]


def _build(c: model.Corpus, seeded: set[str], existing, tables=None) -> list[Spec]:
    import quarantine

    q = set(quarantine.ENGINE_QUARANTINE) | set(quarantine.HANGS)
    byclass = executed.class_features()
    bymapping = executed.mapping_features()
    closure = executed.include_closure()

    covered_p, covered_t = set(), set()
    for s in existing:
        if s.name in q:
            continue
        f = sorted(stacking.spec_features(c, s, byclass, bymapping, closure))
        covered_p.update(itertools.combinations(f, 2))
        covered_t.update(itertools.combinations(f, 3))

    if tables is None:
        import flat
        tables = flat.all_tables(c)

    candidates = []
    seen = set()
    for t in _templates(c, existing):
        for cand in _shape_variants(c, t, seeded, tables):
            if cand.name in seen:
                continue
            seen.add(cand.name)
            if _evaluable(c, cand, tables):
                candidates.append(cand)

    chosen: list[Spec] = []
    while len(chosen) < BUDGET and candidates:
        scored = []
        for cand in candidates:
            f = stacking.spec_features(c, cand, byclass, bymapping, closure)
            scored.append((_score(f, covered_p, covered_t), cand, f))
        scored.sort(key=lambda r: -r[0])
        gain, best, feats = scored[0]
        if gain < MIN_GAIN:
            break
        chosen.append(best)
        candidates.remove(best)
        fs = sorted(feats)
        covered_p.update(itertools.combinations(fs, 2))
        covered_t.update(itertools.combinations(fs, 3))
    return chosen
