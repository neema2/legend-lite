"""Heavily STACKED query shapes over the models the corpus already has.

The corpus is deep in mapping mechanics and shallow in query shape. Measured over 2301
services: a mean of 1.75 distinct query features each, 88% navigating a single hop, 5% with
a filter, 3% with an aggregate, 1% with a groupBy. Most of the fan-out is one bare projection
per subtype set, of which there are 1828 -- breadth of model, not depth of query.

That imbalance shows in the findings. F50-F54 came from query shape, back when that was the
only dimension the corpus had. F55, F56 and F57 all came from mapping structure, which is the
dimension that has been deepened since. We have been mining the seam we dug.

So this digs the other one. It does NOT touch the existing services: a bare projection over a
subtype is the only thing that makes a wrong `~filter` visible -- a wrong filter returns the
WHOLE table rather than erroring -- and a stacked query over the same rows is only
interpretable if a simple one over them passes. Same models, same seed, new services, and the
pair is a differential rather than a replacement. That split has already paid twice:
aggregates.py against tomany.py around F6, and CB_NotEqualsNull around `!=` over NULL.

Five variants per root, each a different way to be complex, so the output does not collapse
into one shape the way stacks.py does (it dedupes by tree signature and saturates at ~45):

  DEEP    the longest to-one chains the model offers, filtered ON A NAVIGATED column rather
          than a local one, composite-sorted, limited
  GROUP   the same reach, then groupBy on a NAVIGATED key with aggregates over it
  TREE    a graphFetch three levels deep
  TREE    a graphFetch three or four levels deep
  SUB     rooted at a SUBTYPE set that navigates its OWN ends. This currently emits NOTHING,
          and the zero is the finding rather than a bug in the generator: every subtype set
          in the corpus reaches other classes only through ends it INHERITS, and F49 makes
          those unnavigable from the subtype's set. The variant is kept so that the day a
          subtype declares an end of its own, it is covered.
  QUAL    derived and qualified properties -- the ones taking arguments -- evaluated at the
          END of a chain rather than on the root
  LOCAL   a filter, a derived property, a composite sort and a limit stacked on a root that
          cannot navigate at all
  AGG     groupBy on a local key with two aggregates over it

The last two exist because of a hard ceiling. Only 45 of 2009 seeded roots can navigate ANY
chain: 1838 of them are subtype sets, and F49 makes an end inherited from a supertype
unnavigable from the subtype's set -- fatal at initialisation, so it cannot even be
quarantined. Depth is therefore unavailable on nine tenths of the corpus's roots, and what
IS available there is stacking width: predicate, aggregation, ordering and computation over
the rows a `~filter` selects. That is still four or five features where there was one.

What is deliberately NOT stacked, because a stack containing a known defect is red for a
reason nobody has to look for and masks the interaction it exists to find:

  F6   aggregates over a to-many end where some parent has no children. Avoided by
       construction: GROUP aggregates over PROJECTED ROWS, not over a to-many navigation.
  F14  groupBy over a many-to-one enum mapping -- enum-typed columns are never group keys.
  F55  a graphFetch edge leaving a mapping for one that mapping includes.
  F49  an end inherited from a supertype, navigated from a subtype set.
"""
from __future__ import annotations

import model
import query as _query
import stacks
from query import Pred, Proj, Spec

# Deeper than stacks.py's 3: the point here is reach, and the model has chains that long.
DEPTH = 4

# EVERY seeded root, not a ranked slice. stacks.py caps at 120 because each of its services is
# expensive to interpret; these are mechanical, and the only cost is wall-clock -- about 0.61s
# per service, so covering all of them adds roughly 25 minutes to a run that takes 35.
#
# That is the trade being made deliberately: the corpus had 2009 roots each read by one bare
# projection, and a bare projection over a subtype proves its ~filter and nothing else.
MAX_ROOTS = 2100

# The DEEP variants are capped separately. 1399 roots can navigate once the taxonomies have
# an edge, but the taxonomies are homogeneous -- the same book.desk.businessUnit chain 1300
# times -- so the 1300th adds a service and no coverage. Roots are ranked by reach, so this
# takes the deepest and most varied, and the flat-root variants still cover everything.
DEEP_ROOTS = 400


# Single-construct mapping FIXTURES, each with its own store, runtime and data element.
# stacks.py excludes combo:: and hier:: for the same reason: they exist to exercise one
# mapping mechanism apiece, their classes are not domain models, and a generated stack over
# one asserts nothing the fixture does not already assert deliberately.
FIXTURES = ("surface::", "doy::", "sub::", "fdw::", "nb::", "rf::", "conf::")


# Declared property types acceptable for each coarse column kind.
_DECLARED = {"string": ("String",), "int": ("Integer", "Float")}


def _enum_typed(c: model.Corpus, cls: str, prop: str) -> bool:
    return (cls, prop) in c.enum_props


def _scalar_leaf(c: model.Corpus, cls: str, kind: str, skip: set[str] | None = None,
                 ok=None):
    """A locally-declared scalar property of `cls`, preferring a labelled one.

    Enum-typed properties are excluded whatever their column says: an enum over a CHAR(3)
    filters as `greaterThan(CtCurrency, String)` and fails to COMPILE, taking the whole file
    with it. stacks.py learned that from a linked project; it is the same rule here.
    """
    cols = c.columns.get(cls, {})
    tbl = c.tables.get(c.main_table.get(cls, ""))
    if tbl is None:
        return None
    fallback = None
    for prop, col in sorted(cols.items()):
        if skip and prop in skip:
            continue
        cc = tbl.columns.get(col)
        if cc is None or cc.kind != kind or _enum_typed(c, cls, prop):
            continue
        # A name can be BOTH a column mapping and an association end -- `jurisdiction` is a
        # column on tax::TaxJurisdiction and an end from it. Projecting it then ends on the
        # association rather than on a scalar, which the reader refuses.
        if (cls, prop) in c.ends:
            continue
        # By DECLARED type, not by the column's. A Boolean property can be mapped from a
        # VARCHAR, and filtering it as a string compares a bool with ' '. spread.py records
        # the same lesson; stacks.py had to learn it again for enums.
        pr = c.classes[cls].props.get(prop) if cls in c.classes else None
        if pr is None or pr.type not in _DECLARED[kind]:
            continue
        # To-ONE only. A `[*]` scalar projects and filters as a LIST, and comparing a list
        # with ' ' is not a predicate.
        if pr.upper is None or pr.upper > 1:
            continue
        # `ok` lets a caller keep SEARCHING rather than take the first match and give up.
        # The tree variant needs a leaf that is null-free, and rejecting the first candidate
        # outright cost 367 of 400 trees; asking for the first candidate that qualifies
        # costs none of them.
        if ok is not None and not ok(prop):
            continue
        if prop.lower() in stacks._LABELLISH:
            return prop
        fallback = fallback or prop
    return fallback


_PRIMITIVE = ("String", "Integer", "Float", "Boolean", "StrictDate", "Date", "DateTime",
              "Decimal", "Number")


def _navigable(c: model.Corpus, root: str, path: list[str]) -> bool:
    """Every hop of `path` must be a CLASS-typed property on the class it leaves.

    A class can declare a scalar and an association END under the same name -- `currency` is
    a String column on most taxonomy bases -- and Pure resolves the declared property, not
    the end. Navigating through it fails to COMPILE with "The property 'x' can't be accessed
    on primitive types", which takes the whole file down before any service runs.
    """
    cls, seen = root, {root}
    for hop in path:
        pr = c.classes[cls].props.get(hop) if cls in c.classes else None
        if pr is not None and pr.type in _PRIMITIVE:
            return False
        end = c.ends.get((cls, hop))
        if end is None:
            return False
        cls = end.target
        # No CYCLES. The model has plenty -- a book has a rollup and a rollup has a book --
        # and a depth-4 walk happily produces `book.rollup.book.rollup.bookId`, which is not
        # a query anyone would write and which fails to route at all.
        if cls in seen:
            return False
        seen.add(cls)
    return True


def _reach(c: model.Corpus, root: str, seeded: set[str]):
    """Navigation chains from `root`, deepest first, each with the class it lands on."""
    return stacks._chains(c, root, seeded, depth=DEPTH)


def _derived(c: model.Corpus, cls: str, with_args: bool):
    """(name, arity) of a derived or qualified property, or None."""
    if cls not in c.classes:
        return None
    for n, d in sorted(c.classes[cls].derived.items()):
        if bool(d.params) == with_args:
            return (n, d.params)
    return None


def _arg_for(p) -> object:
    """A literal for a qualified property's parameter, by its declared type."""
    t = getattr(p, "type", None) or (p[1] if isinstance(p, (tuple, list)) else "String")
    return {"Float": 2.5, "Integer": 2, "Boolean": True}.get(t, "'X'")


def _null_free(c: model.Corpus, cls: str, prop: str, tables) -> bool:
    """Is `prop` safe to put in a graphFetch TREE?

    A tree ENFORCES multiplicity -- "Property of multiplicity [1] can not be null" -- where
    a TDS projection of the same column returns the null happily. The corpus seeds a NULL
    into every nullable column on purpose, so a `[1]` property over one of those is a
    disagreement between the model and the data, and a tree over it tests that disagreement
    rather than graph fetch. graphs.py excludes them for the same reason; DSTree did not,
    and 13 of its 400 services errored on exactly this.
    """
    d = c.classes[cls].props.get(prop) if cls in c.classes else None
    col = c.columns.get(cls, {}).get(prop)
    tbl = c.main_table.get(cls, "")
    if d is None or d.lower < 1 or col is None or tables is None:
        return True
    return not any(r.get(col) is None for r in tables.get(tbl, []))


def build(c: model.Corpus, seeded: set[str], tables=None) -> list[Spec]:
    specs: list[Spec] = []
    reachable = model.mapping_closure(c, "stress::AllMapping") | {"stress::AllMapping"}
    roots = []
    for cls, table in sorted(c.main_table.items()):
        if table not in seeded or cls in c.views or stacks._is_fixture(cls):
            continue
        if cls.startswith(FIXTURES):
            continue
        # The service runs against stress::AllMapping, so a class mapped only in some other
        # mapping cannot be routed and the build refuses it by name.
        if c.declared_in.get(cls) not in reachable:
            continue
        idents = [a for a in stacks._identifiers(c, cls) if (cls, a) not in c.ends]
        if not idents:
            continue
        ident = idents[0]
        # Roots with NO navigable chain are kept: they are 1838 of the 2009, and the LOCAL
        # and AGG variants are exactly for them. Dropping them here is what limited the first
        # version of this generator to 45 services.
        chains = _reach(c, cls, seeded)
        depth = max((len(p) for p, _ in chains), default=0)
        roots.append((len(chains), depth, cls, idents, chains))
    deep_used = 0
    roots.sort(key=lambda r: (-r[1], -r[0], r[2]))
    roots = roots[:MAX_ROOTS]

    for _n, _d, root, idents, chains in roots:
        ident = idents[0]
        key_sort = [(a, False) for a in idents]
        as_of = None
        probe = Spec("x", "x", "x", root)
        try:
            _query.apply_temporal(c, probe)
            as_of = probe.as_of
        except Exception:
            as_of = None
        is_sub = root in c.classes and bool(c.classes[root].supertype)

        # The deepest chains, each landing on a scalar. Deepest first, so a stack takes the
        # longest reach the model actually has rather than whatever sorts first.
        legs = []
        for path, tgt in chains:
            if not _navigable(c, root, path):
                continue
            leaf = _scalar_leaf(c, tgt, "string") or _scalar_leaf(c, tgt, "int")
            if leaf is None:
                continue
            legs.append((path + [leaf], len(path)))
            if len(legs) >= 4:
                break

        local_str = _scalar_leaf(c, root, "string", skip=set(idents))
        local_num = _scalar_leaf(c, root, "int", skip=set(idents))

        def base(tag, doc):
            s = Spec(f"stress::DS{tag}_{_query.short_name(c, root)}",
                     f"/stress/ds{tag.lower()}_{_query.short_name(c, root).lower()}", doc, root)
            s.as_of = as_of
            return s

        # ---- LOCAL and AGG: for every root, including the ones that cannot navigate ----
        if local_str_0 := _scalar_leaf(c, root, "string", skip=set(idents)):
            lo = base("Local", f"A filter, a computed column, a composite sort and a limit "
                               f"stacked on {root} -- which cannot navigate anywhere, "
                               f"because its ends are inherited and F49 makes those "
                               f"unnavigable from a subtype's set. Depth being unavailable "
                               f"is not a reason to leave the query bare. Generated by "
                               f"scripts/corpus/deepstack.py.")
            lo.projections = ([Proj(a, [a]) for a in idents]
                              + [Proj(local_str_0, [local_str_0])])
            n0 = _scalar_leaf(c, root, "int", skip=set(idents) | {local_str_0})
            if n0:
                lo.projections.append(Proj(n0, [n0]))
            pl = _derived(c, root, with_args=False)
            if pl:
                lo.projections.append(Proj(pl[0], [pl[0]]))
            lo.filters = [Pred([local_str_0], ">", " ")]
            lo.sort = key_sort
            lo.limit = 25
            specs.append(lo)

            # groupBy on a local key. Never an enum-typed one: F14 is a defect in exactly
            # that pairing, and a stack containing it is red for a reason nobody has to look
            # for. The key is a column the ~filter did NOT select on, so the groups are a
            # partition of the subtype's rows rather than of the whole table.
            n1 = _scalar_leaf(c, root, "int", skip=set(idents) | {local_str_0})
            if n1 and not _enum_typed(c, root, local_str_0):
                ag = base("Agg", f"groupBy on a local key of {root} with two aggregates "
                                 f"over it. The rows being grouped are the ones its "
                                 f"~filter selected, so a filter that selected the whole "
                                 f"table shows up here as the wrong COUNT rather than as "
                                 f"extra rows. Generated by scripts/corpus/deepstack.py.")
                ag.filters = [Pred([local_str_0], ">", " ")]
                ag.projections = [Proj(local_str_0, [local_str_0]), Proj(ident, [ident]),
                                  Proj(n1, [n1])]
                ag.group_by = [local_str_0]
                ag.aggs = [("n", ident, "count"), ("total", n1, "sum")]
                ag.sort = [(local_str_0, False)]
                specs.append(ag)


        if not legs or deep_used >= DEEP_ROOTS:
            continue
        deep_used += 1
        deep_alias = stacks._alias(legs[0][0][:-1], legs[0][0][-1])

        # ---- DEEP: filter on a NAVIGATED column, not a local one ----
        # The filter is the point. 5% of the corpus filters at all, and what filters does so
        # on a column of the root -- so no service has ever made the ENGINE decide whether a
        # predicate over a joined column belongs in the WHERE or after the join.
        d = base("Deep", f"The deepest to-one reach {root} offers, filtered on a NAVIGATED "
                         f"column rather than a local one, composite-sorted and limited. A "
                         f"predicate over a joined column has to be placed relative to the "
                         f"join, which a filter on the root never tests. Generated by "
                         f"scripts/corpus/deepstack.py.")
        d.projections = ([Proj(a, [a]) for a in idents]
                         + [Proj(stacks._alias(p[:-1], p[-1]), p) for p, _ in legs])
        d.filters = [Pred(list(legs[0][0]), ">", " ")]
        if local_str:
            d.filters.append(Pred([local_str], ">", " "))
        d.sort = key_sort
        d.limit = 25
        specs.append(d)

        # ---- POST: the same reach, filtered AFTER project instead of before ----
        # DSDeep puts the predicate on the model path, so the engine can push it into the
        # WHERE alongside the join. This puts the identical predicate on the projected
        # ALIAS, after the join has already happened, and the two must return the same rows.
        # If they differ, one of the two placements is wrong -- and no service in this corpus
        # had ever asked the question, because filtering after a project was not expressible.
        pf = base("Post", f"The same rows as DSDeep{_query.short_name(c, root)}, filtered "
                          f"AFTER project on the projected alias rather than before it on "
                          f"the model path. The join has already happened by then, so the "
                          f"engine has to decide whether the predicate can be pushed back "
                          f"down; the two services must agree. Generated by "
                          f"scripts/corpus/deepstack.py.")
        pf.projections = ([Proj(a, [a]) for a in idents]
                          + [Proj(stacks._alias(p[:-1], p[-1]), p) for p, _ in legs])
        pf.post_filters = [Pred([deep_alias], ">", " ")]
        pf.sort = key_sort
        pf.limit = 25
        specs.append(pf)

        # ---- GROUP: aggregate over a NAVIGATED key ----
        # Grouped on a projected column that came from a JOIN, which is where a groupBy can
        # silently group the wrong rows. Aggregates run over projected ROWS rather than over a
        # to-many end, so F6's empty-set case cannot arise here by construction.
        if len(legs) >= 2 and not _enum_typed(c, root, ident):
            g = base("Group", f"The same reach as DSDeep{_query.short_name(c, root)}, then "
                              f"grouped on a NAVIGATED key with aggregates over it. The key "
                              f"came through a join, so a groupBy that grouped before the "
                              f"join would return the same columns and different numbers. "
                              f"Generated by scripts/corpus/deepstack.py.")
            # The group key comes through a join, so it is NULL for every row whose
            # navigation lands on nothing -- and a sort key containing NULL makes the rows
            # surviving a limit dialect-dependent, which the oracle refuses outright. So the
            # unmatched rows are excluded HERE, deliberately and visibly, rather than the
            # sort being quietly weakened. DSDeep over the same reach keeps them.
            g.filters = [Pred(list(legs[0][0]), ">", " ")]
            g.projections = [Proj(deep_alias, legs[0][0]), Proj(ident, [ident])]
            if local_num:
                g.projections.append(Proj(local_num, [local_num]))
            g.group_by = [deep_alias]
            g.aggs = [("n", ident, "count")]
            if local_num:
                g.aggs.append(("total", local_num, "sum"))
            g.sort = [(deep_alias, False)]
            specs.append(g)

        # ---- QUAL: a derived or qualified property at the END of a chain ----
        plain = _derived(c, root, with_args=False)
        qual = _derived(c, root, with_args=True)
        if plain or qual:
            q = base("Qual", f"Derived and qualified properties on {root}, alongside the "
                             f"deepest chain the model offers -- a computed value and a "
                             f"joined one in the same projection, so a defect in either is "
                             f"visible against the other. Generated by "
                             f"scripts/corpus/deepstack.py.")
            q.projections = ([Proj(a, [a]) for a in idents]
                             + [Proj(stacks._alias(legs[0][0][:-1], legs[0][0][-1]),
                                     legs[0][0])])
            if plain:
                q.projections.append(Proj(plain[0], [plain[0]]))
            if qual:
                q.projections.append(
                    Proj(f"{qual[0]}Q", [qual[0]], None, [_arg_for(p) for p in qual[1]]))
            q.sort = key_sort
            q.limit = 25
            specs.append(q)

        # ---- TREE: a graphFetch three levels deep ----
        # 56 of 2301 services fetch a tree at all, and those are two levels. A tree is not a
        # projection with different syntax: a sub-object that is wholly ABSENT and one whose
        # every field is null are the same row in a projection and different objects here.
        #
        # F55 is respected per HOP, not per root: an edge that leaves a mapping for one that
        # mapping includes dies at initialisation with "RelationalPropertyMapping cannot be
        # cast to XStorePropertyMapping", which takes the whole batch with it.
        path0, tgt0 = legs and (legs[0][0][:-1], None) or (None, None)
        path0 = legs[0][0][:-1] if legs else []
        tgt0 = None
        cur_cls = root
        for hop in path0:
            cur_cls = c.ends[(cur_cls, hop)].target
        tgt0 = cur_cls
        hop_cls, ok = root, True
        for hop in path0:
            end = c.ends.get((hop_cls, hop))
            if end is None:
                ok = False
                break
            if c.declared_in.get(end.target) in model.mapping_closure(
                    c, c.declared_in.get(hop_cls, "")):
                ok = False
                break
            hop_cls = end.target
        if ok and len(path0) >= 2:
            free = lambda pr: _null_free(c, tgt0, pr, tables)
            leafname = (_scalar_leaf(c, tgt0, "string", ok=free)
                        or _scalar_leaf(c, tgt0, "int", ok=free))
            leafid = stacks._identifier(c, tgt0)
            if leafid and not _null_free(c, tgt0, leafid, tables):
                leafid = None
            if leafname and all(_null_free(c, root, a, tables) for a in idents):
                tree, node = {ident: None}, None
                cur = tree
                for hop in path0:
                    cur[hop] = {}
                    cur = cur[hop]
                cur[leafname] = None
                if leafid and leafid != leafname:
                    cur[leafid] = None
                tr = base("Tree", f"A graphFetch {len(path0)} levels deep from {root}. A "
                                  f"sub-object that is wholly ABSENT and one whose every "
                                  f"field is null are the same row in a projection and "
                                  f"different objects in a tree, so this asserts something "
                                  f"the DSDeep service over the same reach cannot. "
                                  f"Generated by scripts/corpus/deepstack.py.")
                tr.graph = tree
                specs.append(tr)

        # ---- SUB: a subtype root that navigates ----
        # 1828 class mappings use `extends [id]` and almost none of them is ever navigated
        # FROM. _chains already drops ends inherited from a supertype (F49), so what is left
        # is the subtype's own reach.
        if is_sub:
            s = base("Sub", f"Rooted at the SUBTYPE {root} and navigating its OWN ends. "
                            f"1828 class mappings in this corpus extend another set, and "
                            f"almost nothing has ever navigated out of one -- the services "
                            f"over them project local columns and stop. Generated by "
                            f"scripts/corpus/deepstack.py.")
            s.projections = ([Proj(a, [a]) for a in idents]
                             + [Proj(stacks._alias(p[:-1], p[-1]), p) for p, _ in legs[:3]])
            s.filters = [Pred(list(legs[0][0]), ">", " ")]
            s.sort = key_sort
            s.limit = 25
            specs.append(s)

    return specs
