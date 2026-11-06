"""
M2M: the services of 99-m2m-services.pure and 94-gap-services.pure (docs/STRESS_CORPUS_PLAN_2026_10_19.md, Phase 5).

The corpus had five M2M class mappings, and lite ran none of them. These services reach the m2m:: pack
(8114-m2m.pure, 8115-m2m-gaps.pure): target classes mapped in Pure from source classes that are themselves mapped
relationally over the seeded TRADE, COUNTERPARTY, BOOK and ALLOCATION tables, chained through one ModelChainConnection,
so every test reads the corpus's relational seed.

Expected results come by INVARIANCE, as battery.py's _m2m_pair does it: each M2M service (MX_) mirrors a relational
twin (MXR_) over the source classes that computes the same values in SQL -- a derived or qualified property of the
source, a function applied in the query, the source's own relational enumeration mapping -- and each group is in
battery.INVARIANCE_GROUPS, so build.py fails if the two ever expect different rows. The oracle evaluates the twin
alone; it needs no M2M evaluator.

  transforms               m2m::TradeView: arithmetic, concatenation, a comparison, functions of a value and through
                           map of an optional one, a conditional; projected, fetched, filtered, ordered and grouped
  derived source property  m2m::TradeRef reads the source's derived and qualified properties; fetched it answers
                           nothing (F84)
  enumeration transforms   a String code, an optional one, an INTEGER one, each against the source's relational
                           enumeration mapping of the same column
  nested sets              m2m::TradeCard's class-typed properties, each with a set of its own: over an association,
                           and from the source object itself
  passthrough              m2m::TradeLink's properties typed with the SOURCE classes, passed through as they are
  target instantiation     m2m::TradeStamp: `^` builds an object a transform reads back from, directly and through a
                           function; over a concatenation it answers the first operand (F85)
  ~filter, union, merge    a ~filter on a property and through a navigation; a union of two Pure sets; a merge,
                           which the engine does not run over a ModelChainConnection (F86)
  to-many                  m2m::DealActivity reads whether a to-many end is empty
  local property           75-surface-gaps.pure's `+localTag`, through surface::GapPureSurface on a chain of its own

lite refuses three of these when it builds the model, the enumeration transforms, the passthrough (8115-m2m-gaps.pure)
and the local property (75-surface-gaps.pure), so it excludes their files, and their M2M services are written to
94-gap-services.pure, which it excludes with them (the user's choice, 2026-10-30). Their twins, over the source mapping,
are in 99-m2m-services.pure with the rest, and run in both engines.

Each shape was probed on the pinned release first. Over a ModelChainConnection the engine inlines a TDS projection's
transforms into the source's SQL and runs a graph fetch in memory, and the two fail apart (8114-m2m.pure's header),
so each feature is written in the form that runs, and a defect is pinned by a quarantined service.
"""
from __future__ import annotations

from query import Apply, DateArg, EnumArg, If, Pred, Proj, Spec

VIEWS = "m2m::Views", "m2m::ViewsRT"
# The enumeration transforms and the passthrough, in 8115-m2m-gaps.pure, which lite excludes
GAP_VIEWS = "m2m::GapViews", "m2m::GapViewsRT"
MERGED = "m2m::Merged", "m2m::MergedRT"
SOURCE = "m2m::SourceMapping", "m2m::SourceRT"
CONNECTION = "environment"

# The local property's services: 75-surface-gaps.pure's mappings, on GapDB's own connection and data
GAP_PURE = "surface::GapPureSurface", "surface::GapPureRT"
GAP_SOURCE = "surface::GapSourceMapping", "surface::GapSourceRT"
GAP_DB, GAP_DATA, GAP_CONNECTION = "surface::GapDB", "surface::GapData", "gapEnv"
GAP_MAPPINGS = {GAP_PURE[0], GAP_SOURCE[0], GAP_VIEWS[0]}


def _upper(through_map=False):
    return Apply((("toUpper", ()),), "String", through_map=through_map)


def _lower():
    return Apply((("toLower", ()),), "String")


def _bind(s: Spec, binding, connection=CONNECTION, data=None) -> Spec:
    s.mapping, s.runtime = binding
    s.connection, s.data_element = connection, data
    return s


def _twin(name: str, doc: str, root: str, cols=None, graph=None, filters=(), binding=SOURCE, connection=CONNECTION,
          data=None) -> Spec:
    """A relational twin: the source read straight from SQL, the expectation its M2M services share."""
    s = Spec(f"stress::MXR_{name}", f"/stress/m2m/twin/{name.lower()}", doc, root)
    if graph is not None:
        s.graph = graph
    else:
        s.projections = list(cols)
    s.filters = list(filters)
    return _bind(s, binding, connection, data)


def _m2m(name: str, doc: str, root: str, twin: Spec, cols=None, graph=None, filters=(), binding=VIEWS,
         connection=CONNECTION, data=None) -> Spec:
    """An M2M service: a TDS projection (the form a ModelChainConnection takes, F11) or a graph fetch, mirroring its
    twin's rows."""
    s = Spec(f"stress::MX_{name}", f"/stress/m2m/{name.lower()}", doc, root)
    if graph is not None:
        s.graph = graph
    else:
        s.projections = list(cols)
        s.paradigm = "tds"
    s.filters = list(filters)
    s.mirrors = twin
    return _bind(s, binding, connection, data)


def _same(s: Spec, twin: Spec) -> Spec:
    """Sort, limit and grouping as the twin's: the rows the two compare are the same rows."""
    s.sort, s.limit, s.group_by, s.aggs = twin.sort, twin.limit, list(twin.group_by), list(twin.aggs)
    return s


def _cols(*names) -> list[Proj]:
    return [Proj(n, n.split(".")) for n in names]


# ---------------------------------------------------------------- transforms

_VIEW_TWIN = [Proj("dealId", ["dealId"]), Proj("day", ["tradeDate"]), Proj("gross", ["gross"]),
              Proj("ref", ["ref"]), Proj("large", ["large"]), Proj("scaled", ["scaled"], args=[2.0]),
              Proj("statusLower", ["status"], apply=_lower()),
              Proj("venueUpper", ["venue"], apply=_upper(through_map=True)),
              Proj("feesDoubled", ["fees"], apply=Apply((("*", (2.0,)),), "Float", through_map=True)),
              Proj("band", ["notional"], apply=If(">", 1000000.0, "large", "small", "String"))]
_VIEW = ["dealId", "day", "gross", "ref", "large", "scaled", "statusLower", "venueUpper", "feesDoubled", "band"]


def _transforms() -> list[list[Spec]]:
    deal, view = "m2m::src::Deal", "m2m::TradeView"
    twin = _twin("TradeView", "The source's columns, derived and qualified properties, and functions applied in the "
                              "query: what m2m::TradeView's inline transforms must compute.", deal, _VIEW_TWIN)
    groups = [[twin,
               _m2m("TradeView", "m2m::TradeView's transforms projected: arithmetic, concatenation, a comparison, "
                                 "a function of a value and, through map, of an optional one, and a conditional.",
                    view, twin, _cols(*_VIEW)),
               _m2m("TradeViewTree", "The same transforms fetched as a graph, which the engine evaluates in memory "
                                     "over the fetched source rather than in SQL.", view, twin,
                    graph={n: None for n in _VIEW})]]

    t = _twin("TradeViewFiltered", "Deals whose gross exceeds 300,000.", deal,
              [Proj("dealId", ["dealId"]), Proj("gross", ["gross"]),
               Proj("band", ["notional"], apply=If(">", 1000000.0, "large", "small", "String"))],
              filters=[Pred(["gross"], ">", 300000.0)])
    groups.append([t, _m2m("TradeViewFiltered", "A query filter over a transformed property of the M2M target.",
                           view, t, _cols("dealId", "gross", "band"), filters=[Pred(["gross"], ">", 300000.0)])])

    t = _twin("TradeViewTop", "The five largest gross amounts.", deal,
              [Proj("dealId", ["dealId"]), Proj("gross", ["gross"])])
    t.sort, t.limit = [("gross", True), ("dealId", False)], 5
    groups.append([t, _same(_m2m("TradeViewTop", "The M2M target ordered on a transformed property and cut.",
                                 view, t, _cols("dealId", "gross")), t)])

    t = _twin("TradeViewBands", "Deals counted and their gross summed per band.", deal,
              [Proj("dealId", ["dealId"]), Proj("gross", ["gross"]),
               Proj("band", ["notional"], apply=If(">", 1000000.0, "large", "small", "String"))])
    t.group_by, t.aggs = ["band"], [("deals", "dealId", "count"), ("total", "gross", "sum")]
    groups.append([t, _same(_m2m("TradeViewBands", "The M2M target grouped on a conditional transform.",
                                 view, t, _cols("dealId", "gross", "band")), t)])

    t = _twin("TradeViewLarge", "Deals whose notional exceeds a million, by the source's derived Boolean.", deal,
              [Proj("dealId", ["dealId"]), Proj("scaled", ["scaled"], args=[2.0])],
              filters=[Pred(["large"], "is")])
    groups.append([t, _m2m("TradeViewLarge", "A query filter on a Boolean transform, `$x.large`.", view, t,
                           _cols("dealId", "scaled"), filters=[Pred(["large"], "is")])])

    day = DateArg("2024-06-05")
    t = _twin("TradeViewDay", "The deals of 5 June.", deal,
              [Proj("dealId", ["dealId"]), Proj("day", ["tradeDate"]), Proj("gross", ["gross"])],
              filters=[Pred(["tradeDate"], "==", day)])
    groups.append([t, _m2m("TradeViewDay", "A query filter on a StrictDate the transform carries over.", view, t,
                           _cols("dealId", "day", "gross"), filters=[Pred(["day"], "==", day)])])
    upper = _upper(through_map=True)
    t = _twin("TradeViewVenue", "The deals whose venue, upper-cased, is XNAS: TRD-0014's is written `xnas`.", deal,
              [Proj("dealId", ["dealId"]), Proj("venueUpper", ["venue"], apply=upper)],
              filters=[Pred(["venue"], "==", "XNAS", apply=upper)])
    groups.append([t, _m2m("TradeViewVenue", "A query filter on a transform of an optional value through map: it "
                                             "must see the transformed value, not the stored one.", view, t,
                           _cols("dealId", "venueUpper"), filters=[Pred(["venueUpper"], "==", "XNAS")])])
    band = If(">", 1000000.0, "large", "small", "String")
    t = _twin("TradeViewLargeBand", "The deals in the large band.", deal,
              [Proj("dealId", ["dealId"]), Proj("band", ["notional"], apply=band)],
              filters=[Pred(["notional"], "==", "large", apply=band)])
    groups.append([t, _m2m("TradeViewLargeBand", "A query filter on a conditional transform's result.", view, t,
                           _cols("dealId", "band"), filters=[Pred(["band"], "==", "large")])])
    t = _twin("TradeViewNoFees", "The deals with no fees, whose doubled fees are therefore absent too.", deal,
              [Proj("dealId", ["dealId"]), Proj("statusLower", ["status"], apply=_lower())],
              filters=[Pred(["fees"], "isEmpty")])
    groups.append([t, _m2m("TradeViewNoFees", "A query filter on the absence of a transform through map: absent where "
                                              "its source value is.", view, t, _cols("dealId", "statusLower"),
                           filters=[Pred(["feesDoubled"], "isEmpty")])])
    lower = _lower()
    t = _twin("TradeViewExecuted", "The deals whose status, lower-cased, is `executed`: TRD-0009's is `EXECUTE`.", deal,
              [Proj("dealId", ["dealId"]), Proj("statusLower", ["status"], apply=lower)],
              filters=[Pred(["status"], "==", "executed", apply=lower)])
    groups.append([t, _m2m("TradeViewExecuted", "A query filter on a function's transform of a value.", view, t,
                           _cols("dealId", "statusLower"), filters=[Pred(["statusLower"], "==", "executed")])])
    return groups


# ---------------------------------------------------------------- derived source properties

def _derived() -> list[list[Spec]]:
    deal, ref = "m2m::src::Deal", "m2m::TradeRef"
    cols = [Proj("dealId", ["dealId"]), Proj("gross", ["gross"]), Proj("ref", ["ref"]), Proj("large", ["large"]),
            Proj("scaled", ["scaled"], args=[2.0])]
    t = _twin("TradeRef", "The source's derived and qualified properties, evaluated in SQL.", deal, cols)
    groups = [[t, _m2m("TradeRef", "m2m::TradeRef's transforms read the source's derived properties and a qualified "
                                   "one with an argument, `$src.scaled(2.0)`.", ref, t,
                       _cols("dealId", "gross", "ref", "large", "scaled"))]]
    t = _twin("TradeRefKey", "Each deal's derived reference.", deal, [Proj("dealId", ["dealId"]),
                                                                       Proj("ref", ["ref"])])
    groups.append([t, _m2m("TradeRefTree", "A derived source property fetched as a graph: the engine answers it "
                                           "absent for every row (F84).", ref, t, graph={"dealId": None, "ref": None})])
    t = _twin("TradeRefLarge", "The derived references of the deals the derived Boolean calls large.", deal,
              [Proj("dealId", ["dealId"]), Proj("ref", ["ref"])], filters=[Pred(["large"], "is")])
    groups.append([t, _m2m("TradeRefLarge", "A query filter on a value the transform reads from a derived source "
                                            "property.", ref, t, _cols("dealId", "ref"),
                           filters=[Pred(["large"], "is")])])
    t = _twin("TradeRefTop", "The three largest gross amounts and their derived references.", deal,
              [Proj("dealId", ["dealId"]), Proj("gross", ["gross"]), Proj("ref", ["ref"])])
    t.sort, t.limit = [("gross", True), ("dealId", False)], 3
    groups.append([t, _same(_m2m("TradeRefTop", "The target ordered on a derived source property and cut.", ref, t,
                                 _cols("dealId", "gross", "ref")), t)])
    t = _twin("TradeRefGroups", "Deals counted and their gross summed by the derived Boolean.", deal,
              [Proj("dealId", ["dealId"]), Proj("gross", ["gross"]), Proj("large", ["large"])])
    t.group_by, t.aggs = ["large"], [("deals", "dealId", "count"), ("total", "gross", "sum")]
    groups.append([t, _same(_m2m("TradeRefGroups", "The target grouped on a value read from a derived source "
                                                   "property.", ref, t, _cols("dealId", "gross", "large")), t)])
    return groups


# ---------------------------------------------------------------- enumeration transforms

def _enums() -> list[list[Spec]]:
    deal, party, desk = "m2m::src::Deal", "m2m::src::Party", "m2m::src::BookRecord"
    no_zz = [Pred(["sideCode"], "!=", "ZZ")]
    t = _twin("DealSide", "TRADE.SIDE through trading::Codes' SideMapping, apart from TRD-0012's unmapped ZZ.", deal,
              _cols("dealId", "side"), filters=no_zz)
    groups = [[t,
               _m2m("DealSide", "An M2M enumeration transform of a String code, two of whose codes (B, BOT) "
                                "collapse onto one value.", "m2m::DealSide", t, _cols("dealId", "side"),
                    binding=GAP_VIEWS),
               _m2m("DealSideTree", "The same transform fetched as a graph.", "m2m::DealSide", t,
                    graph={"dealId": None, "side": None}, binding=GAP_VIEWS)]]
    buy = EnumArg("trading::Side", "BUY")
    t = _twin("DealSideBuys", "The buys, by the relational enumeration mapping.", deal, _cols("dealId"),
              filters=no_zz + [Pred(["side"], "==", buy)])
    groups.append([t, _m2m("DealSideBuys", "A query filter on an enumeration literal over the transformed value.",
                           "m2m::DealSide", t, _cols("dealId"), filters=[Pred(["side"], "==", buy)],
                           binding=GAP_VIEWS)])

    t = _twin("PartyTier", "COUNTERPARTY.TIER through PartyTierCodes.", party, _cols("partyId", "tier"))
    groups.append([t, _m2m("PartyTier", "An M2M enumeration transform of a String code onto three values.",
                           "m2m::PartyTier", t, _cols("partyId", "tier"), binding=GAP_VIEWS)])
    prime = EnumArg("m2m::Tier", "PRIME")
    t = _twin("PartyTierPrime", "The prime tier, by the relational enumeration mapping.", party, _cols("partyId"),
              filters=[Pred(["tier"], "==", prime)])
    groups.append([t, _m2m("PartyTierPrime", "A query filter on an enumeration literal over the transformed value.",
                           "m2m::PartyTier", t, _cols("partyId"), filters=[Pred(["tier"], "==", prime)],
                           binding=GAP_VIEWS)])
    t = _twin("PartyGrade", "COUNTERPARTY.TIER and CREDIT_RATING through their enumeration mappings; CP-0004 has no "
                            "rating, so no grade.", party, _cols("partyId", "tier", "grade"))
    groups.append([t,
                   _m2m("PartyGradeTree", "Two enumeration transforms fetched as a graph, one over an absent code.",
                        "m2m::PartyTier", t, graph={"partyId": None, "tier": None, "grade": None},
                        binding=GAP_VIEWS),
                   _m2m("PartyGrade", "The same projected: the absent code's value comes back as the empty string, "
                                      "not absent (F87).", "m2m::PartyTier", t, _cols("partyId", "tier", "grade"),
                        binding=GAP_VIEWS)])

    t = _twin("DeskLoad", "BOOK.POSITION_COUNT, an INTEGER, through DeskLoadCodes.", desk, _cols("bookId", "load"))
    groups.append([t,
                   _m2m("DeskLoad", "An M2M enumeration transform of an INTEGER code.", "m2m::DeskLoad", t,
                        _cols("bookId", "load"), binding=GAP_VIEWS),
                   _m2m("DeskLoadTree", "The same transform fetched as a graph.", "m2m::DeskLoad", t,
                        graph={"bookId": None, "load": None}, binding=GAP_VIEWS)])
    idle = EnumArg("m2m::Load", "IDLE")
    t = _twin("DeskLoadIdle", "The idle desks, by the relational enumeration mapping.", desk, _cols("bookId"),
              filters=[Pred(["load"], "==", idle)])
    groups.append([t, _m2m("DeskLoadIdle", "A query filter on an INTEGER-coded enumeration's transformed value.",
                           "m2m::DeskLoad", t, _cols("bookId"), filters=[Pred(["load"], "==", idle)],
                           binding=GAP_VIEWS)])
    return groups


# ---------------------------------------------------------------- nested sets and passthrough

def _nested() -> list[list[Spec]]:
    deal, card, link = "m2m::src::Deal", "m2m::TradeCard", "m2m::TradeLink"
    twin_cols = [Proj("dealId", ["dealId"]), Proj("partyName", ["party", "legalName"]),
                 Proj("deskName", ["desk", "name"]), Proj("qty", ["qty"]), Proj("px", ["px"])]
    t = _twin("TradeCard", "Each deal with its counterparty's and book's names and its economics; TRD-0007's "
                           "counterparty, CP-NONE, is an orphan.", deal, twin_cols)
    card_cols = [Proj("dealId", ["dealId"]), Proj("partyName", ["party", "legalName"]),
                 Proj("deskName", ["desk", "name"]), Proj("qty", ["economics", "qty"]),
                 Proj("px", ["economics", "px"])]
    groups = [[t, _m2m("TradeCard", "Class-typed properties each mapped by a set of its own, projected: two over "
                                    "associations of the source, one from the source object itself (`$src`).",
                       card, t, card_cols)]]
    t = _twin("TradeCardGb", "Deals with a GB counterparty.", deal,
              [Proj("dealId", ["dealId"]), Proj("partyName", ["party", "legalName"])],
              filters=[Pred(["party", "domicile"], "==", "GB")])
    groups.append([t, _m2m("TradeCardGb", "A query filter through a nested set.", card, t,
                           [Proj("dealId", ["dealId"]), Proj("partyName", ["party", "legalName"])],
                           filters=[Pred(["party", "domicile"], "==", "GB")])])
    t = _twin("TradeCardTop", "The first five deals by book name, then id.", deal,
              [Proj("dealId", ["dealId"]), Proj("deskName", ["desk", "name"])])
    t.sort, t.limit = [("deskName", False), ("dealId", False)], 5
    groups.append([t, _same(_m2m("TradeCardTop", "The M2M target ordered on a nested set's property and cut.", card,
                                 t, [Proj("dealId", ["dealId"]), Proj("deskName", ["desk", "name"])]), t)])
    t = _twin("TradeCardEconomics", "Deals of more than 10,000 units, with their book.", deal,
              [Proj("dealId", ["dealId"]), Proj("qty", ["qty"]), Proj("deskName", ["desk", "name"])],
              filters=[Pred(["qty"], ">", 10000.0)])
    groups.append([t, _m2m("TradeCardEconomics", "A query filter through the nested set mapped from the source object "
                                                 "itself, `economics[economics]: $src`.", card, t,
                           [Proj("dealId", ["dealId"]), Proj("qty", ["economics", "qty"]),
                            Proj("deskName", ["desk", "name"])],
                           filters=[Pred(["economics", "qty"], ">", 10000.0)])])
    tree = {"dealId": None, "party": {"partyId": None, "legalName": None, "domicile": None},
            "desk": {"bookId": None, "name": None, "currency": None}}
    t = _twin("TradeCardTree", "Each deal with its counterparty and book, fetched as a graph.", deal, graph=tree)
    groups.append([t, _m2m("TradeCardTree", "The nested sets fetched as a graph: each class-typed property an object "
                                            "of its own set, null where the association lands on nothing.", card, t,
                           graph=tree)])

    tree = {"dealId": None, "party": {"partyId": None, "legalName": None, "rating": None},
            "desk": {"bookId": None, "name": None, "strategy": None}}
    t = _twin("TradeLinkTree", "Each deal with its counterparty and book as the source maps them.", deal, graph=tree)
    groups.append([t, _m2m("TradeLinkTree", "Properties typed with the source classes and mapped with no set: the "
                                            "source objects pass through as they are.", link, t, graph=tree,
                           binding=GAP_VIEWS)])
    tree = {"dealId": None, "desk": {"bookId": None, "isActive": None, "positions": None}}
    t = _twin("TradeLinkDesk", "Each deal's book, its Boolean and Integer properties.", deal, graph=tree)
    groups.append([t, _m2m("TradeLinkDesk", "A passed-through object's Boolean and Integer properties.", link, t,
                           graph=tree, binding=GAP_VIEWS)])
    tree = {"dealId": None, "party": {"partyId": None, "tier": None}}
    t = _twin("TradeLinkTier", "Each deal's counterparty's tier, through the source's enumeration mapping.", deal,
              graph=tree)
    groups.append([t, _m2m("TradeLinkTier", "A passed-through object's enumeration-mapped property: the source's "
                                            "relational enumeration mapping, reached through the chain.", link, t,
                           graph=tree, binding=GAP_VIEWS)])
    return groups


# ---------------------------------------------------------------- target instantiation

def _instantiation() -> list[list[Spec]]:
    deal, stamp = "m2m::src::Deal", "m2m::TradeStamp"
    t = _twin("TradeStamp", "Each deal's id and currency lower-cased, and its venue.", deal,
              [Proj("dealId", ["dealId"]), Proj("stamp", ["dealId"], apply=_lower()),
               Proj("ccy", ["currency"], apply=_lower()), Proj("venue", ["venue"])])
    groups = [[t, _m2m("TradeStampTree", "`^m2m::Stamp(...)` instantiated in a transform and read back: directly, "
                                         "through a function returning it, and its optional property.", stamp, t,
                       graph={"dealId": None, "stamp": None, "ccy": None, "venue": None})]]
    t = _twin("TradeStampJoined", "Each deal's derived reference, `dealId:currency`.", deal,
              [Proj("dealId", ["dealId"]), Proj("joined", ["ref"])])
    groups.append([t, _m2m("TradeStampJoined", "`^m2m::Stamp(text = a + b).text`: the engine answers `a` (F85).",
                           stamp, t, graph={"dealId": None, "joined": None})])
    return groups


# ---------------------------------------------------------------- ~filter, union, merge, to-many

def _operations() -> list[list[Spec]]:
    deal, party = "m2m::src::Deal", "m2m::src::Party"
    executed = [Pred(["status"], "==", "EXECUTED")]
    t = _twin("OpenTrade", "The executed deals.", deal, _cols("dealId", "notional", "status"), filters=executed)
    groups = [[t,
               _m2m("OpenTrade", "An M2M ~filter on a source property, projected.", "m2m::OpenTrade", t,
                    _cols("dealId", "notional", "status")),
               _m2m("OpenTradeTree", "The same ~filter fetched as a graph.", "m2m::OpenTrade", t,
                    graph={"dealId": None, "notional": None, "status": None})]]
    t = _twin("OpenTradeTop", "The three largest executed notionals.", deal, _cols("dealId", "notional"),
              filters=executed)
    t.sort, t.limit = [("notional", True), ("dealId", False)], 3
    groups.append([t, _same(_m2m("OpenTradeTop", "The ~filtered target ordered and cut.", "m2m::OpenTrade", t,
                                 _cols("dealId", "notional")), t)])
    usd = [Pred(["desk", "currency"], "==", "USD")]
    t = _twin("UsdDeskTrade", "Deals booked in a USD book.", deal,
              [Proj("dealId", ["dealId"]), Proj("deskName", ["desk", "name"])], filters=usd)
    groups.append([t,
                   _m2m("UsdDeskTrade", "An M2M ~filter through a navigation, `$src.desk.currency == 'USD'`.",
                        "m2m::UsdDeskTrade", t, _cols("dealId", "deskName")),
                   _m2m("UsdDeskTradeTree", "The same fetched as a graph.", "m2m::UsdDeskTrade", t,
                        graph={"dealId": None, "deskName": None})])
    t = _twin("UsdDeskTradeTop", "The first four deals in a USD book, by book name, then id.", deal,
              [Proj("dealId", ["dealId"]), Proj("deskName", ["desk", "name"])], filters=usd)
    t.sort, t.limit = [("deskName", False), ("dealId", False)], 4
    groups.append([t, _same(_m2m("UsdDeskTradeTop", "The target ~filtered through a navigation, ordered and cut.",
                                 "m2m::UsdDeskTrade", t, _cols("dealId", "deskName")), t)])
    t = _twin("UsdDeskCount", "Deals counted per USD book.", deal,
              [Proj("dealId", ["dealId"]), Proj("deskName", ["desk", "name"])], filters=usd)
    t.group_by, t.aggs = ["deskName"], [("deals", "dealId", "count")]
    groups.append([t, _same(_m2m("UsdDeskCount", "The ~filtered target grouped.", "m2m::UsdDeskTrade", t,
                                 _cols("dealId", "deskName")), t)])

    t = _twin("Ticket", "Each deal, its origin by its trade type, and its notional.", deal,
              [Proj("dealId", ["dealId"]),
               Proj("origin", ["tradeType"], apply=If("==", "AGENCY", "agency", "principal", "String")),
               Proj("amount", ["notional"])])
    groups.append([t, _m2m("TicketTree", "A union of two Pure sets, each ~filtered on the trade type and carrying "
                                         "its own constant, fetched as a graph.", "m2m::Ticket", t,
                           graph={"dealId": None, "origin": None, "amount": None})])

    t = _twin("PartyProfile", "Each counterparty's name and rating.", party, _cols("partyId", "legalName", "rating"))
    groups.append([t, _m2m("PartyProfileTree", "A merge of two Pure sets over two source classes, joined by the "
                                               "validation lambda on their key: not over a ModelChainConnection "
                                               "(F86).", "m2m::PartyProfile", t,
                           graph={"partyId": None, "legalName": None, "rating": None}, binding=MERGED)])

    t = _twin("DealActivity", "Whether each deal has an allocation.", deal,
              [Proj("dealId", ["dealId"]), Proj("hasSlices", ["slices"], agg="isNotEmpty")])
    groups.append([t,
                   _m2m("DealActivity", "A transform over a to-many end, `$src.slices->isNotEmpty()`.",
                        "m2m::DealActivity", t, _cols("dealId", "hasSlices")),
                   _m2m("DealActivityTree", "The same fetched as a graph.", "m2m::DealActivity", t,
                        graph={"dealId": None, "hasSlices": None})])
    return groups


# ---------------------------------------------------------------- the local property (75-surface-gaps.pure)

def _local() -> list[list[Spec]]:
    src, tgt = "surface::GapSrc", "surface::GapTgt"
    t = _twin("LocalProperty", "GAP's rows straight from SQL, the local property's value computed in the query.", src,
              [Proj("k", ["k"]), Proj("v", ["v"]), Proj("localTag", ["v"], apply=Apply((("toLower", ()),), "String", through_map=True))],
              binding=GAP_SOURCE, connection=GAP_CONNECTION, data=GAP_DATA)
    return [[t,
             _m2m("LocalProperty", "A local mapping property, `+localTag`, projected through the M2M mapping that "
                                   "declares it.", tgt, t, _cols("k", "v", "localTag"), binding=GAP_PURE,
                  connection=GAP_CONNECTION, data=GAP_DATA),
             _m2m("LocalPropertyTree", "The same fetched as a graph.", tgt, t,
                  graph={"k": None, "v": None, "localTag": None}, binding=GAP_PURE, connection=GAP_CONNECTION,
                  data=GAP_DATA)]]


INVARIANCE_GROUPS = _transforms() + _derived() + _enums() + _nested() + _instantiation() + _operations() + _local()
SPECS = [s for g in INVARIANCE_GROUPS for s in g]
NAMES = {s.name for s in SPECS}


def is_gap(spec: Spec) -> bool:
    """The services over a mapping in a file lite excludes (75-surface-gaps.pure, 8115-m2m-gaps.pure), which go with
    it."""
    return spec.mapping in GAP_MAPPINGS


def data_element(c, tables) -> str:
    """GapDB's ###Data, which the local property's services reference."""
    import emit

    return emit.store_data_element(c, tables, GAP_DB, GAP_DATA)


def check(c, tables) -> list[str]:
    """Every twin has rows, so no pair agrees on nothing, and every M2M service mirrors a twin."""
    import oracle

    problems = [f"{s.short}: no rows" for s in SPECS
                if s.short.startswith("MXR_") and not oracle.evaluate(c, s, tables)]
    problems += [f"{s.short}: an M2M service with no twin" for s in SPECS
                 if s.short.startswith("MX_") and getattr(s, "mirrors", None) is None]
    return problems
