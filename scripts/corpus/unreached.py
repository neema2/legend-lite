"""
Declared but unreached: the services of 99-unreached-services.pure (docs/STRESS_CORPUS_PLAN_2026_10_19.md, Phase 7).

The corpus declared mapping and store constructs that no service ran, each scoring as present in the feature report
while nothing could detect it behaving wrongly (report §3's ○ rows, §8's unused joins). These services reach them:

  DMF_/DMD_  each class stress::DenseMapping maps (59-dense-mapping.pure), projected column for column through the
             flat mapping (stress::AllMapping, DMF_) and through DenseMapping (DMD_, on stress::DenseRT). DenseMapping
             respells the flat mapping with scope blocks in both forms, set ids, root markers and an explicit
             ~primaryKey, and means the same thing, so each pair is an invariance group: no expectation is computed
             for one side that the other does not share.
  UO_        the mapping DSL's Operation forms and literal lists (71-operation-surface.pure, 74-surface-last.pure),
             each through the mapping that holds it, over the seed's rows (seed.py's OP_CAR and OP_TRUCK, the expansion's OP_A, OP_B, OP_C, MG_A and MG_B):
             the router union (`special_union`), projected and fetched; the inheritance dispatch (`inheritance()`
             over two mapped subclasses), projected, filtered and fetched; a literal list as coalesce's one
             argument and as the list `in` tests (both refused by the engine, F83); the store's two joins as one
             chain; and the merge operation, projected. legend-engine's router reads a merge as its sets, a
             union's way (router_operations.pure), each row absent where its own set maps nothing; it merges
             instances only in an M2M graph fetch, and a graph fetch over these relational sets fails at plan
             generation, so none is written. The classes reached carry a profile of the corpus's own.
  UJ_        the joins no mapping used (report §8), each mapped and followed: both ends of the products' and the
             counterparty's associations (declared and never mapped), of new ones over the back office's, the OTC
             trades' and core-fx's joins (89-join-links.pure), an issuer's country through the hop HIER_COUNTRY was
             declared for, and each of surface::JoinDB's join conditions from J1 on its own (70-surface-store2.pure),
             its two not-equal operators moved there from tables no row was seeded into. Ten joins went instead (the
             user's decision, 2026-10-29); check() fails the build if a join is left that nothing names.
"""
from __future__ import annotations

import dense_mapping
from query import Pred, Proj, Spec, apply_temporal

DENSE_MAPPING = "stress::DenseMapping"
# store -> (the flat side's mapping and runtime, DenseMapping's runtime, the connection id, its ###Data): the corpus
# default for store::DB; a class of the second store reads it alone, through its own connection and data
BINDINGS = {
    "store::DB": (None, None, "stress::DenseRT", None, None),
    "external::EntityDB": ("external::LegalEntityMapping", "stress::EntityRT", "stress::DenseEntityRT", "entities",
                           "external::EntityData"),
}


def _projection(c, cls: str) -> list[Proj]:
    """Every property DenseMapping maps for the class, its identifier first: what both mappings must agree on."""
    ident = dense_mapping._identifier(c, cls)
    return [Proj(p, [p]) for p in [ident] + [p for p in sorted(c.columns[cls]) if p != ident]]


def dense_pairs(c, tables) -> list[tuple[Spec, Spec]]:
    """(flat, dense) for each class DenseMapping maps, in its seed order."""
    seeded = {t for t, rows in tables.items() if rows}
    out = []
    for cls in dense_mapping.candidates(c, seeded):
        short = cls.split("::")[-1]
        flat_mapping, flat_runtime, dense_runtime, conn, data = BINDINGS[dense_mapping._db_of(c, c.main_table[cls])]
        flat = Spec(f"stress::DMF_{short}", f"/stress/dense/flat/{short.lower()}",
                    f"Every column DenseMapping maps for {cls}, through the flat mapping: the control for "
                    f"DMD_{short}.", cls)
        flat.projections = _projection(c, cls)
        flat.mapping, flat.runtime, flat.connection, flat.data_element = flat_mapping, flat_runtime, conn, data
        apply_temporal(c, flat)
        dense = Spec(f"stress::DMD_{short}", f"/stress/dense/scoped/{short.lower()}",
                     f"The same columns through stress::DenseMapping, which spells {cls}'s mapping with scope "
                     f"blocks, a set id and an explicit ~primaryKey: it must return exactly what DMF_{short} does.",
                     cls)
        dense.projections, dense.as_of = list(flat.projections), flat.as_of
        dense.mapping, dense.runtime, dense.connection, dense.data_element = DENSE_MAPPING, dense_runtime, conn, data
        out.append((flat, dense))
    return out


# (mapping, runtime) of each Operation form's services
OPERATIONS = "surface::OperationSurface", "surface::OpRT"
INHERITANCE = "surface::InheritanceSurface", "surface::InheritanceRT"
MERGE = "surface::LastSurface", "surface::LastRT"
# database -> (its ###Data element, the connection id its runtimes give it): written into 99-unreached-services.pure
DATA = {"surface::OpDB": ("surface::OpData", "opEnv"), "surface::LastDB": ("surface::LastData", "lastEnv"),
        "surface::JoinDB": ("surface::JoinData", "joinEnv")}
# A store whose data element also carries tables of a store it INCLUDES: JoinDB's joins reach MapDB's LEG, one
# physical database through the include, so its connection's data must create LEG too
DATA_TABLES = {"surface::JoinDB": ["J1", "J2", "J3", "LEG"]}


def _op(name: str, doc: str, root: str, binding: tuple[str, str], db: str, cols: list[str],
        paradigm: str = "relation", graph: bool = False, filters=()) -> Spec:
    s = Spec(f"stress::UO_{name}", f"/stress/unreached/{name.lower()}", doc, root)
    if graph:
        s.graph = {p: None for p in cols}
    else:
        s.projections = [Proj(p, [p]) for p in cols]
    s.filters = list(filters)
    s.paradigm = paradigm
    s.mapping, s.runtime = binding
    s.data_element, s.connection = DATA[db]
    return s


def operations() -> list[Spec]:
    op, vehicle, code, link, merged = ("surface::Op", "surface::OpVehicle", "surface::OpCode", "surface::OpLink",
                                       "surface::MergeTgt")
    ops_db, last_db = "surface::OpDB", "surface::LastDB"
    return [
        _op("RouterUnion", "The router union over OP_A and OP_B: every row of each set, concatenated.", op,
            OPERATIONS, ops_db, ["k", "v"]),
        _op("RouterUnionTds", "The router union, projected as a TDS.", op, OPERATIONS, ops_db, ["k", "v"],
            paradigm="tds"),
        _op("RouterUnionTree", "The router union, fetched as a graph.", op, OPERATIONS, ops_db, ["k", "v"],
            graph=True),
        _op("Inheritance", "inheritance(): the parent's rows are its mapped leaf subtypes', OP_CAR's and "
                           "OP_TRUCK's, though no query names a subtype.", vehicle, INHERITANCE, ops_db,
            ["k", "label"]),
        _op("InheritanceFilter", "inheritance() under a filter on a property every leaf maps.", vehicle,
            INHERITANCE, ops_db, ["k"], filters=[Pred(["label"], "isNotEmpty", None)]),
        _op("InheritanceTree", "inheritance(), fetched as a graph.", vehicle, INHERITANCE, ops_db,
            ["k", "label"], graph=True),
        _op("LiteralListCoalesce", "A literal list as coalesce's one argument, `coalesce([V, K])`: the key where "
                                   "the value is absent.", code, OPERATIONS, ops_db, ["k", "firstOf"]),
        _op("LiteralListIn", "A literal list `in` tests against, `in(K, ['OB-0001', 'OB-0004'])`.", code, OPERATIONS,
            ops_db, ["k", "listed"]),
        _op("JoinChain", "The store's two joins as one chain, OP_A to the OP_B its NEXT_K names and on to OP_C: "
                         "a hop that lands twice, on an absent value, from a NULL key and an orphan.", link,
            OPERATIONS, ops_db, ["k", "nextV", "nextCode"]),
        _op("JoinChainExpression", "The same chain ending in an expression, `@opA_opB > @opB_opC | toLower(OP_C.V)`: "
                                   "the call evaluated on the row the chain lands on, absent where it lands on "
                                   "nothing.", link, OPERATIONS, ops_db, ["k", "codeLower"]),
        _op("Merge", "The merge operation, projected: its two sets concatenated, each row absent where its own set "
                     "maps nothing, though each table holds a value there.", merged, MERGE, last_db,
            ["k", "a", "b"]),
        _op("MergeTds", "The merge operation, projected as a TDS.", merged, MERGE, last_db, ["k", "a", "b"],
            paradigm="tds"),
    ]


def data_elements(c, tables) -> str:
    """The ###Data the UO_ services' connections reference, one element per store."""
    import emit

    return "\n\n".join(emit.store_data_element(c, tables, db, elem, only=DATA_TABLES.get(db))
                       for db, (elem, _conn) in sorted(DATA.items()))


LINKS = "stress::LinksRT"
# J2's least N: the range service keeps the hosts at or above it, each reaching one J2 row or more
RANGE_FLOOR = 157


def _link(name: str, doc: str, root: str, mapping: str, cols: list, filters=(), runtime: str = LINKS,
          data: tuple[str, str] | None = None) -> Spec:
    s = Spec(f"stress::UJ_{name}", f"/stress/unreached/joins/{name.lower()}", doc, root)
    s.projections = [p if isinstance(p, Proj) else Proj(p.replace(".", "_"), p.split(".")) for p in cols]
    s.filters = list(filters)
    s.mapping, s.runtime = mapping, runtime
    if data:
        s.data_element, s.connection = data
    return s


# products::Instrument's end -> (its subclass, that class's identifier), behind the joins *_Instrument
_LEGS = {"equity": ("products::Equity", "equityId"), "bond": ("products::Bond", "bondId"),
         "future": ("products::Future", "futureId"), "fxForward": ("products::FxForward", "forwardId"),
         "swap": ("products::Swap", "swapId"), "cds": ("products::CreditDefaultSwap", "cdsId"),
         "structuredProduct": ("products::StructuredProduct", "productId"), "etf": ("products::ETF", "etfId")}


def joins() -> list[Spec]:
    """The joins no mapping used before Phase 7, each through the mapping 89-join-links.pure or 70-surface-store2.pure
    gives it: both ends of every association, and each of J1's join conditions on its own."""
    legs, cpty, recon, otc, fx = ("products::InstrumentLegsMapping", "counterparty::ClassificationLinkMapping",
                                  "backoffice::ReconLinksMapping", "derivatives::OtcCounterpartyMapping",
                                  "fxlinks::FxLinksMapping")
    out = [_link("InstrumentLegs", "Each instrument's equity, bond, future, FX forward, swap, CDS, structured product "
                                   "and ETF, through the subclass tables' joins to INSTRUMENT.", "products::Instrument",
                 legs, ["instrumentId"] + [f"{end}.{ident}" for end, (_cls, ident) in _LEGS.items()])]
    for end, (cls, ident) in _LEGS.items():
        short = cls.split("::")[-1]
        out.append(_link(f"{short}Instrument", f"Each {short} row's instrument: its key absent, an orphan, or "
                                               f"an instrument's.", cls, legs, [ident, "instrument.ticker"]))
    out += [
        _link("CounterpartyClassification", "A counterparty's classification, through Counterparty_Classification.",
              "counterparty::Counterparty", cpty, ["counterpartyId", "classification.classificationType"]),
        _link("ClassificationCounterparty", "A classification's counterparty, its key absent or an orphan or a "
                                            "counterparty's.", "counterparty::Classification", cpty,
              ["classificationId", "counterparty.legalName"]),
        _link("ReconItemLinks", "A reconciliation item's payment and movement: an unmatched item has one and not the "
                                "other.", "backoffice::ReconciliationItem", recon,
              ["reconId", "reconPayment.amount", "reconMovement.amount"]),
        _link("PaymentReconItems", "Whether a payment has a reconciliation item.", "backoffice::Payment", recon,
              ["paymentId", Proj("noRecon", ["paymentReconItems"], agg="isEmpty")]),
        _link("MovementReconItems", "Whether a nostro movement has a reconciliation item.", "backoffice::NostroMovement",
              recon, ["movementId", Proj("noRecon", ["movementReconItems"], agg="isEmpty")]),
        _link("OtcTradeCounterparty", "An OTC trade's counterparty, through OtcTrade_Counterparty.",
              "derivatives::OtcTrade", otc, ["otcId", "otcCounterparty.legalName"]),
        _link("CounterpartyOtcTrades", "Whether a counterparty has an OTC trade.", "counterparty::Counterparty", otc,
              ["counterpartyId", Proj("noOtc", ["otcTrades"], agg="isEmpty")]),
        _link("FxRateLinks", "A rate's pair and its publisher, through Cfx_RatePair and Cfx_RateSource.",
              "core_fx::CfxRate", fx, ["pairCode", "cobDate", "rateType", "ratePair.baseCurrency",
                                       "fixingSource.sourceName"]),
        _link("FxCrossLegs", "A cross rate's two legs' pairs, through Cfx_CrossLegOnePair and Cfx_CrossLegTwoPair.",
              "core_fx::CfxCrossRate", fx, ["crossCode", "cobDate", "legOnePair.baseCurrency",
                                            "legTwoPair.quoteCurrency"]),
        _link("FxPairUses", "Whether a pair has rates, and crosses through either leg.", "core_fx::CfxCurrencyPair", fx,
              ["pairCode", Proj("noRates", ["pairRates"], agg="isEmpty"),
               Proj("noLegOne", ["legOneCrosses"], agg="isEmpty"), Proj("noLegTwo", ["legTwoCrosses"], agg="isEmpty")]),
        _link("FxSourceRates", "Whether a fixing source publishes a rate.", "core_fx::CfxFixingSource", fx,
              ["sourceCode", Proj("noRates", ["sourceRates"], agg="isEmpty")]),
        _link("IssuerCountry", "An issuer's country name through HierIssuer_Country, the hop HIER_COUNTRY was declared "
                               "for.", "hier::IssuerCountry", "hier::IssuerCountryMapping",
              ["issuerId", "countryCode", "countryName"], runtime="hier::IssuerCountryRT",
              data=("hier::IssuerData", "issuerEnv")),
    ]
    host, join_data = "surface::JoinHost", DATA["surface::JoinDB"]
    for name, prop, doc in (
            ("JoinIsNull", "nullLeg", "`J1.NEXT_K is null and J2.K = J1.K`"),
            ("JoinIsNotNull", "notNullTail", "`J1.NEXT_K is not null and J3.K = J1.K`"),
            ("JoinNumericConst", "constLeg", "`J1.N = -5 and J2.K = J1.K`, a numeric constant"),
            ("JoinArrayArg", "listedLeg", "`in(J1.N, [1, 2, 3]) and J2.K = J1.K`, a literal list"),
            ("JoinNestedDyna", "dynaLeg", "`concat(toUpper(J1.K), J1.NEXT_K) = J2.K`, a function over a function"),
            ("JoinCrossDb", "legLabel", "`[JoinDB]J1.LEG_ID = [MapDB]LEG.LEG_ID`, a hop into the included database"),
            ("JoinNotEqual", "neLeg", "`J1.NEXT_K = J2.K and J2.NEXT_K != J1.K`: SQL's `!=`, matching nothing on a "
                                      "NULL"),
            ("JoinAnsiNotEqual", "ansiNeLeg", "`J1.NEXT_K = J2.K and J2.NEXT_K <> J1.K`, the ANSI spelling")):
        out.append(_link(name, f"J1's row through the join {doc}.", host, "surface::JoinSurface", ["k", prop],
                         runtime="surface::JoinRT", data=join_data))
    out.append(_link("JoinThreeLinks", "J1 to J2 to J3, a chain of two joins over three tables.", host,
                     "surface::JoinSurface", ["k", "nextN", "tailN"], runtime="surface::JoinRT", data=join_data))
    # count() over the range's ends, only where every host has one: F6 makes count() of an empty to-many 1
    out.append(_link("JoinRange", "The J2 rows at or below J1's N, `J1.N >= J2.N`: a to-many end over a range, counted "
                                  "for the hosts that reach one.", host, "surface::JoinSurface",
                     ["k", "n", Proj("atOrBelowCount", ["atOrBelow"], agg="count")],
                     filters=[Pred(["n"], ">=", RANGE_FLOOR)], runtime="surface::JoinRT", data=join_data))
    return out


def build(c, tables) -> list[Spec]:
    return [s for pair in dense_pairs(c, tables) for s in pair] + operations() + joins()


def invariance_groups(specs: list[Spec]) -> list[list[Spec]]:
    """Each DMF_ with its DMD_."""
    by = {s.short: s for s in specs}
    return [[by[n], by["DMD_" + n[len("DMF_"):]]] for n in sorted(by) if n.startswith("DMF_")]


def unused_joins(c) -> list[str]:
    """Joins no mapping, view or filter names (`@J`; comments stripped). The improvement plan's Phase 7 left none: each
    was mapped or deleted, and this keeps it so."""
    import re

    import model

    used = set()
    for f in model.linked_files() + model.stress_sources():
        for line in f.read_text(encoding="utf-8").splitlines():
            used.update(re.findall(r"@(\w+)", model._strip(line)))
    return sorted(set(c.joins) - used)


def check(c, specs: list[Spec], tables) -> list[str]:
    """Every pair has rows: a pair that agrees on nothing asserts nothing. Each of J1's join conditions lands on a value
    for one row and misses for another (seed.J1's rows are built against the expansion's J2, J3 and LEG, which move
    with the seed), and the range counts at least one J2 row for every host it keeps (F6 makes count() of nothing 1)."""
    import oracle

    problems = [f"{s.short}: no rows" for s in specs if s.short.startswith("DMF_") and not oracle.evaluate(c, s, tables)]
    problems += [f"join {j} is used by no mapping, view or filter: map it or delete it" for j in unused_joins(c)]
    for s in specs:
        if not s.short.startswith("UJ_Join"):
            continue
        rows = oracle.evaluate(c, s, tables)
        if s.short == "UJ_JoinRange":
            if len(rows) < 2 or any(not r["atOrBelowCount"] for r in rows):
                problems.append(f"{s.short}: every kept host must reach a J2 row: {rows}")
            continue
        for p in s.projections[1:]:
            vals = [r[p.alias] for r in rows]
            if all(v is None for v in vals) or all(v is not None for v in vals):
                problems.append(f"{s.short}: {p.alias} must land for one row and miss for another: {vals}")
    return problems
