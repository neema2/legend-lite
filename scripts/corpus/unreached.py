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
DATA = {"surface::OpDB": ("surface::OpData", "opEnv"), "surface::LastDB": ("surface::LastData", "lastEnv")}


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

    return "\n\n".join(emit.store_data_element(c, tables, db, elem) for db, (elem, _conn) in sorted(DATA.items()))


def build(c, tables) -> list[Spec]:
    return [s for pair in dense_pairs(c, tables) for s in pair] + operations()


def invariance_groups(specs: list[Spec]) -> list[list[Spec]]:
    """Each DMF_ with its DMD_."""
    by = {s.short: s for s in specs}
    return [[by[n], by["DMD_" + n[len("DMF_"):]]] for n in sorted(by) if n.startswith("DMF_")]


def check(c, specs: list[Spec], tables) -> list[str]:
    """Every pair has rows: a pair that agrees on nothing asserts nothing."""
    import oracle

    return [f"{s.short}: no rows" for s in specs if s.short.startswith("DMF_") and not oracle.evaluate(c, s, tables)]
