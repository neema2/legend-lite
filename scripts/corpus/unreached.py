"""
Declared but unreached: the services of 99-unreached-services.pure (docs/STRESS_CORPUS_PLAN_2026_10_19.md, Phase 7).

The corpus declared mapping and store constructs that no service ran, each scoring as present in the feature report
while nothing could detect it behaving wrongly (report §3's ○ rows, §8's unused joins). These services reach them:

  DMF_/DMD_  each class stress::DenseMapping maps (59-dense-mapping.pure), projected column for column through the
             flat mapping (stress::AllMapping, DMF_) and through DenseMapping (DMD_, on stress::DenseRT). DenseMapping
             respells the flat mapping with scope blocks in both forms, set ids, root markers and an explicit
             ~primaryKey, and means the same thing, so each pair is an invariance group: no expectation is computed
             for one side that the other does not share.
"""
from __future__ import annotations

import dense_mapping
from query import Proj, Spec, apply_temporal

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


def build(c, tables) -> list[Spec]:
    return [s for pair in dense_pairs(c, tables) for s in pair]


def invariance_groups(specs: list[Spec]) -> list[list[Spec]]:
    """Each DMF_ with its DMD_."""
    by = {s.short: s for s in specs}
    return [[by[n], by["DMD_" + n[len("DMF_"):]]] for n in sorted(by) if n.startswith("DMF_")]


def check(c, specs: list[Spec], tables) -> list[str]:
    """Every pair has rows: a pair that agrees on nothing asserts nothing."""
    import oracle

    return [f"{s.short}: no rows" for s in specs if s.short.startswith("DMF_") and not oracle.evaluate(c, s, tables)]
