"""
Semi-structured documents: the services of 99-semi-services.pure (docs/STRESS_CORPUS_PLAN_2026_10_19.md, Phase 9).

The corpus declared two semi-structured columns, 66-store-surface.pure's SURFACE_TYPES.C_SEMI (SEMISTRUCTURED) and
C_JSON (JSON), and no service read either. The semi:: pack (8118-semi.pure) maps classes over C_SEMI and over a
SEMISTRUCTURED column of its own store, SEMI_DOC.DOC, and its seed (`_SEMI_DOCS` in seed.py) puts the same five booking
payloads in both. These services read them, each construct on each side:

  SJ_<Name>   over SURFACE_TYPES, a TDS (`project([...], [...])`), whose tests legend-engine 4.145.0 runs on H2
  SD_<Name>   over SEMI_DOC, a Relation (`project(~[...])`), whose tests it runs on DuckDB

    Text, Numbers, Dates   NAVIGATION: extractFromSemiStructured of a key, each type of the whitelist the class reads
    Index, Quoted          ARRAY INDEX and path edges: an element of an object array, an index past the end, consecutive
                           indices into an array of arrays, bracket-quoted keys holding a space and a dot
    BigQty, RefFilter      a query filter over a navigated value
    StoreFilter            a store Filter over a navigated value, named by a class mapping's ~filter
    Body, BodyDates        a Binding over the column: its keys, and a nested object's
    Flatten, FlattenLegs   FLATTEN: a Binding's to-many, one row per element, a NULL row for none
    Explode, ExplodeAny    EXPLODE: a join that fans a document's legs into rows of SEMI_HOP, projected one row per
                           joined row, and as isNotEmpty
    Fee                    a DECIMAL navigation, which neither database answers (docs/UPSTREAM_FINDINGS.md; quarantined)
    SD_HopList ... SD_SortedHops   WILDCARD PATHS and the ARRAY FUNCTIONS over them, the DuckDB side alone

What the pin was probed for before any service was written (2026-10-31, models under the session's scratchpad; each
answer below is the engine's on both databases unless one is named):
  - A service's tests run on H2 unless its function returns a Relation, which runs them on DuckDB (legend-engine's
    ServiceTestRunner: `TestReturnTypeHelper.isRelationReturnType` sets the relation hint, and RelationalConnectionFactory
    builds a DuckDB test connection for it). The plan's premise that service tests always run on H2 holds for TDS and
    graph services only.
  - DuckDB cannot create SURFACE_TYPES: its REAL and OTHER columns fail the test connection's setup ("Match failure:
    RealObject instanceOf Real", then "OtherObject instanceOf Other" with REAL removed). So the Relation services read
    SEMI_DOC, and the TDS services read SURFACE_TYPES.
  - A JSON-typed column takes no value: both loaders write the cell unquoted, as SQL (H2: a syntax error; DuckDB:
    "Binder Error: Referenced column ... not found"). C_JSON stays NULL, and nothing maps it.
  - The two loaders read a quoted CSV field's quotes differently. DuckDB's is RFC 4180 (a doubled quote is one quote;
    a lone inner quote is "invalid char between encapsulated token and delimiter"); H2's keeps every character between
    the outer quotes, doubled quotes included, so an RFC-encoded document reaches H2 as invalid JSON and every
    navigation over it raises. So each side has its own ###Data: SurfaceData writes a document between quotes with its
    inner quotes as they are, which H2's splitter reads by parity (no JSON string may hold a comma or a quote), and
    DuckData writes it as RFC 4180 does.
  - H2's flatten (a Binding's to-many, and explode) calls legend_h2_extension_flatten_array, which reads the whole base
    table itself and raises on a NULL document ("argument content is null"), through a view that filters NULL documents
    out too. So SURFACE_TYPES holds no NULL document; SEMI_DOC holds one (SD-06), which DuckDB flattens to nothing.
  - On H2 a wildcard path or an array_* function cannot be planned ("Couldn't find DynaFunction to Postgres model", or
    "Match failure: MapRelationalLambdaObject"), so those are the DuckDB side's alone. A wildcard of two levels in one
    path, 'grid[*][*]', fails to compile on both ("has an empty segment around a [*]").
  - A graph fetch over an extracted property fails at planning ("Type inference not supported yet! Dyna function:
    extractFromSemiStructured"), and over a Binding ("Cannot cast a collection of size 0 to multiplicity [1]"); a
    graph fetch over the exploded join runs. No service fetches a graph.
  - `->count()` over the exploded join answers 1 for a document with no leg (F6's shape), and `->size()` of a Binding's
    to-many is invalid SQL on both (the projection's column is not grouped). ExplodeAny reads isNotEmpty, which agrees.
Lite (2026-10-31, the stress tool over the same models): its model build reads every construct here; at query time it
reads a one-key path and the store filter as the engine does, answers null for a dotted, indexed or quoted path, fails a
wildcard or an array function (the mapping fails to normalize), does not know explodeSemiStructured, and reads a
Binding's column as a Variant where the class is expected.
"""
from __future__ import annotations

import json

from query import Pred, Proj, Spec

SURFACE = "semi::SurfaceDocs", "semi::SurfaceRT", "semi::SurfaceData"
DUCK = "semi::DuckDocs", "semi::DuckRT", "semi::DuckData"
CONNECTION = "semiEnv"
# Each construct's class, per side (8118-semi.pure says why each has its own).
ROOTS = {side: {k: f"semi::{side[0]}{side[1].lower()}{v}" for k, v in
                (("doc", "Doc"), ("big", "Big"), ("bound", "Bound"), ("route", "Route"), ("arrays", "Arrays"))}
         for side in ("SJ", "SD")}


def _spec(side: str, name: str, doc: str, root: str, cols, filters=(), fan_out: bool = False) -> Spec:
    mapping, runtime, data = SURFACE if side == "SJ" else DUCK
    s = Spec(f"stress::{side}_{name}", f"/stress/semi/{side.lower()}/{name.lower()}", doc, root)
    s.projections = [c if isinstance(c, Proj) else Proj(c[0], c[1]) for c in cols]
    s.filters = list(filters)
    s.mapping, s.runtime, s.data_element, s.connection = mapping, runtime, data, CONNECTION
    s.paradigm = "tds" if side == "SJ" else "relation"
    s.fan_out = fan_out
    return s


def _side(side: str) -> list[Spec]:
    r = ROOTS[side]
    doc, big, bound, route = r["doc"], r["big"], r["bound"], r["route"]
    key = ("docKey", ["docKey"]) if side == "SJ" else ("docId", ["docId"])
    db = "H2" if side == "SJ" else "DuckDB"
    out = [
        _spec(side, "Text", f"Navigation to a string, as VARCHAR and as STRING, on {db}: a JSON null and an absent key "
                            "are both absent.", doc, [key, ("ref", ["ref"]), ("refName", ["refName"])]),
        _spec(side, "Numbers", f"Navigation to an INTEGER, a FLOAT and a BOOLEAN, on {db}: a zero, a negative, a "
                               "false and absent keys.", doc,
              [key, ("qty", ["qty"]), ("px", ["px"]), ("live", ["live"])]),
        _spec(side, "Dates", f"Navigation along a dotted path to a DATE, a TIMESTAMP and a DATETIME, on {db}; three "
                             "documents have no dates object.", doc,
              [key, ("tradeDate", ["tradeDate"]), ("stamp", ["stamp"]), ("stampAt", ["stampAt"])]),
        _spec(side, "Fee", f"Navigation to a DECIMAL, on {db}: 12.5 and 0.25 as they are written. The pinned engine "
                           "answers neither (docs/UPSTREAM_FINDINGS.md); quarantined.", doc, [key, ("fee", ["fee"])]),
        _spec(side, "Index", f"Array index on {db}: the first leg's hop, the second leg's venue (past the end of a "
                             "one-leg document, absent for a null venue), and consecutive indices into an array of "
                             "arrays.", doc,
              [key, ("firstHop", ["firstHop"]), ("secondVenue", ["secondVenue"]), ("cell", ["cell"]),
               ("lowerCell", ["lowerCell"])]),
        _spec(side, "Quoted", f"Bracket-quoted keys holding a space and a dot, on {db}.", doc,
              [key, ("book", ["book"])]),
        _spec(side, "BigQty", f"A query filter over a navigated INTEGER, on {db}: an absent qty is not over 100.", doc,
              [key, ("ref", ["ref"]), ("qty", ["qty"])], [Pred(["qty"], ">", 100)]),
        _spec(side, "RefFilter", f"A query filter over a navigated string, on {db}.", doc,
              [key, ("qty", ["qty"])], [Pred(["ref"], "==", "TKT-1002")]),
        _spec(side, "StoreFilter", f"A store Filter over a navigated INTEGER, the class mapping's ~filter, on {db}.",
              big, [key, ("ref", ["ref"])]),
        _spec(side, "Body", f"A Binding over the semi-structured column, on {db}: the bound class's keys.", bound,
              [key, ("ref", ["body", "ref"]), ("qty", ["body", "qty"]), ("px", ["body", "px"]),
               ("live", ["body", "live"])]),
        _spec(side, "BodyDates", f"A Binding's nested object, on {db}: absent where the document has no dates.",
              bound,
              [key, ("tradeDate", ["body", "dates", "trade"])]),
        _spec(side, "Flatten", f"Flatten on {db}: a Binding's to-many, one row per leg; an empty or absent array is "
                               "one row with no venue, and so is a leg whose venue is null.", bound,
              [key, ("venue", ["body", "legs", "venue"])], fan_out=True),
        _spec(side, "FlattenLegs", f"Flatten on {db}: two keys of each leg, read from the same element.", bound,
              [key, ("hop", ["body", "legs", "hop"]), ("legQty", ["body", "legs", "qty"])], fan_out=True),
        _spec(side, "Explode", f"Explode on {db}: each leg's hop joined to SEMI_HOP, one row per joined leg; a leg "
                               "naming no hop drops out, and a document with none is one row with no hop.", route,
              [key, ("hopId", ["hops", "hopId"]), ("label", ["hops", "label"])], fan_out=True),
        _spec(side, "ExplodeAny", f"Explode on {db}, as isNotEmpty over the joined hops.", route,
              [key, Proj("hasHop", ["hops"], agg="isNotEmpty")]),
    ]
    if side == "SD":
        doc = r["arrays"]
        out += [
            _spec(side, "HopList", "A wildcard path, 'legs[*].hop', joined by array_to_string: absent with no leg.",
                  doc, [key, ("hopList", ["hopList"])]),
            _spec(side, "LegCount", "array_size of 'legs[*]': 0 with no leg, and over a NULL document.", doc,
                  [key, ("legCount", ["legCount"])]),
            _spec(side, "MaxQty", "array_max of 'legs[*].qty' as INTEGER[]: absent with no leg.", doc,
                  [key, ("maxQty", ["maxQty"])]),
            _spec(side, "SumQty", "array_sum of 'legs[*].qty' as INTEGER[]: absent with no leg.", doc,
                  [key, ("sumQty", ["sumQty"])]),
            _spec(side, "OnXnas", "array_contains of 'legs[*].venue', 'XNAS': false with no leg, and beside a null "
                                  "venue.", doc, [key, ("onXnas", ["onXnas"])]),
            _spec(side, "SortedHops", "array_sort, then array_to_string, of 'legs[*].hop'.", doc,
                  [key, ("sortedHops", ["sortedHops"])]),
        ]
    return out


def specs(c) -> list[Spec]:
    return _side("SJ") + _side("SD")


# ------------------------------------------------------------------------------------------- the two ###Data elements

def _h2_cell(v) -> str:
    """A cell as H2's test-data loader reads it: a document holding a comma between quotes, its inner quotes as they
    are, which the loader's splitter reads by parity (an inner quote toggles in and out of a JSON string, so a
    structural comma is always inside the field's quotes); a document with no comma bare, since the loader unwraps a
    quoted field only when it holds one (`"{}"` reached H2 with its quotes, 2026-10-31); any other value as emit
    writes it."""
    import emit

    if isinstance(v, str) and v[:1] in "{[":
        return '"' + v + '"' if "," in v else v
    return emit._csv_cell(v)


def _element(c, tables, name: str, which: list[tuple[str, list[str]]], cell) -> str:
    import emit

    out = ["###Data", f"Data {name}", "{", "  Relational", "  #{"]
    for table, cols in which:
        lines = [",".join(cols)] + [",".join(cell(r.get(col)) for col in cols) for r in tables[table]]
        out.append(f"    {emit._ident(c.tables[table].schema)}.{emit._ident(table)}:")
        out.append(" +\n".join(f"      '{emit._pure_str(l)}\\n'" for l in lines) + ";")
    out += ["  }#", "}"]
    return "\n".join(out)


def data_elements(c, tables) -> str:
    """SurfaceData, for the H2 side: SURFACE_TYPES's key and semi-structured columns alone (H2's insert of every
    declared column, the BINARY, ARRAY and OTHER ones NULL, is a syntax error), and SEMI_HOP. DuckData, for the DuckDB
    side: SEMI_DOC and SEMI_HOP, each cell as RFC 4180 writes it."""
    import emit

    hop = ("SEMI_HOP", list(c.tables["SEMI_HOP"].columns))
    surface = _element(c, tables, SURFACE[2], [hop, ("SURFACE_TYPES", ["TK", "TK2", "C_SEMI", "C_JSON"])], _h2_cell)
    duck = _element(c, tables, DUCK[2], [("SEMI_DOC", list(c.tables["SEMI_DOC"].columns)), hop], emit._csv_cell)
    return surface + "\n\n" + duck


# --------------------------------------------------------------------------------------------------------- the check

def _strings(v):
    if isinstance(v, str):
        yield v
    elif isinstance(v, dict):
        for k, x in v.items():
            yield k
            yield from _strings(x)
    elif isinstance(v, list):
        for x in v:
            yield from _strings(x)


def check(c, tables, specs_) -> list[str]:
    """The documents travel to both databases (no JSON string with a comma, a quote, an apostrophe or a backslash; no
    NULL document where H2 flattens), and each side's shapes stay observable: a flatten row with no element, an
    exploded leg that joins nothing, an index past the end, a NULL document on the DuckDB side."""
    import oracle

    problems = []
    for table, col in (("SURFACE_TYPES", "C_SEMI"), ("SEMI_DOC", "DOC")):
        for r in tables[table]:
            raw = r.get(col)
            if raw is None:
                if table == "SURFACE_TYPES":
                    problems.append(f"{table}: a NULL document, over which H2's flatten raises")
                continue
            bad = [s for s in _strings(json.loads(raw)) if any(ch in s for ch in ",\"'\\")]
            if bad:
                problems.append(f"{table}: {bad[:3]} cannot travel in H2's test data or a Pure string")
    if all(r.get("C_JSON") is None for r in tables["SURFACE_TYPES"]) is False:
        problems.append("SURFACE_TYPES.C_JSON holds a value, which neither test database loads at the pin")
    if not any(r.get("DOC") is None for r in tables["SEMI_DOC"]):
        problems.append("SEMI_DOC has no NULL document")
    named = {s.short: s for s in specs_}
    for side in ("SJ", "SD"):
        flat = oracle.evaluate(c, named[f"{side}_Flatten"], tables)
        if not any(r["venue"] is None for r in flat) or len(flat) <= len(tables["SURFACE_TYPES"]):
            problems.append(f"{side}_Flatten: no row without a venue, or no document with several legs")
        exploded = oracle.evaluate(c, named[f"{side}_Explode"], tables)
        if any(r["hopId"] == "HP-9" for r in exploded) or not any(r["hopId"] is None for r in exploded):
            problems.append(f"{side}_Explode: HP-9 joined, or no document lands on nothing")
        index = oracle.evaluate(c, named[f"{side}_Index"], tables)
        if not any(r["secondVenue"] is None and r["firstHop"] is not None for r in index):
            problems.append(f"{side}_Index: no document with an index past the end")
    return problems
