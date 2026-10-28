"""
Generates the COMBINATION matrix: mapping features stacked on each other, not merely present.

The taxonomy reached "35 of 35 features present" while four classes carried every advanced
construct in the corpus and exactly two pairs ever co-occurred (chain+dyna on one class,
binding+embedded on another). Presence is not coverage. A feature that appears once has been
shown to work in one shape, and the defects this corpus has actually found -- concat over
NULL, count over an empty to-many, a chain filter that excluded nothing -- all live at the
point where one feature meets another.

So the axes are enumerated and crossed rather than illustrated:

  PROPERTY axes                            CLASS-MAPPING axes
    reach    col | chain1 | chain2           filter    none | direct | chain
    type     string | int | float | bool     extends   no | yes
    xform    per type -- see XFORM
    nulls    notnull | nullable
    host     top | embedded

The transform axis DEPENDS on the type axis, because a dynafunction's arguments and result
have to agree with the property it maps, so the cross is over the per-type transform lists
rather than one shared list: 180 cells, every one generated. The class cross is 3x2 = 6, and
the property cells are dealt across the six so that each property shape also meets each
class shape. What that buys over 48 separate one-feature tests is the interaction: a
dynafunction over a two-hop chain, inside an embedded block, on a set that extends another
set and carries a filter reached through joins, where the source column is NULL for some rows.

WHY THE SOURCE DATA MATTERS MORE THAN THE MATRIX. Every table here is reachable from the
seed's ordinary expansion, so each foreign key carries the adversarial shapes (absent key,
dangling key, childless parent) and each nullable column carries a NULL. Without that a
combination test is a shape with nothing interesting flowing through it -- which is exactly
what the corpus's first join chain was, resolving for every row because two tables' generated
values coincided.

Nothing here is asserted from engine output. Each generated property is evaluated by
oracle.py from the seed, and a cell whose value the oracle cannot compute independently is
not generated at all -- it is reported by `unsupported()` so the gap is visible rather than
quietly absent.
"""
from __future__ import annotations

import emptiness
import model
from query import Proj, Spec

DB = "combo::ComboDB"
MAPPING = "combo::ComboMapping"
RUNTIME = "combo::ComboRT"
CONN_ID = "comboEnv"
DATA = "combo::ComboData"

ROOT, HOP1, HOP2 = "COMBO_ROOT", "COMBO_HOP1", "COMBO_HOP2"
# A fourth table reached by the JOIN FORMS the corpus declared but never executed. Its rows
# are DERIVED from COMBO_ROOT's (see derive_alt) so each form pairs deterministically and
# to-one; generated independently, a multi-column or dynafunction condition matches nothing
# at all, and a chain over it returns NULL for every row -- a green test proving nothing,
# which is precisely what the corpus's first join chain was.
ALT = "COMBO_ALT"
J1, J2 = "Combo_Hop1", "Combo_Hop2"
# One join per form. Each is the SAME shape dense_store.py generates, but over data built to
# satisfy it.
J_MULTI, J_INEQ, J_OR = "Combo_Multi", "Combo_Ineq", "Combo_Or"
J_DYNA1, J_DYNA2 = "Combo_Dyna1", "Combo_Dyna2"

# Derivation columns on COMBO_ALT: each exists to be the far side of one join form.
ALT_KEY, ALT_LOW, ALT_CAT, ALT_UP = "A_KEY", "A_LOW", "A_CAT", "A_UP"
SENTINEL = "ALT-SENTINEL"
# A table inside a Schema block, and the class mapped over it. Both the mapping reference
# (`[db]schema.TABLE.COL`) and the ###Data key (`schema.TABLE`) were verified against the
# engine before this existed; the corpus had declared a Schema and mapped over none.
SCHEMA = "analytics"
SCHEMA_TABLE = "COMBO_SUMMARY"
SCHEMA_CLASS = "combo::Summary"
SCHEMA_ASSOC = "combo::RootSummary"
J_SUMMARY = "Combo_Summary"
# A class mapped through a SCOPE block, and one reached by an association whose ends name
# both set ids. Both constructs existed in the corpus and neither was executed: the 60
# scope-using class mappings live in a Mapping no runtime binds, and the only association
# with explicit src/tgt ids spans two stores, so navigating it hits F26.
SCOPE_CLASS = "combo::Scoped"
HOP_CLASS = "combo::Hop"
ASSOC = "combo::RootHop"
# An OTHERWISE mapping whose inline branch always applies. F13 breaks the FALLBACK -- an
# Otherwise never falls back under TDS projection -- so the only service using one is
# quarantined and the construct had no passing demonstration. The inline branch is a
# separate half and it works, so it is covered here and the broken half stays pinned by
# O1_CounterpartyOtherwise. Same split aggregates.py makes around F6.
OTW_CLASS = "combo::WithOtherwise"
OTW_TARGET = "combo::OtherwiseTarget"
# The inline branch and the fallback map the SAME class, and this reader keys a property's
# column by class rather than by (mapping, class) -- so the two sets' columns collapse into
# one view and the model fails to resolve unless every name exists on both tables. Hence one
# pair of column names carried by COMBO_ROOT and COMBO_HOP1 alike.
#
# The VALUES still differ: the seeder generates per (column, row index), and a root row joins
# to a hop row at a different index -- or to none at all, where the key is absent. So the
# expectation distinguishes the branches even though the column names do not.
OTW_COLS = ("OTW_CODE", "OTW_LABEL")

# ---------------------------------------------------------------- the axes
REACH = ("col", "chain1", "chain2")
# Pure type per axis value, and the SQL type behind it. Several values share a Pure type on
# purpose: DECIMAL and DOUBLE are both Float[0..1] to the model, and whether they behave the
# same through a transform is exactly the question -- a corpus using only DOUBLE cannot ask
# it. DATE and TIMESTAMP earn their place separately: their RENDERING is where F24 lives.
# REAL is ABSENT deliberately. It parses, compiles, and then fails at execution on DuckDB
# with "Match failure: RealObject instanceOf Real" in the connector's type conversion -- see
# F31. Every other type here round-trips, which is what identifies REAL rather than the
# probe being wrong: scripts/corpus/repro/real-type-unconvertible/ runs all eleven.
TYPES = ("string", "char", "int", "bigint", "smallint",
         "float", "decimal", "bool", "date", "timestamp")
NULLS = ("notnull", "nullable")
HOST = ("top", "embedded")

# `multigrain` is a MultiGrainFilter, which a mapping references exactly like a Filter --
# the difference is consumed by the planner for join elision. Included as its own axis value
# because otherwise the corpus can declare one and never find out whether a mapping using it
# even executes: it was declared once in the generated dense store and referenced nowhere.
FILTERS = ("none", "direct", "chain", "multigrain")
EXTENDS = ("no", "yes")
# How a class mapping is WRITTEN, not what it maps. `scope([db]TABLE)` states the table once
# and names its columns bare; the alternative repeats `[db]TABLE.` on every one. Crossed as
# an axis because it is a spelling every property mapping can be written in, so covering it
# once says nothing about whether a TRANSFORM survives being written that way -- and the
# engine does accept a dynafunction over bare names inside a scope.
WRITTEN = ("plain", "scope")

# Transforms are PER TYPE, because a dynafunction's arguments and result have to agree with
# the property it maps. Every one preserves its argument's type, so a cell's Pure type is
# decided by the type axis alone.
#
# Each was verified usable in a relational property mapping before being listed -- nothing
# validates a dynafunction name at parse OR compile, so an unusable one fails at SQL
# generation with an error far from the mapping. `plus`/`minus`/`times` in particular are
# binary here, which was a guess worth checking rather than assuming from Pure's list form.
XFORM = {
    # No `sub`: relational `substring` disagrees with the in-memory one (F37), and a matrix
    # cell asserting either answer would be asserting a defect. It has its own pair of
    # services in 77-substring-paths.pure, where the disagreement is the point rather than
    # noise across sixteen classes.
    "string": ("none", "up", "low", "trim", "rev", "cat", "nest", "coal",
               "lpad", "repl", "split", "left"),
    "char": ("none", "up", "trim"),
    "int": ("none", "abs", "sign", "plus", "minus", "times", "mod", "band", "bxor"),
    "bigint": ("none", "plus", "abs", "shl"),
    "smallint": ("none", "abs", "sign"),
    "float": ("none", "abs", "sqrt", "times", "plus", "minus", "div", "log", "cbrt",
              "sin"),
    "decimal": ("none", "abs", "times", "todec"),
    # isNull/isNotNull are the only transforms whose RESULT type is fixed rather than
    # inherited, and Boolean is the one type where that is not a mismatch.
    # The boolean transforms read a STRING column and answer a Boolean, so they are the one
    # place a cell's source type differs from its own type -- see READS.
    "bool": ("none", "isnull", "isnotnull", "not", "starts", "ends", "has", "alnum"),
    # No transform over a date or a timestamp. The oracle implements no date function, and
    # adding one would be a bet about calendar semantics rather than a fact -- the same trap
    # concat set. What these cells test is the round trip and the RENDERING, which is where
    # F24 lives: a DateTime serializes with a UTC offset through one path and without it
    # through another.
    # No `previousDayOfWeek`/`mostRecentDayOfWeek`: both are in getSupportedFunctions() and
    # both fail at execution with "not supported yet" (F36). See probe_functions.py.
    # No `fdweek`: it returns a DateTime through a StrictDate property (F38). Covered by
    # 78-firstday-types.pure alongside the three siblings that behave.
    "date": ("none", "dpart", "fdmonth", "fdyear", "fdquarter"),
    "timestamp": ("none", "fhday", "fminhour", "fsecmin"),
}

# The source column type a transform READS, where it differs from the cell's own type. A
# boolean cell computed by `startsWith` reads a string; without this the generator would ask
# for a BIT column and hand it to a string function.
#
# `cbrt` reads CUBE: a DOUBLE column whose every value is a perfect cube (seed_cubes), so the
# true cube root is a double, which a C library's `cbrt` within an ulp returns. Over the ordinary
# float columns the oracle's correctly rounded root and the library's differed in the last
# bit on Windows -- cbrt(325.0) came back one double up (F60) -- and the stress corpus plan's
# D6 chose inputs with exact roots over per-platform verdicts. The cells still test `cbrt`,
# though not its last bit. One column per nullability: `cbrt` takes one argument.
CUBE = "cube"
READS = {"not": "bool", "isnull": "string", "isnotnull": "string",
         "starts": "string", "ends": "string", "has": "string", "alnum": "string",
         "cbrt": CUBE}

TYPE_SQL = {"string": "VARCHAR(200)", "char": "CHAR(12)", "int": "INTEGER",
            "bigint": "BIGINT", "smallint": "SMALLINT", "float": "DOUBLE",
            "decimal": "DECIMAL(18,4)", "bool": "BIT",
            "date": "DATE", "timestamp": "TIMESTAMP", CUBE: "DOUBLE"}
TYPE_PURE = {"string": "String", "char": "String", "int": "Integer",
             "bigint": "Integer", "smallint": "Integer", "float": "Float",
             "decimal": "Float", "bool": "Boolean",
             "date": "StrictDate", "timestamp": "DateTime"}

# Two columns of every (type, nullability) per table, because a two-argument transform whose
# arguments are the same column cannot detect an argument-order defect.
_PREFIX = {ROOT: "R", HOP1: "H1", HOP2: "H2", ALT: "A", "COMBO_SUMMARY": "SM"}
_SHORT = {"string": "S", "char": "C", "int": "I", "bigint": "BI",
          "smallint": "SI", "float": "F", "decimal": "DE",
          "bool": "B", "date": "DT", "timestamp": "TS", CUBE: "CU"}


def column(table: str, type_: str, nulls: str, which: int) -> str:
    return f"{_PREFIX[table]}_{_SHORT[type_]}_{'NN' if nulls == 'notnull' else 'NL'}{which}"


def _cube_tables() -> list[str]:
    """The tables a `cbrt` cell reads, in reach order: the ones that carry CUBE columns."""
    return list(dict.fromkeys(_TABLE_FOR[axis(c, "reach")] for c in cells()
                              if axis(c, "xform") == "cbrt"))


# The join FORMS, as one-hop reaches onto COMBO_ALT. Crossed with type and nullability but
# not with the transform and host axes: the point of these cells is that each join form
# EXECUTES and yields the right row, and multiplying them by every transform would triple the
# matrix to assert the same thing about the join five more times.
JOIN_REACH = {"multicol": J_MULTI, "ineq": J_INEQ, "ormix": J_OR,
              "dyna1": J_DYNA1, "dyna2": J_DYNA2}

_TABLE_FOR = {"col": ROOT, "chain1": HOP1, "chain2": HOP2,
              **{r: ALT for r in JOIN_REACH}}
_CHAIN_FOR = {"col": [], "chain1": [J1], "chain2": [J1, J2],
              **{r: [j] for r, j in JOIN_REACH.items()}}
AXES = ("reach", "type", "xform", "nulls", "host")


def cells() -> list[tuple[str, str, str, str, str]]:
    """The full property cross, in a fixed order so names are stable across runs.

    Not a rectangle: the transform axis depends on the type axis, so the cross is over the
    per-type transform lists rather than one shared list."""
    full = [(r, ty, x, n, h)
            for r in REACH for ty in TYPES for x in XFORM[ty]
            for n in NULLS for h in HOST]
    # The join forms get a narrower cross -- see JOIN_REACH. Two transforms rather than all
    # of them: `none` shows the join lands on the right row, and one transform shows a
    # dynafunction composes over it.
    forms = [(r, ty, x, n, "top")
             for r in sorted(JOIN_REACH) for ty in TYPES
             for x in XFORM[ty][:2] for n in NULLS]
    return full + forms


def class_cells() -> list[tuple[str, str, str]]:
    return [(f, e, w) for f in FILTERS for e in EXTENDS for w in WRITTEN]


def prop_name(cell) -> str:
    r, ty, x, n, h = cell
    return f"{r}_{ty}_{x}_{n}_{h}"


def pure_type(cell) -> str:
    return TYPE_PURE[axis(cell, "type")]


def axis(cell, name: str):
    """One axis value of a cell, BY NAME.

    Positional indexing is what broke when the type axis was inserted: `cell[3]` had meant
    `host` and silently became `nulls`, so every class mapping came out with no properties
    at all -- a generator that emitted a well-formed, entirely empty matrix.
    """
    return cell[AXES.index(name)]


def _ref(reach: str, col: str) -> str:
    """The value expression for one column at one reach."""
    table = _TABLE_FOR[reach]
    joins = _CHAIN_FOR[reach]
    if not joins:
        return f"[{DB}]{table}.{col}"
    chain = " > ".join(f"@{j}" for j in joins)
    return f"[{DB}]{chain} | [{DB}]{table}.{col}"


_UNARY = {"up": "toUpper", "low": "toLower", "trim": "trim", "rev": "reverseString",
          "abs": "abs", "sign": "sign", "sqrt": "sqrt", "log": "log", "cbrt": "cbrt",
          "sin": "sin", "todec": "toDecimal", "not": "not", "alnum": "isAlphaNumeric",
          "isnull": "isNull", "isnotnull": "isNotNull",
          "dpart": "datePart", "fdmonth": "firstDayOfMonth", "fdyear": "firstDayOfYear",
          "fdquarter": "firstDayOfQuarter", "fdweek": "firstDayOfWeek",
          "fhday": "firstHourOfDay", "fminhour": "firstMinuteOfHour",
          "fsecmin": "firstSecondOfMinute"}
_BINARY = {"cat": "concat", "plus": "plus", "times": "times", "minus": "minus",
           "div": "divide", "mod": "mod", "band": "bitAnd", "bxor": "bitXor",
           "coal": "coalesce", "starts": "startsWith",
           }
# Transforms whose second argument is a LITERAL rather than a second column.
_LITERAL_ARG = {"sub": ("substring", "2, 4"), "lpad": ("lpad", "12, 'x'"),
                "repl": ("replace", "'A', 'z'"), "split": ("splitPart", "'-', 1"),
                "left": ("left", "3"), "shl": ("bitShiftLeft", "1"),
                # LITERAL patterns. With a COLUMN as the second argument these three are
                # false for every row (F39), so a matrix cell using the column form would
                # assert a defect across sixteen classes instead of testing the function.
                "starts": ("startsWith", "'A'"), "ends": ("endsWith", "'1'"),
                "has": ("contains", "'-'")}


def expression(cell, bare: bool = False) -> str:
    """The right-hand side of the property mapping for one cell.

    `bare` writes the columns without their `[db]TABLE.` prefix, which is the form a
    `scope([db]TABLE)` block requires -- the scope states the table once for everything
    inside it.
    """
    reach, type_, xform, nulls, _host = cell
    table = _TABLE_FOR[reach]
    src = READS.get(xform, type_)
    if bare:
        ra, rb = (column(table, src, nulls, 1), column(table, src, nulls, 2))
    else:
        ra = _ref(reach, column(table, src, nulls, 1))
        rb = _ref(reach, column(table, src, nulls, 2))
    if xform in _LITERAL_ARG:
        fn, lit = _LITERAL_ARG[xform]
        return f"{fn}({ra}, {lit})"
    if xform == "none":
        return ra
    if xform in _UNARY:
        return f"{_UNARY[xform]}({ra})"
    if xform in _BINARY:
        return f"{_BINARY[xform]}({ra}, {rb})"
    if xform == "nest":
        # A function OVER a function. Neither regex the reader used before could span this,
        # which is why it is in the matrix rather than assumed to work.
        return f"toUpper(concat({ra}, {rb}))"
    raise ValueError(xform)


# ---------------------------------------------------------------- source generation
def _class_for(idx: int) -> str:
    return f"combo::C{idx}"


def _pairs(cell, ccell) -> set:
    """Every (property-axis value, class-axis value) this placement would witness."""
    cnames = ("filter", "extends", "written")
    return {((a, v), (b, w))
            for a, v in zip(AXES, cell) for b, w in zip(cnames, ccell)}


def assignment() -> dict[int, list[tuple]]:
    """Property cells dealt across the class cells to MAXIMISE pair coverage.

    Round-robin was the obvious choice and it was wrong. `host` is the innermost axis, so
    it has period 2; with six classes, cell k always lands on a class of the same parity as
    k, and every `embedded` cell landed on an `extends=yes` class while every `top` cell
    landed on an `extends=no` one. The matrix would have reported 48 combinations while
    two of its ten cross-axis pairs could never occur -- a coverage hole manufactured by
    the dealing scheme rather than by the features.

    So each cell goes to the class that witnesses the most pairs not yet witnessed, subject
    to an even split. check() then asserts that every pair is covered, because a dealing
    rule that silently misses one is exactly what this replaces.
    """
    ccells = class_cells()
    cap = -(-len(cells()) // len(ccells))        # ceiling, so the split stays even
    out: dict[int, list[tuple]] = {i: [] for i in range(len(ccells))}
    remaining = list(cells())
    covered: set = set()

    # PASS 1 -- coverage first, capacity second. A plain greedy is order-dependent: whatever
    # it meets last competes for whatever capacity is left, and two different orderings each
    # left a different pair uncovered (embedded/extends, then chain2/filter, then
    # nest/filter). Chasing that with a better ordering is guessing. Instead each REQUIRED
    # pair is placed deliberately, and only then is the remainder dealt.
    for pair in sorted(_required(), key=str):
        if pair in covered:
            continue
        (paxis, pval), (caxis, cval) = pair
        pidx = AXES.index(paxis)
        cidx = ("filter", "extends", "written").index(caxis)
        for cell in remaining:
            if cell[pidx] != pval:
                continue
            slot = next((i for i, cc in enumerate(ccells)
                         if cc[cidx] == cval and len(out[i]) < cap), None)
            if slot is None:
                continue
            out[slot].append(cell)
            covered |= _pairs(cell, ccells[slot])
            remaining.remove(cell)
            break

    # PASS 2 -- the rest go wherever there is room, emptiest first, so the split stays even.
    for cell in list(remaining):
        slot = min((i for i in range(len(ccells)) if len(out[i]) < cap),
                   key=lambda i: (len(out[i]), i))
        out[slot].append(cell)
        covered |= _pairs(cell, ccells[slot])
    return out


def _required() -> set:
    values = {"reach": REACH, "type": TYPES, "nulls": NULLS, "host": HOST,
              "xform": tuple(sorted({x for xs in XFORM.values() for x in xs}))}
    return {((a, v), (b, w))
            for a, vs in values.items() for v in vs
            for b, ws in (("filter", FILTERS), ("extends", EXTENDS),
                          ("written", WRITTEN)) for w in ws}


def check_data(c: model.Corpus, tables: dict[str, list[dict]]) -> list[str]:
    """The matrix must be non-vacuous ON THE SEED, not merely well-shaped.

    Three ways a combination test can be green while testing nothing, all of which this
    corpus has actually shipped at least once:

      * a class filter that excludes no row -- indistinguishable from no filter;
      * a class filter that excludes EVERY row -- the service returns nothing and every
        cell on it is asserted vacuously;
      * a `nullable` cell whose source column carries no NULL, so the transform under test
        never meets the case its null rule exists for.

    Checked here rather than trusted, because each was found only after the fact.
    """
    import oracle

    bad = []
    for i, (filt, _ext, _w) in enumerate(class_cells()):
        cls = _class_for(i)
        if cls not in c.main_table:
            continue
        rows = tables.get(c.main_table[cls], [])
        kept = oracle._mapping_filtered(c, cls, list(rows), tables)
        if filt != "none" and len(kept) == len(rows):
            bad.append(f"{cls}: filter={filt} excludes no row of {len(rows)} -- "
                       f"indistinguishable from no filter")
        if not kept:
            bad.append(f"{cls}: filter={filt} excludes EVERY row; its cells assert nothing")
    # Every nullable source column must actually carry a NULL somewhere.
    for table in (ROOT, HOP1, HOP2, ALT):
        for type_ in TYPES:
            for which in (1, 2):
                col = column(table, type_, "nullable", which)
                vals = [r.get(col) for r in tables.get(table, [])]
                if vals and not any(v is None for v in vals):
                    bad.append(f"{table}.{col} is a `nullable` source but carries no NULL, "
                               f"so every cell reading it tests the non-null case twice")
    for table in _cube_tables():
        for nulls in NULLS:
            col = column(table, CUBE, nulls, 1)
            vals = [r.get(col) for r in tables.get(table, [])]
            if nulls == "nullable" and vals and not any(v is None for v in vals):
                bad.append(f"{table}.{col} is a `nullable` source but carries no NULL")
            # seed_cubes, not the expansion's generic floats, must have written these
            bad += [f"{table}.{col}: {v!r} is not a perfect cube, so its cbrt is platform-dependent (F60)"
                    for v in vals if v is not None and round(v ** (1 / 3)) ** 3 != v]
    return bad


def check() -> list[str]:
    """Every (property-axis value, class-axis value) pair must actually co-occur."""
    ccells = class_cells()
    deal = assignment()
    covered = {p for i, cs in deal.items() for c in cs for p in _pairs(c, ccells[i])}
    want = _required()
    return [f"pair never co-occurs: {p[0][0]}={p[0][1]} with {p[1][0]}={p[1][1]}"
            for p in sorted(want - covered, key=str)]


def build_source() -> str:
    """The .pure text: store, classes, and the mapping carrying every combination."""
    ccells = class_cells()
    deal = assignment()
    L = [
        "// GENERATED by scripts/corpus/combos.py -- do not edit by hand.",
        "//",
        "// The COMBINATION matrix. Every cell of the property cross",
        "//     reach {col, chain1, chain2} x type {string, int, float, bool}",
        "//     x xform (per type) x nulls {notnull, nullable} x host {top, embedded}",
        "// dealt across the class cross",
        "//     filter {none, direct, chain} x extends {no, yes}",
        "//",
        "// 180 property cells over 6 class mappings. The corpus previously had two feature",
        "// PAIRS in total; this crosses them deliberately, because every defect it has",
        "// found so far lived where one feature met another rather than in a feature",
        "// alone.",
        "###Pure",
    ]

    # ---- classes ----------------------------------------------------------------
    # A shared supertype so `extends [setId]` has a base set to extend, and an embedded
    # child so the `host: embedded` cells have somewhere to live.
    L += [
        "Class combo::Base",
        "{",
        "   rootId: String[1];",
        "}",
        "",
    ]
    for i, (filt, ext, written) in enumerate(ccells):
        cls = _class_for(i)
        tops = [c for c in deal[i] if axis(c, "host") == "top"]
        embs = [c for c in deal[i] if axis(c, "host") == "embedded"]
        head = (f"Class {cls} extends combo::Base" if ext == "yes" else f"Class {cls}")
        L.append(f"// filter={filt}  extends={ext}")
        L.append(head)
        L.append("{")
        if ext == "no":
            L.append("   rootId: String[1];")
        for cell in tops:
            L.append(f"   {prop_name(cell)}: {pure_type(cell)}[0..1];")
        if embs:
            L.append(f"   nested: combo::E{i}[0..1];")
        L.append("}")
        L.append("")
        if embs:
            L.append(f"Class combo::E{i}")
            L.append("{")
            for cell in embs:
                L.append(f"   {prop_name(cell)}: {pure_type(cell)}[0..1];")
            L.append("}")
            L.append("")

    # ---- store ------------------------------------------------------------------
    def cols(table: str, last: bool = False) -> list[str]:
        """Column lines for `table`: two of every (type, nullability).

        `last=True` drops the trailing comma -- a Table's final columnDefinition must not
        carry one, and the parser reports that deep inside relationalIdentifier rather than
        at the comma, so it reads as a bad column name."""
        out = []
        for type_ in TYPES:
            for nulls in NULLS:
                for which in (1, 2):
                    sfx = "" if nulls == "nullable" else " NOT NULL"
                    out.append(f"      {column(table, type_, nulls, which)} "
                               f"{TYPE_SQL[type_]}{sfx},")
        # After every typed column, so the expansion's NULLs (placed by column order) stay
        # where they were.
        if table in _cube_tables():
            out += [f"      {column(table, CUBE, nulls, 1)} {TYPE_SQL[CUBE]}"
                    f"{'' if nulls == 'nullable' else ' NOT NULL'}," for nulls in NULLS]
        if table in (ROOT, HOP1):
            out += [f"      {c} VARCHAR(200) NOT NULL," for c in OTW_COLS]
        if last:
            out[-1] = out[-1].rstrip(",")
        return out

    L += [
        f"// Carries an embedded property with an `Otherwise` fallback. Its inline columns",
        f"// are NOT NULL for every row, so the inline branch always applies and the",
        f"// fallback is never reached -- which is the half F13 does not break.",
        f"Class {OTW_CLASS}",
        "{",
        "   rootId: String[1];",
        f"   inline: {OTW_TARGET}[0..1];",
        "}",
        "",
        f"Class {OTW_TARGET}",
        "{",
        "   code: String[1];",
        "   label: String[0..1];",
        "}",
        "",
        "// Mapped through a SCOPE block: the property mappings inside name their columns",
        "// BARE, with the table stated once by the scope. 60 class mappings in this corpus",
        "// are written that way and none was executed -- they live in a Mapping that no",
        "// runtime binds, so nothing could resolve one.",
        f"Class {SCOPE_CLASS}",
        "{",
        "   rootId: String[1];",
        *[f"   {column(ROOT, ty, nl, 1).lower()}: {TYPE_PURE[ty]}[0..1];"
          for ty in TYPES for nl in NULLS],
        "}",
        "",
        "// Reached from a root class by an association whose ends name BOTH set ids.",
        f"Class {HOP_CLASS}",
        "{",
        "   hopCode: String[1];",
        *[f"   {column(HOP1, ty, nl, 1).lower()}: {TYPE_PURE[ty]}[0..1];"
          for ty in TYPES for nl in NULLS],
        "}",
        "",
        f"Association {ASSOC}",
        "{",
        f"   hop: {HOP_CLASS}[0..1];",
        f"   roots: {_class_for(0)}[*];",
        "}",
        "",
        "// Reaches the SCHEMA-qualified table by navigation rather than only by being rooted",
        "// at it, which is what lets `Schema` combine with anything else at all.",
        f"Association {SCHEMA_ASSOC}",
        "{",
        f"   summary: {SCHEMA_CLASS}[0..1];",
        f"   summarised: {_class_for(0)}[*];",
        "}",
        "",
        f"// Mapped over a table inside the `{SCHEMA}` Schema.",
        f"Class {SCHEMA_CLASS}",
        "{",
        "   summaryId: String[1];",
        "   rootId: String[0..1];",
        *[f"   {column(SCHEMA_TABLE, ty, nl, 1).lower()}: {TYPE_PURE[ty]}[0..1];"
          for ty in TYPES for nl in NULLS],
        "}",
        "",
        "###Relational",
        f"Database {DB}",
        "(",
        f"   Table {ROOT}",
        "   (",
        "      ROOT_ID VARCHAR(64) PRIMARY KEY,",
        *cols(ROOT),
        "      HOP1_CODE VARCHAR(64)",
        "   )",
        f"   Table {HOP1}",
        "   (",
        "      HOP1_CODE VARCHAR(64) PRIMARY KEY,",
        *cols(HOP1),
        "      HOP2_CODE VARCHAR(64)",
        "   )",
        f"   Table {HOP2}",
        "   (",
        "      HOP2_CODE VARCHAR(64) PRIMARY KEY,",
        *cols(HOP2, last=True),
        "   )",
        "",
        "   // Reached by the JOIN FORMS below rather than by a foreign key. Its rows are",
        "   // derived from COMBO_ROOT's so each form pairs deterministically and to-one --",
        "   // see combos.derive_alt. The four derivation columns exist only to be the far",
        "   // side of one form each.",
        f"   Table {ALT}",
        "   (",
        "      ALT_CODE VARCHAR(64) PRIMARY KEY,",
        f"      {ALT_KEY} VARCHAR(200),",
        f"      {ALT_LOW} INTEGER,",
        f"      {ALT_CAT} VARCHAR(400),",
        f"      {ALT_UP} VARCHAR(200),",
        *cols(ALT, last=True),
        "   )",
        "",
        "   // A SCHEMA. Every other table in this corpus sits in the default schema, so a",
        "   // schema-qualified reference had never been resolved by anything -- not by a",
        "   // mapping, which writes `[db]schema.TABLE.COL`, and not by a ###Data element,",
        "   // which keys its rows `schema.TABLE`. Both were probed against the engine",
        "   // first; a Schema that only ever appears in a Database block is a declaration,",
        "   // not a tested feature.",
        f"   Schema {SCHEMA}",
        "   (",
        f"      Table {SCHEMA_TABLE}",
        "      (",
        "         SUMMARY_ID VARCHAR(64) PRIMARY KEY,",
        "         // A real FOREIGN KEY, so the schema-qualified table can be REACHED.",
        "         // Without one it is an island: nothing navigates to it, and `Schema`",
        "         // appears only in the single service rooted at it. The first attempt",
        "         // joined on the numeric suffix of the two ids instead --",
        "         // right(ROOT_ID,4) = right(SUMMARY_ID,4) -- which is correct, needs no",
        "         // schema change, and cannot use an index: a dynafunction on both sides",
        "         // of a join is a cross-product, and it took the suite from thirteen",
        "         // minutes to over fifty. A column is cheaper than cleverness.",
        "         ROOT_ID VARCHAR(64),",
        *[f"   {line}" for line in cols(SCHEMA_TABLE, last=True)],
        "      )",
        "   )",
        "",
        f"   Join {J_SUMMARY}({ROOT}.ROOT_ID = {SCHEMA}.{SCHEMA_TABLE}.ROOT_ID)",
        f"   Join {J1}({ROOT}.HOP1_CODE = {HOP1}.HOP1_CODE)",
        f"   Join {J2}({HOP1}.HOP2_CODE = {HOP2}.HOP2_CODE)",
        "",
        "   // MULTI-COLUMN: two equalities joined by `and`.",
        f"   Join {J_MULTI}({ROOT}.HOP1_CODE = {ALT}.ALT_CODE and "
        f"{ROOT}.{column(ROOT, 'string', 'notnull', 1)} = {ALT}.{ALT_KEY})",
        "   // NON-EQUALITY. A_LOW is seeded so exactly one ALT row sits below every root",
        "   // value and the rest sit above, which keeps the navigation to-one; the root",
        "   // column is NULLABLE, so the row where it is NULL matches nothing and the",
        "   // no-match case is exercised too.",
        f"   Join {J_INEQ}({ROOT}.{column(ROOT, 'int', 'nullable', 1)} > {ALT}.{ALT_LOW})",
        "   // `or`, with BOTH branches reachable: the equality matches for a root row whose",
        "   // key resolves, and the null branch matches the sentinel row for the root whose",
        "   // key is absent. Parenthesised because and/or are equal-precedence and",
        "   // right-associative, so `A and B or C` would parse as `A and (B or C)`.",
        f"   Join {J_OR}(({ROOT}.HOP1_CODE = {ALT}.ALT_CODE) or "
        f"({ROOT}.HOP1_CODE is null and {ALT}.ALT_CODE = '{SENTINEL}'))",
        "   // DYNAFUNCTION on one side, and on both.",
        f"   Join {J_DYNA1}(concat({ROOT}.{column(ROOT, 'string', 'notnull', 1)}, "
        f"{ROOT}.{column(ROOT, 'string', 'notnull', 2)}) = {ALT}.{ALT_CAT})",
        f"   Join {J_DYNA2}(toUpper({ROOT}.{column(ROOT, 'string', 'notnull', 1)}) = "
        f"toUpper({ALT}.{ALT_UP}))",
        "",
        "   // The DIRECT filter. It tests the foreign key rather than one of the columns",
        "   // the matrix cells read, for two reasons: the seeder GUARANTEES an absent key",
        "   // (property A2) so the filter provably excludes a row, and filtering on a cell's",
        "   // own source column would remove the very NULL that the `nullable` cells on this",
        "   // class exist to meet -- making that pair vacuous while looking covered.",
        "   //",
        "   // The first version tested `R_MEMO is not null` and excluded NOTHING, because the",
        "   // seeder nulls only the FIRST nullable column of a table. A filter matching every",
        "   // row is indistinguishable from no filter at all.",
        "   Filter ComboLinked(COMBO_ROOT.HOP1_CODE is not null)",
        "   // The chain filter's predicate, on the far end of two joins. It names a NOT",
        "   // NULL column deliberately: what this cell tests is that the FILTER FOLLOWS THE",
        "   // CHAIN, so the rows it must exclude are the ones whose chain breaks -- an",
        "   // absent key, a dangling key -- not the ones failing a predicate. Pointing it",
        "   // at a nullable column instead excluded every row, which the vacuity guard",
        "   // caught: a filter matching nothing asserts as little as one matching",
        "   // everything.",
        f"   Filter ComboNamedHop2({HOP2}.{column(HOP2, 'string', 'notnull', 1)} is not null)",
        "   // A MultiGrainFilter. A mapping references it exactly as it references a Filter",
        "   // -- the distinction is consumed by the planner, for join elision -- so nothing",
        "   // about the reference tells you which kind it is. It is in the matrix because a",
        "   // construct that can only be DECLARED is a construct nobody has run: the",
        "   // generated dense store declared one and referenced it nowhere.",
        f"   MultiGrainFilter ComboGrain({ROOT}.{column(ROOT, 'string', 'nullable', 1)} "
        f"is not null)",
        ")",
        "",
    ]

    # ---- mapping ----------------------------------------------------------------
    L += ["###Mapping", f"Mapping {MAPPING}", "("]
    # The root set every `extends` set extends. It maps only the inherited identity, so an
    # extending set below contributes nothing but its own combination cells.
    L += [
        "   // The ROOT set. A class mapped by more than one set implementation needs exactly",
        "   // one root, and an extending set carries no ~mainTable of its own.",
        "   *combo::Base[base]: Relational",
        "   {",
        f"      ~primaryKey ( [{DB}]{ROOT}.ROOT_ID )",
        f"      ~mainTable [{DB}]{ROOT}",
        f"      rootId: [{DB}]{ROOT}.ROOT_ID",
        "   }",
        "",
    ]
    for i, (filt, ext, written) in enumerate(ccells):
        cls = _class_for(i)
        tops = [c for c in deal[i] if axis(c, "host") == "top"]
        embs = [c for c in deal[i] if axis(c, "host") == "embedded"]
        L.append(f"   // filter={filt}  extends={ext}  "
                 f"({len(tops)} top-level, {len(embs)} embedded)")
        if ext == "yes":
            L.append(f"   {cls}[c{i}] extends [base]: Relational")
        else:
            L.append(f"   {cls}[c{i}]: Relational")
        L.append("   {")
        # Directive order is fixed: ~filter first, and an extending set has no ~mainTable.
        if filt == "direct":
            L.append(f"      ~filter [{DB}]ComboLinked")
        elif filt == "multigrain":
            L.append(f"      ~filter [{DB}]ComboGrain")
        elif filt == "chain":
            L.append(f"      ~filter [{DB}]@{J1} > @{J2} | [{DB}]ComboNamedHop2")
        if ext == "no":
            L.append(f"      ~primaryKey ( [{DB}]{ROOT}.ROOT_ID )")
            L.append(f"      ~mainTable [{DB}]{ROOT}")
        body = []
        if ext == "no":
            body.append(f"      rootId: [{DB}]{ROOT}.ROOT_ID")
        # In `scope` form the cells reading COMBO_ROOT move inside a scope block and lose
        # their `[db]TABLE.` prefix. Only those: a chain names its own tables and cannot be
        # covered by a scope over the root, so mixing the two in one mapping is the point --
        # it is what a real mapping written this way looks like.
        scoped = [x for x in tops if written == "scope" and _TABLE_FOR[axis(x, "reach")] == ROOT]
        if scoped:
            inner = ",\n".join(f"         {prop_name(x)}: {expression(x, bare=True)}"
                               for x in scoped)
            body.append(f"      scope([{DB}]{ROOT})\n      (\n{inner}\n      )")
        for cell in [x for x in tops if x not in scoped]:
            body.append(f"      {prop_name(cell)}: {expression(cell)}")
        if embs:
            inner = ",\n".join(f"         {prop_name(cell)}: {expression(cell)}"
                               for cell in embs)
            body.append("      nested\n      (\n" + inner + "\n      )")
        L.append(",\n".join(body))
        L.append("   }")
        L.append("")
    L += [
        "   // OTHERWISE. The embedded block is the inline branch; the set id after",
        "   // `Otherwise` names the fallback, reached by a join when the inline branch does",
        "   // not apply. Both columns below are NOT NULL, so it never does -- deliberately,",
        "   // because F13 makes the fallback unreachable under TDS projection and a service",
        "   // that needed it could only ever be quarantined.",
        f"   *{OTW_TARGET}[otwFallback]: Relational",
        "   {",
        f"      ~primaryKey ( [{DB}]{HOP1}.HOP1_CODE )",
        f"      ~mainTable [{DB}]{HOP1}",
        f"      code: [{DB}]{HOP1}.{OTW_COLS[0]},",
        f"      label: [{DB}]{HOP1}.{OTW_COLS[1]}",
        "   }",
        "",
        f"   {OTW_CLASS}: Relational",
        "   {",
        f"      ~primaryKey ( [{DB}]{ROOT}.ROOT_ID )",
        f"      ~mainTable [{DB}]{ROOT}",
        f"      rootId: [{DB}]{ROOT}.ROOT_ID,",
        "      inline",
        "      (",
        f"         code: [{DB}]{ROOT}.{OTW_COLS[0]},",
        f"         label: [{DB}]{ROOT}.{OTW_COLS[1]}",
        f"      ) Otherwise ([otwFallback]: [{DB}]@{J1})",
        "   }",
        "",
        "   // SCOPE. `scope([db]TABLE)` states the table once; the property mappings inside",
        "   // name bare columns. A second form, `scope([db])`, qualifies each column with",
        "   // its table instead -- this uses the first.",
        f"   {SCOPE_CLASS}: Relational",
        "   {",
        f"      ~primaryKey ( [{DB}]{ROOT}.ROOT_ID )",
        f"      ~mainTable [{DB}]{ROOT}",
        f"      scope([{DB}]{ROOT})",
        "      (",
        ",\n".join(
            ["         rootId: ROOT_ID"]
            + [f"         {column(ROOT, ty, nl, 1).lower()}: {column(ROOT, ty, nl, 1)}"
               for ty in TYPES for nl in NULLS]),
        "      )",
        "   }",
        "",
        f"   {HOP_CLASS}[hop1]: Relational",
        "   {",
        f"      ~primaryKey ( [{DB}]{HOP1}.HOP1_CODE )",
        f"      ~mainTable [{DB}]{HOP1}",
        ",\n".join(
            [f"      hopCode: [{DB}]{HOP1}.HOP1_CODE"]
            + [f"      {column(HOP1, ty, nl, 1).lower()}: [{DB}]{HOP1}.{column(HOP1, ty, nl, 1)}"
               for ty in TYPES for nl in NULLS]),
        "   }",
        "",
        "   // ASSOCIATION ENDS WITH EXPLICIT SOURCE AND TARGET SET IDS. The ids are load-",
        "   // bearing rather than ceremony here: C0 is one of eight sets over COMBO_ROOT, so",
        "   // an unqualified end would be ambiguous about which it connects.",
        f"   {ASSOC}: Relational",
        "   {",
        "      AssociationMapping",
        "      (",
        f"         hop[c0, hop1]: [{DB}]@{J1},",
        f"         roots[hop1, c0]: [{DB}]@{J1}",
        "      )",
        "   }",
        "",
        f"   {SCHEMA_ASSOC}: Relational",
        "   {",
        "      AssociationMapping",
        "      (",
        f"         summary[c0, combo_Summary]: [{DB}]@{J_SUMMARY},",
        f"         summarised[combo_Summary, c0]: [{DB}]@{J_SUMMARY}",
        "      )",
        "   }",
        "",
        f"   // The SCHEMA-QUALIFIED mapping: every reference names {SCHEMA}.{SCHEMA_TABLE}.",
        f"   {SCHEMA_CLASS}: Relational",
        "   {",
        f"      ~primaryKey ( [{DB}]{SCHEMA}.{SCHEMA_TABLE}.SUMMARY_ID )",
        f"      ~mainTable [{DB}]{SCHEMA}.{SCHEMA_TABLE}",
        ",\n".join(
            [f"      summaryId: [{DB}]{SCHEMA}.{SCHEMA_TABLE}.SUMMARY_ID",
             f"      rootId: [{DB}]{SCHEMA}.{SCHEMA_TABLE}.ROOT_ID"]
            + [f"      {column(SCHEMA_TABLE, ty, nl, 1).lower()}: "
               f"[{DB}]{SCHEMA}.{SCHEMA_TABLE}.{column(SCHEMA_TABLE, ty, nl, 1)}"
               for ty in TYPES for nl in NULLS]),
        "   }",
        "",
    ]
    L.append(")")
    L.append("")
    return "\n".join(L)


# ---------------------------------------------------------------- services
def specs(c: model.Corpus) -> list[Spec]:
    """One service per class cell, projecting every combination cell it carries.

    A cell whose value the oracle cannot compute is DROPPED from the projection rather than
    silently asserted, and reported by unsupported(). Grouping a class's cells into one
    service keeps the count readable; the cost is that one divergent cell fails the whole
    service, which is why the known divergences get their own below.
    """
    import oracle
    import flat

    tables = flat.all_tables(c)
    out = []
    for i, (filt, ext, written) in enumerate(class_cells()):
        cls = _class_for(i)
        if cls not in c.main_table:
            continue
        spec = Spec(f"stress::CB_{cls.split('::')[-1]}", f"/stress/cb_c{i}",
                    f"Combination matrix cell: class filter={filt}, extends={ext}. "
                    f"Projects every property cell dealt to this class -- each a distinct "
                    f"(reach, transform, nullability, host) combination. Generated by "
                    f"scripts/corpus/combos.py.", cls)
        projs = [Proj("rootId", ["rootId"])]
        for cell in assignment()[i]:
            path = (["nested", prop_name(cell)] if axis(cell, "host") == "embedded"
                    else [prop_name(cell)])
            projs.append(Proj(prop_name(cell), path))
        spec.projections = projs
        spec.sort = ("rootId", False)
        spec.mapping, spec.runtime = MAPPING, RUNTIME
        spec.connection, spec.data_element = CONN_ID, DATA
        out.append(spec)
    extra = predicate_specs(c)
    for maker in (schema_spec, schema_reach_spec, scope_spec, assoc_ids_spec,
                  otherwise_spec):
        s = maker(c)
        if s is not None:
            extra.append(s)
    return out + extra


def otherwise_spec(c: model.Corpus):
    """A service over the embedded property that carries an `Otherwise` fallback.

    Its inline columns are NOT NULL for every row, so the inline branch always applies and
    the fallback is never reached. That is deliberate: F13 makes an Otherwise never fall
    back under TDS projection, so a service that NEEDED the fallback could only ever be
    quarantined -- and the construct then has no demonstration that it works at all.

    The expectation is the inline columns' own values, which is what makes the split honest:
    if the engine took the fallback anyway it would return the joined table's values and
    this would fail rather than quietly agree.
    """
    if (OTW_CLASS, "inline") not in c.embedded:
        return None
    spec = Spec("stress::CB_Otherwise", "/stress/cb_otherwise",
                f"Reads the embedded property on {OTW_CLASS}, mapped with an `Otherwise` "
                f"fallback that never applies because the inline columns are NOT NULL. The "
                f"fallback half is pinned by O1_CounterpartyOtherwise under F13. Generated "
                f"by scripts/corpus/combos.py.", OTW_CLASS)
    spec.projections = [Proj("rootId", ["rootId"]),
                        Proj("code", ["inline", "code"]),
                        Proj("label", ["inline", "label"])]
    spec.sort = ("rootId", False)
    spec.mapping, spec.runtime = MAPPING, RUNTIME
    spec.connection, spec.data_element = CONN_ID, DATA
    return spec


def scope_spec(c: model.Corpus):
    """A service over the class mapped through a SCOPE block.

    The corpus has 60 scope-using class mappings and executed none of them: they live in
    stress::DenseMapping, which no runtime binds, so nothing could resolve one. A construct
    reachable only from a mapping nobody runs is a construct nobody has tested.
    """
    if SCOPE_CLASS not in c.main_table:
        return None
    spec = Spec("stress::CB_Scoped", "/stress/cb_scoped",
                f"Reads {SCOPE_CLASS}, whose property mappings sit inside `scope([db]TABLE)` "
                f"and name their columns BARE. Generated by scripts/corpus/combos.py.",
                SCOPE_CLASS)
    spec.projections = [Proj(p, [p]) for p in sorted(c.columns.get(SCOPE_CLASS, {}))]
    spec.sort = ("rootId", False)
    spec.mapping, spec.runtime = MAPPING, RUNTIME
    spec.connection, spec.data_element = CONN_ID, DATA
    return spec


def assoc_ids_spec(c: model.Corpus):
    """A service navigating the association whose ends name both set ids.

    The only other association written that way spans two stores, so navigating it hits F26
    and cannot execute at all -- and its ends were not even bound, because binding matched
    on the association's NAME and that one is not named `A_B`.
    """
    cls = _class_for(0)
    if c.ends.get((cls, "hop")) is None or c.ends[(cls, "hop")].join is None:
        return None
    leaf = column(HOP1, "string", "notnull", 1).lower()
    spec = Spec("stress::CB_AssocSetIds", "/stress/cb_assoc_setids",
                f"Navigates {ASSOC} from {cls}, whose AssociationMapping names an explicit "
                f"SOURCE and TARGET set id on each end. The ids are load-bearing: COMBO_ROOT "
                f"carries eight set implementations, so an unqualified end would be "
                f"ambiguous. Generated by scripts/corpus/combos.py.", cls)
    spec.projections = [Proj("rootId", ["rootId"]),
                        Proj("hopCode", ["hop", "hopCode"]),
                        Proj("hopLeaf", ["hop", leaf])]
    spec.sort = ("rootId", False)
    spec.mapping, spec.runtime = MAPPING, RUNTIME
    spec.connection, spec.data_element = CONN_ID, DATA
    return spec


def schema_reach_spec(c: model.Corpus):
    """Reaches the schema-qualified table BY NAVIGATION, from a matrix root.

    This was briefly filed as a defect in schema navigation (F47), which it was not. The
    failure was that the association ends did not NAME their set ids, and combo::C0 is one of
    eight sets over COMBO_ROOT; the schema was a coincidence of where I happened to be
    looking. See F47 for what the message actually means.

    schema_spec below is rooted AT the schema table, which proves a mapping can resolve
    `[db]schema.TABLE.COL` and nothing more -- one service, and `Schema` sat at three of its
    forty possible feature pairs because nothing else could reach it. This one starts at C0
    and navigates, so Schema arrives stacked on whatever C0 carries: the set ids, the scope
    block, the join forms.

    The FK it crosses is seeded by the expansion, which means it carries the corpus's
    adversarial shapes for free -- one summary row points at a root that does not exist and
    another points at nothing at all. So this is a navigation over a broken edge, not a tidy
    one.
    """
    cls = _class_for(0)
    if c.ends.get((cls, "summary")) is None or c.ends[(cls, "summary")].join is None:
        return None
    leaf = column(SCHEMA_TABLE, "string", "notnull", 1).lower()
    spec = Spec("stress::CB_SchemaReach", "/stress/cb_schema_reach",
                f"Navigates {SCHEMA_ASSOC} from {cls} to the schema-qualified "
                f"{SCHEMA}.{SCHEMA_TABLE}. Generated by scripts/corpus/combos.py.", cls)
    spec.projections = [Proj("rootId", ["rootId"]),
                        Proj("summaryId", ["summary", "summaryId"]),
                        Proj("summaryLeaf", ["summary", leaf])]
    spec.sort = ("rootId", False)
    spec.mapping, spec.runtime = MAPPING, RUNTIME
    spec.connection, spec.data_element = CONN_ID, DATA
    return spec


def schema_spec(c: model.Corpus):
    """A service over the SCHEMA-QUALIFIED table.

    Its own service rather than a cell of the matrix: what is under test is that a mapping
    can resolve `[db]schema.TABLE.COL` and that a ###Data element can seed `schema.TABLE`,
    neither of which any other service exercises, and both of which are properties of the
    table rather than of any transform applied to it.
    """
    if SCHEMA_CLASS not in c.main_table:
        return None
    cols = sorted(c.columns.get(SCHEMA_CLASS, {}))
    spec = Spec("stress::CB_SchemaQualified", "/stress/cb_schema",
                f"Reads {SCHEMA_CLASS}, whose every reference is schema-qualified. The "
                f"corpus declared a Schema and mapped over none, so a schema-qualified "
                f"reference had never been resolved by a mapping and a schema-qualified "
                f"###Data key had never seeded anything. Generated by "
                f"scripts/corpus/combos.py.", SCHEMA_CLASS)
    spec.projections = [Proj(p, [p]) for p in cols]
    spec.sort = ("summaryId", False)
    spec.mapping, spec.runtime = MAPPING, RUNTIME
    spec.connection, spec.data_element = CONN_ID, DATA
    return spec


def predicate_specs(c: model.Corpus) -> list[Spec]:
    """The three comparison operators against a NULL operand, side by side.

    Pure compares an absent value as false and `!=` as its negation, true (oracle._cmp), so
    `==` and `>` drop the row whose operand is NULL and `!=` keeps it. The trio was written
    to pin the opposite claim -- F28, that keeping it was an engine defect, since SQL's
    three-valued logic would drop it -- and the oracle then followed SQL. Both engines keep
    the row, as legend-pure defines; F28 is withdrawn (2026-10-23, the improvement plan's
    Phase 1) and the trio now pins Pure's rule. Kept as three because the pair that drops is
    what makes the one that keeps legible.

    Deliberately here rather than on the dense services, which used to carry it. Thirty of
    them diverged at once when the seeder began nulling every nullable column, coupling
    every deep-navigation test to one question. Same split aggregates.py makes around F6.
    """
    from query import Pred

    # SEARCHED, not assumed. The first version hardcoded class 0 and found nothing: the
    # dealing is a coverage-driven greedy, so which class carries a given cell is not
    # something a caller can predict -- and the services silently did not exist.
    #
    # A class with NO filter, so what the predicate excludes is not confounded with what a
    # class filter already excluded.
    want = ("string", "none", "nullable", "top")
    cls = prop = None
    for i, (filt, _ext, _w) in enumerate(class_cells()):
        if filt != "none":
            continue
        hit = next((x for x in assignment()[i]
                    if (axis(x, "type"), axis(x, "xform"),
                        axis(x, "nulls"), axis(x, "host")) == want), None)
        if hit is not None and _class_for(i) in c.main_table:
            cls, prop = _class_for(i), prop_name(hit)
            break
    if prop is None:
        raise SystemExit(
            "combination matrix carries no plain nullable string cell on an unfiltered "
            "class, so the NULL-comparison trio cannot be pinned -- it would vanish silently")
    out = []
    for name, op, value, note in (
            ("NotEqualsNull", "!=", " none",
             "The row whose operand is NULL is KEPT: Pure compares an absent value as "
             "false, so `!=`, its negation, is true. SQL three-valued logic would drop it."),
            ("EqualsNull", "==", " none",
             "The complement: an absent value is not equal, so the NULL row is dropped, as "
             "SQL would drop it too."),
            ("GreaterNull", ">", " ",
             "An ORDERED comparison against the same NULL: never satisfied by an absent "
             "value, so dropped -- while `!=` keeps it.")):
        spec = Spec(f"stress::CB_{name}", f"/stress/cb_{name.lower()}",
                    f"Comparison `{op}` against a column that is NULL in some rows. {note} "
                    f"Generated by scripts/corpus/combos.py.", cls)
        spec.projections = [Proj("rootId", ["rootId"]), Proj(prop, [prop])]
        spec.filters = [Pred([prop], op, value)]
        spec.sort = ("rootId", False)
        spec.mapping, spec.runtime = MAPPING, RUNTIME
        if name == "EqualsNull":
            # no row holds the comparand, and the absent one is not equal to it
            spec.emptiness = emptiness.EXCLUDES_ALL
        spec.connection, spec.data_element = CONN_ID, DATA
        out.append(spec)
    return out


def derive_alt(c: model.Corpus, tables: dict[str, list[dict]]) -> None:
    """Rewrite COMBO_ALT so each join FORM pairs deterministically with COMBO_ROOT.

    Generated independently, none of these conditions matches anything: a multi-column
    equality needs both columns to agree, and `concat(a, b) = x` needs a column literally
    holding that concatenation. The corpus's generated dense joins are in exactly that
    state -- `dense_DynaOneSide` and `dense_DynaBothSides` pair ZERO rows with zero, which
    is why declaring them was never the same as executing them.

    So the far side is derived from the near side, in the open:

        ALT_CODE   the root's own HOP1_CODE, so the equality and multi-column forms land
        A_KEY      the root's first NOT NULL string, the multi-column form's second term
        A_LOW      one row far below every root value and the rest far above, so the
                   non-equality form stays TO-ONE rather than fanning out
        A_CAT      concat of the root's two NOT NULL strings
        A_UP       the root's first NOT NULL string LOWERCASED, so `toUpper(x) = toUpper(y)`
                   matches while a plain equality would not -- otherwise the test would pass
                   whether or not the engine applied the function

    One extra SENTINEL row exists for the `or` form's second branch, so the root whose key
    is absent still lands somewhere and both branches of the disjunction are exercised.
    """
    root_rows = tables.get(ROOT) or []
    if not root_rows:
        return
    s1 = column(ROOT, "string", "notnull", 1)
    s2 = column(ROOT, "string", "notnull", 2)
    # The generated rows are KEPT and only the join keys are overwritten. Copying one
    # template row into all of them would give every ALT row identical typed values and no
    # NULLs, so every cell reading COMBO_ALT would return the same thing for every source
    # row -- five assertions of one fact, and the nullability axis silently dead on this
    # whole reach.
    generated = tables.get(ALT) or []
    template = generated[0] if generated else {}
    out = []
    for i, r in enumerate(root_rows):
        row = dict(generated[i] if i < len(generated) else template)
        row["ALT_CODE"] = r.get("HOP1_CODE") or f"ALT-ORPHAN-{i}"
        row[ALT_KEY] = r.get(s1)
        # Exactly one row below every root value; the others above, so `>` matches at most
        # one. Without that the navigation fans out and the oracle refuses it -- correctly,
        # since a to-one property cannot land on three rows.
        row[ALT_LOW] = -1 if i == 0 else 10 ** 6
        row[ALT_CAT] = f"{r.get(s1)}{r.get(s2)}"
        row[ALT_UP] = str(r.get(s1)).lower()
        out.append(row)
    sentinel = dict(out[0])
    sentinel["ALT_CODE"] = SENTINEL
    # Distinct on every join key, so it pairs ONLY through the `or` form's null branch.
    sentinel[ALT_KEY] = sentinel[ALT_CAT] = sentinel[ALT_UP] = None
    sentinel[ALT_LOW] = 10 ** 6
    out.append(sentinel)
    tables[ALT] = out


def seed_cubes(tables: dict[str, list[dict]]) -> None:
    """Overwrite the CUBE columns' generic floats with perfect cubes: 6^3 up on COMBO_ROOT,
    and on each later table five past the one before, so a reach that lands on the wrong
    table returns a different root. The expansion's NULL is kept."""
    for t, table in enumerate(_cube_tables()):
        for i, row in enumerate(tables.get(table) or []):
            for nulls in NULLS:
                col = column(table, CUBE, nulls, 1)
                if row.get(col) is not None:
                    row[col] = float((6 + 5 * t + i) ** 3)


def runtime_text() -> str:
    return f"""// GENERATED by scripts/corpus/combos.py -- do not edit by hand.
//
// Connection and runtime for the combination matrix. Its tables live in their own store,
// and test data is bound to a CONNECTION, so it needs its own.
###Connection
RelationalDatabaseConnection combo::ComboConn
{{
    type: DuckDB;
    specification: DuckDB {{ }};
    auth: Test;
}}


###Runtime
Runtime {RUNTIME}
{{
    mappings:
    [
        {MAPPING}
    ];
    connections:
    [
        {DB}: [ {CONN_ID}: combo::ComboConn ]
    ];
}}
"""


if __name__ == "__main__":
    c = model.load()
    deal = assignment()
    problems = check()
    print(f"property cells: {len(cells())}   class cells: {len(class_cells())}")
    print(f"pair coverage: {'COMPLETE' if not problems else problems}")
    for i, (f, e, w) in enumerate(class_cells()):
        got = deal[i]
        print(f"  C{i}  filter={f:<10} extends={e:<4} written={w:<6} {len(got)} cells "
              f"({sum(1 for g in got if axis(g, 'host') == 'embedded')} embedded)")
    print()
    seen = {ax: set() for ax in ("reach", "xform", "nulls", "host")}
    for r, x, n, h in cells():
        seen["reach"].add(r); seen["xform"].add(x)
        seen["nulls"].add(n); seen["host"].add(h)
    for k, v in seen.items():
        print(f"  {k:<7}{sorted(v)}")
