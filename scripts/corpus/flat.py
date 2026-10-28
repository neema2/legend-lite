"""
Derives TRADE_FLAT — the denormalized reporting table — from the normalized seed.

This file is short and the reason for it is the whole point of L2.

Mapping invariance says: the same query, asked through two semantically equivalent
mappings, must return the same rows. It is the strongest assertion in the corpus because
it needs no oracle — a violation is unambiguously an engine defect, not a disagreement
about what the answer should be.

That only holds if the two shapes really do carry the same information. Hand-authoring
TRADE_FLAT would break exactly that: one transcription slip and the "invariance failure"
is a fixture bug wearing an engine bug's clothes, which is worse than no test. So the flat
rows are COMPUTED from the normalized rows by walking the same joins the normalized
mapping walks, using the same resolver.

The NULLs matter most. TRD-0007 points at a counterparty that does not exist and two
trades carry no trader, so the flat row must hold NULLs in those columns — precisely what
the outer join produces on the normalized side. Encoding the join RESULT, not the join
INPUT, is what makes the two paths comparable.
"""
from __future__ import annotations

import model
import oracle
import seed

# flat column -> path from trading::Trade in the normalized model.
# Every one of these is resolved through model.py, so a typo is a build failure rather
# than a silently NULL column that would make invariance look satisfied.
COLUMNS: dict[str, list[str]] = {
    "TRADE_ID": ["tradeId"],
    "TRADE_DATE": ["tradeDate"],
    "SETTLEMENT_DATE": ["settlementDate"],
    "QUANTITY": ["quantity"],
    "PRICE": ["price"],
    "NOTIONAL": ["notional"],
    "STATUS": ["status"],
    "TRADE_TYPE": ["tradeType"],
    "CURRENCY": ["currency"],
    "COMMISSION": ["commission"],
    "FEES": ["fees"],
    "EXECUTION_VENUE": ["executionVenue"],
    "IS_BLOCK": ["isBlock"],
    "CREATED_TIME": ["createdTime"],
    "LAST_MODIFIED_TIME": ["lastModifiedTime"],
    "INSTR_ID": ["instrument", "instrumentId"],
    "INSTR_NAME": ["instrument", "name"],
    "INSTR_TICKER": ["instrument", "ticker"],
    "INSTR_ASSET_CLASS": ["instrument", "assetClass"],
    "INSTR_CURRENCY": ["instrument", "currency"],
    "INSTR_ISIN": ["instrument", "isin"],
    "BOOK_ID": ["book", "bookId"],
    "BOOK_NAME": ["book", "name"],
    "BOOK_CURRENCY": ["book", "currency"],
    "CPTY_ID": ["counterparty", "counterpartyId"],
    "CPTY_NAME": ["counterparty", "legalName"],
    "CPTY_LEI": ["counterparty", "lei"],
    "CPTY_TIER": ["counterparty", "tier"],
    "TRADER_ID": ["trader", "traderId"],
    "TRADER_FIRST": ["trader", "firstName"],
    "TRADER_LAST": ["trader", "lastName"],
}

ROOT = "trading::Trade"


def build(c: model.Corpus, tables: dict[str, list[dict]]) -> list[dict]:
    rows = []
    for t in tables["TRADE"]:
        row = {}
        for col, path in COLUMNS.items():
            _table, src, hops = c.resolve(ROOT, path)
            landed = oracle.walk(c, tables, t, hops)
            row[col] = None if landed is None else landed.get(src)
        # SIDE is deliberately carried as the SOURCE CODE, not the label: the flat mapping
        # reuses the same EnumerationMapping, so the enum translation is exercised on both
        # paths rather than being quietly pre-resolved on one of them.
        row["SIDE"] = t["SIDE"]
        rows.append(row)
    return rows


def check(c: model.Corpus, flat: list[dict]) -> list[str]:
    """The flat table is only useful if it is genuinely equivalent AND still carries the
    awkward shapes. A denormalization that quietly dropped the orphan row would make
    invariance trivially true."""
    bad = []
    declared = set(c.tables["TRADE_FLAT"].columns)
    for i, r in enumerate(flat):
        unknown = set(r) - declared
        if unknown:
            bad.append(f"TRADE_FLAT[{i}]: columns not in schema: {sorted(unknown)}")
    if len(flat) != len(seed.TABLES["TRADE"]):
        bad.append(f"TRADE_FLAT has {len(flat)} rows, TRADE has "
                   f"{len(seed.TABLES['TRADE'])} — denormalization must not change "
                   f"cardinality for a to-one join")
    if not any(r["CPTY_ID"] is None for r in flat):
        bad.append("no NULL CPTY_ID — the orphan counterparty (A1) did not survive "
                   "denormalization, so invariance would not test outer-join semantics")
    if not any(r["TRADER_ID"] is None for r in flat):
        bad.append("no NULL TRADER_ID — the NULL foreign key (A2) did not survive")
    if not any(r["INSTR_NAME"] is not None for r in flat):
        bad.append("no instrument names resolved — the join is not working")
    return bad


# Which trades have their counterparty cache POPULATED. Everything else keeps only the
# fallback FK, so the Otherwise branch is what produces its counterparty.
#
# Chosen deliberately rather than by a modulus: TRD-0007's counterparty does not exist at
# all, so it must come back NULL through BOTH branches -- Otherwise must not invent one.
_CACHED = {"TRD-0001", "TRD-0002", "TRD-0003", "TRD-0010", "TRD-0013"}


def partial(c: model.Corpus, tables: dict[str, list[dict]]) -> list[dict]:
    """TRADE_FLAT_PARTIAL: the FK always present, the inline cache present for some."""
    rows = []
    for t in tables["TRADE"]:
        cached = t["TRADE_ID"] in _CACHED
        cpty = next((x for x in tables["COUNTERPARTY"]
                     if x["COUNTERPARTY_ID"] == t["COUNTERPARTY_ID"]), None)
        rows.append(dict(
            TRADE_ID=t["TRADE_ID"], NOTIONAL=t["NOTIONAL"], STATUS=t["STATUS"],
            # The FK is carried verbatim, INCLUDING the dangling one — the fallback join
            # then finds nothing, which is the correct answer, not a reason to skip it.
            CPTY_FK=t["COUNTERPARTY_ID"],
            CPTY_ID_INLINE=(cpty or {}).get("COUNTERPARTY_ID") if cached else None,
            CPTY_NAME_INLINE=(cpty or {}).get("LEGAL_NAME") if cached else None,
            CPTY_LEI_INLINE=(cpty or {}).get("LEI") if cached else None,
        ))
    return rows


def check_partial(rows: list[dict]) -> list[str]:
    bad = []
    if not any(r["CPTY_ID_INLINE"] for r in rows):
        bad.append("no row has the counterparty cache populated; the embedded branch "
                   "would never be taken")
    if not any(r["CPTY_ID_INLINE"] is None and r["CPTY_FK"] for r in rows):
        bad.append("no row has an empty cache with a usable FK; the Otherwise branch "
                   "would never be taken, and the two mappings would agree trivially")
    return bad


# Populated by all_tables(): the tables the expansion ring generated, and only those.
EXPANDED: dict[str, list[dict]] = {}
# One (generated, base) pair per expansion ring, in order.
EXPANSION_LAYERS: list[tuple[dict, dict]] = []
# What spines.check() found when all_tables last ran; build.py reports it with the seed's checks.
SPINE_PROBLEMS: list[str] = []


def all_tables(c: model.Corpus) -> dict[str, list[dict]]:
    """The seed plus every DERIVED table. Each consumer of the corpus data goes through
    here, so a derived table can never be stale relative to its source — it does not
    exist until it is recomputed.

    Both derivations exist to make an invariance claim honest: TRADE_FLAT (L2) varies how
    a row is assembled, the partitions (L4) vary where rows come from.
    """
    import aggregate
    import expand
    import partition
    import views

    tables = dict(seed.TABLES)
    tables["TRADE_FLAT"] = build(c, seed.TABLES)
    tables.update(partition.build(c, seed.TABLES))
    tables["TRADE_FLAT_PARTIAL"] = partial(c, seed.TABLES)
    tables["TRADE_BY_BOOK"] = aggregate.build(c, seed.TABLES)
    # The expansion ring: tables one join out from the hand-written core. Added BEFORE
    # views so a view over an expanded table would still see rows.
    # Recorded so the build can validate exactly what the expansion produced. Recomputing
    # the candidate list from the raw seed would also sweep in the OTHER derived tables
    # (TRADE_FLAT, the partitions, the pre-aggregate) and demand adversarial shapes of
    # them that they deliberately do not have.
    # Expand OUTWARD in rings until the join graph stops yielding satisfiable tables.
    # Each ring's output must be checked against the base it was generated from -- what
    # counts as a foreign key changes as more tables gain rows -- so the layers are kept
    # rather than merged into one before-and-after pair.
    expanded, layers = expand.build_rings(c, tables)
    EXPANDED.clear()
    EXPANDED.update(expanded)
    EXPANSION_LAYERS.clear()
    EXPANSION_LAYERS.extend(layers)
    tables.update(expanded)
    # SPINES (the improvement plan's Phase 2): the fewest links that give every chain a deep,
    # dense or tree generator reads paths that arrive at a value. After the expansion, whose
    # tables the chains run through. The views are computed once before it, so the generators
    # it asks for their chains see the tables they will see at build time, and again after it,
    # since they read what it links.
    import spines
    for name, v in c.views.items():
        tables[name] = views.build(c, v, tables)
    before = dict(tables)
    spines.build(c, tables)
    SPINE_PROBLEMS[:] = spines.check(c, before, tables)
    # Views are computed for the ORACLE only. They are not physical tables: nothing seeds
    # them, no DDL creates them, and the engine inlines the GROUP BY. Emitting one as
    # ###Data would create a real table that shadows the view and silently stop testing
    # the aggregation at all — so emit.py and differential.py both skip anything in
    # c.views.
    for name, v in c.views.items():
        tables[name] = views.build(c, v, tables)
    # The combination matrix's join-form table, derived from its root table so each join
    # FORM pairs deterministically. Last, because it reads rows the expansion produced.
    import combos
    combos.derive_alt(c, tables)
    # and its cbrt cells' sources, perfect cubes, so their roots are exact on every platform
    combos.seed_cubes(tables)
    _narrow_single_precision(c, tables)
    return tables


class F64(float):
    """A value in an 8-byte DOUBLE column: its arithmetic is BINARY, not decimal.

    The oracle computes `+ - *` exactly, because that is what a DECIMAL column does --
    108.7500 minus 107.9000 really is 0.85 in the database and 0.8499999999999943 in binary.
    A DOUBLE column is the other case and had no tag at all, so it was being modelled as
    decimal too. A bid of 1.0708 and an ask of 1.0710 in DOUBLE columns differ by
    0.00019999999999997797, which is what the engine returns and what exact arithmetic
    refuses to produce.

    Only a TAG: the value and its repr are an ordinary float. What it changes is that
    arithmetic touching it stays in binary and keeps the tag, exactly as F32 does for four
    bytes.
    """


class F32(float):
    """A value in a 4-byte column: narrowed for arithmetic, shortest-repr for display.

    The engine does BOTH things and they disagree. A FLOAT column holding 16.1 really holds
    16.100000381469727, so `25.0 - pct` comes back as -4.799999237060547 -- and projecting
    the same column prints `16.1`, because the shortest decimal that round-trips through
    four bytes is what gets serialised.

    Modelling only the first gave an expectation of 16.100000381469727 against the engine's
    16.1; modelling only the second gave -4.8 against its -4.799999237060547. So this is a
    float whose VALUE is the narrowed one and whose repr is the short one, and the JSON
    encoder is told to use the repr.
    """

    def __new__(cls, v):
        import struct
        return super().__new__(cls, struct.unpack("f", struct.pack("f", float(v)))[0])

    def __repr__(self):
        # The shortest decimal that still round-trips through four bytes. 9 significant
        # digits always round-trips; fewer usually does, and the engine prints the fewest.
        import struct
        for digits in range(1, 10):
            s = f"%.{digits}g" % float(self)
            if struct.pack("f", float(s)) == struct.pack("f", float(self)):
                return repr(float(s))
        return repr(float(self))


def _narrow_single_precision(c: model.Corpus, tables: dict[str, list[dict]]) -> None:
    """Round every FLOAT/REAL column to what a 4-byte column can actually hold.

    A DECIMAL or DOUBLE column stores what the seed says. A FLOAT does not: it is four
    bytes, so 29.8 becomes 29.799999237060547 the moment it is written, and `25.0 - pct`
    is -4.799999237060547 rather than -4.8.

    That is not a defect and nothing is wrong with the engine's answer -- it is what FLOAT
    means. It is a defect in an ORACLE that reads the seed as written and then does exact
    decimal arithmetic on it, which is what this one did: the first FLOAT column in the
    corpus made a subtraction disagree in the seventh decimal place while the column itself
    still projected as 29.8, so the row that failed and the value that caused it looked
    unrelated.

    Applied to the assembled tables rather than to the seed source, so the ###Data element
    still carries the number a person wrote and the database still does the narrowing.
    """
    def tag(v, cls):
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            return v
        return cls(v)

    for name, rows in tables.items():
        spec = c.tables.get(name)
        if spec is None:
            continue
        by_kind = {}
        for n, col in spec.columns.items():
            base = str(col.type or "").upper().split("(")[0]
            if base in ("FLOAT", "REAL"):
                by_kind.setdefault(F32, []).append(n)
            elif base == "DOUBLE":
                by_kind.setdefault(F64, []).append(n)
        if not by_kind:
            continue
        for r in rows:
            for cls, cols in by_kind.items():
                for n in cols:
                    if r.get(n) is not None:
                        r[n] = tag(r[n], cls)


if __name__ == "__main__":
    c = model.load()
    rows = build(c, seed.TABLES)
    problems = check(c, rows)
    nulls = sum(1 for r in rows for v in r.values() if v is None)
    print(f"TRADE_FLAT: {len(rows)} rows x {len(COLUMNS) + 1} columns, "
          f"{nulls} NULL cells")
    for r in rows:
        if r["CPTY_ID"] is None or r["TRADER_ID"] is None:
            print(f"  {r['TRADE_ID']}  cpty={r['CPTY_NAME']}  trader={r['TRADER_LAST']}"
                  f"  instr={r['INSTR_NAME']}")
    print(f"\nself-check: {'OK' if not problems else str(len(problems)) + ' PROBLEMS'}")
    for p in problems:
        print("  -", p)
