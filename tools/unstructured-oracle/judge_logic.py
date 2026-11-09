"""The judge's rules, with no database, server or model behind them, so that they can be tested without the corpus."""
from __future__ import annotations

from decimal import Decimal

PASS, DROPPED, STALE_HIT, EMPTY_OK, FALSE_HIT, REFUSED, ERROR, UNRESOLVED = (
    "PASS", "DROPPED", "STALE_HIT", "EMPTY_OK", "FALSE_HIT", "REFUSED", "ERROR", "UNRESOLVED")
VERDICTS = (PASS, DROPPED, STALE_HIT, EMPTY_OK, FALSE_HIT, REFUSED, ERROR, UNRESOLVED)
COLUMNS = ("case", "route", "arm", "backend", "road", "verdict", "path", "section", "asOfDate", "sha256", "rank", "detail")
KEY_COLUMNS = ("case", "route", "arm", "backend")
TOP = 5


def cut(accepted: list) -> int:
    """How far down a case is judged: the top 5, or as many as it accepts, so that a match: all case can pass in principle
    (DECISIONS.md entry 3)."""
    return max(TOP, len(accepted))


def pointer(source: str, path: str, char_start: int) -> tuple:
    return (source, path, char_start)


def text_verdict(results: list[dict], accepted: list[tuple], match: str, superseded: list[tuple], negative: bool) -> dict:
    """One arm's answer to one case. `results` are the arm's rows in rank order: rank, sha256, score and locations.

    Judged at the case's cut(). A superseded pointer inside it is STALE_HIT even with the accepted one there too.
    `match: all` needs every accepted pointer and `match: any` needs one; only then is a pointer's absence a DROPPED.
    """
    if negative:
        if not results:
            return {"verdict": EMPTY_OK, "receipts": [], "detail": "nothing returned"}
        first = results[0]
        where = first["locations"][0]
        return {"verdict": FALSE_HIT, "receipts": [], "detail": f"{len(results)} returned; rank 1 {where['path']}@{where['charStart']} score {first['score']:.4f}"}
    limit = cut(accepted)
    top = [row for row in results if row["rank"] <= limit]
    found = {}
    for row in top:
        for location in row["locations"]:
            found.setdefault(pointer(location["source"], location["path"], location["charStart"]), (row, location))
    stale = [p for p in superseded if p in found]
    hit = [p for p in accepted if p in found]
    missing = [p for p in accepted if p not in found]
    if match == "all":
        satisfied = not missing
    elif match == "any":
        satisfied = bool(hit)
    else:
        raise ValueError(f"match must be all or any, not {match!r}")
    of = f"{len(hit)} of {len(accepted)} accepted in the top {limit} (match: {match})"
    if stale:
        ranks = "; ".join(f"{p[1]}@{p[2]} rank {found[p][0]['rank']}" for p in stale)
        return {"verdict": STALE_HIT, "receipts": [], "detail": f"superseded in the top {limit}: {ranks}; {of}"}
    if not satisfied:
        return {"verdict": DROPPED, "receipts": [], "detail": "missing: " + "; ".join(f"{p[1]}@{p[2]}" for p in missing) + f"; {of}"}
    receipts = [{"path": found[p][1]["path"], "section": found[p][1]["section"], "asOfDate": str(found[p][1]["asOfDate"]),
                 "sha256": found[p][0]["sha256"], "rank": found[p][0]["rank"]} for p in hit]
    return {"verdict": PASS, "receipts": receipts, "detail": of}


def comparable(rows: list[list]) -> set[tuple]:
    """Rows as a set, numbers by value: 4, 4.0 and 4.00 are one value, and a boolean is not a number."""
    def cell(value):
        if isinstance(value, bool):  # True == Decimal(1) in Python, so a boolean is carried tagged
            return ("boolean", value)
        if value is None or isinstance(value, str):
            return value
        if isinstance(value, (int, float)):
            return Decimal(repr(value))
        raise ValueError(f"a cell of a type the key cannot express: {value!r}")
    return {tuple(cell(value) for value in row) for row in rows}


def structured_verdict(results: list[list[list]], expected: list[list[list]], superseded_rows: list[list], negative: bool) -> dict:
    """The structured route's answer to one case: one result per query, each a set of rows against the key's."""
    if negative:
        returned = sum(len(result) for result in results)
        if returned == 0:
            return {"verdict": EMPTY_OK, "receipts": [], "detail": "no rows"}
        return {"verdict": FALSE_HIT, "receipts": [], "detail": f"{returned} rows returned"}
    answered = [comparable(result) for result in results]
    stale = comparable(superseded_rows)
    if any(result & stale for result in answered):
        return {"verdict": STALE_HIT, "receipts": [], "detail": f"a superseded row was returned: {sorted(map(str, stale))}"}
    differences = []
    for index, (got, want) in enumerate(zip(answered, (comparable(rows) for rows in expected)), start=1):
        if got != want:
            differences.append(f"query {index}: missing {sorted(map(str, want - got))}, unexpected {sorted(map(str, got - want))}")
    if differences:
        return {"verdict": DROPPED, "receipts": [], "detail": "; ".join(differences)}
    return {"verdict": PASS, "receipts": [], "detail": f"{sum(len(result) for result in results)} rows, as the key"}


def regressions(old: list[dict], new: list[dict]) -> list[str]:
    """Every row that was PASS and is not now, or is gone. Better counts elsewhere do not excuse one."""
    now = {tuple(row[c] for c in KEY_COLUMNS): row for row in new}
    failures = []
    for row in old:
        if row["verdict"] != PASS:
            continue
        key = tuple(row[c] for c in KEY_COLUMNS)
        if key not in now:
            failures.append(f"{' '.join(key)}: was PASS, the row is gone")
        elif now[key]["verdict"] != PASS:
            failures.append(f"{' '.join(key)}: was PASS, now {now[key]['verdict']}: {now[key]['detail']}")
    return failures


def cell(value: str) -> str:
    return value.replace("\t", " ").replace("\n", " ")


def ledger_row(identity: dict, outcome: dict) -> dict:
    """A ledger row: a PASS's receipts as parallel `|`-joined cells, one entry per accepted pointer it found."""
    receipts = outcome["receipts"]
    columns = {}
    for name in ("path", "section", "asOfDate", "sha256", "rank"):
        values = [str(receipt[name]) for receipt in receipts]
        if any("|" in value for value in values):
            raise ValueError(f"{name} holds the receipt separator: {values}")
        columns[name] = "|".join(values) if values else "-"
    return {**identity, "verdict": outcome["verdict"], **{k: cell(v) for k, v in columns.items()}, "detail": cell(outcome["detail"])}


def write_ledger(path, header: list[str], rows: list[dict]) -> None:
    lines = [f"# {line}" for line in header] + ["\t".join(COLUMNS)] + ["\t".join(row[c] for c in COLUMNS) for row in rows]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def read_ledger(path) -> list[dict]:
    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if not line.startswith("#")]
    if tuple(lines[0].split("\t")) != COLUMNS:
        raise ValueError(f"{path} does not have the ledger's columns")
    return [dict(zip(COLUMNS, line.split("\t"))) for line in lines[1:]]
