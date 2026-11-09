#!/usr/bin/env python3
"""Load the unstructured study's corpora into DuckDB and, if PG_URL is set, into Postgres.

    python tools/unstructured-corpus/load.py
    PG_URL=postgresql://host/database python tools/unstructured-corpus/load.py

Needs duckdb, psycopg when PG_URL is set, and the EDGAR corpus on disk (projects/unstructured/corpus/edgar/README.md).

The tables, their keys, the stored value of every enum and the DuckDB file's path are all read from
projects/unstructured/model.pure, so there is no second copy here to drift. store::Sales and store::Lending are created
empty until their generators exist. Everything is checked before anything is written; a refusal names what it refused.

A run replaces the DuckDB file, and in Postgres drops and recreates the model's tables and only those. Deterministic:
two runs print the same row counts and the same digest of the passages' sha256 set.

A passage is whole lines of one extracted .txt file, about TARGET_CHARS long: lines are added until the next would pass
the target, so a paragraph or table row is never split and one longer than the target stands alone. Passages do not
overlap. charStart and charEnd count Unicode code points of the decoded file; sha256 is of exactly that span, as UTF-8.
"""
from __future__ import annotations

import csv
import datetime
import hashlib
import os
import re
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "edgar-corpus"))
from fetch import CORPUS_ROOT, load_manifest, sha256_of, verify_disk  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
MODEL_PATH = REPO_ROOT / "projects" / "unstructured" / "model.pure"
TARGET_CHARS = 800

# The newest DuckDB whose files //projects/unstructured:corpus_test can open: its JDBC driver is this version
# (tools/deps/jars_table.bzl, duckdb_jdbc). DuckDB reads older files, so an older Python package is fine.
NEWEST_DUCKDB_THE_JDBC_DRIVER_READS = (1, 5, 6)

REBUILD = (
    'EDGAR_USER_AGENT="Name email@example.org" python tools/edgar-corpus/fetch.py\n'
    "  python tools/edgar-corpus/extract.py"
)

COLUMN = re.compile(r"(\w+)\s+([A-Z]+(?:\([0-9, ]+\))?)((?:\s+PRIMARY KEY|\s+NOT NULL)*)")
SIZED_VARCHAR = re.compile(r"VARCHAR\((\d+)\)")
ENUM_MAPPING = re.compile(r"\S+: EnumerationMapping (\w+)\s*\{([^}]*)\}")
ENUM_USE = re.compile(r"EnumerationMapping (\w+): \[store::\w+\](\w+)\.(\w+)")
DUCKDB_PATH = re.compile(r"specification: DuckDB \{ path: '([^']+)'; \}")
LINE = re.compile(r"[^\n]+")


class Model:
    """What load.py reads from model.pure."""

    def __init__(self, text: str) -> None:
        sections = {part.split("\n", 1)[0].strip(): part for part in re.sub(r"(?m)^\s*//.*$", "", text).split("###")[1:]}
        self.tables: dict[str, list[tuple[str, str, bool, bool]]] = {}  # name -> (column, type, not null, in the key)
        for name, body in tables_of(sections["Relational"]):
            self.tables[name] = [column_of(name, entry) for entry in split_top_level(body)]
        mappings = {name: set(re.findall(r"'([^']*)'", body)) for name, body in ENUM_MAPPING.findall(sections["Mapping"])}
        self.enums: dict[tuple[str, str], set[str]] = {}
        for mapping, table, column in ENUM_USE.findall(sections["Mapping"]):
            if not any(c[0] == column for c in self.tables[table]):
                raise SystemExit(f"model.pure: {table}.{column} is enum-mapped but not declared")
            self.enums[(table, column)] = mappings[mapping]
        paths = DUCKDB_PATH.findall(sections["Connection"])
        if len(paths) != 1:
            raise SystemExit(f"model.pure: expected one DuckDB connection path, found {paths}")
        self.duckdb_path = REPO_ROOT / paths[0]


def tables_of(relational: str):
    for match in re.finditer(r"\bTable\s+(\w+)\s*\(", relational):
        depth, index = 1, match.end()
        while depth:
            depth += {"(": 1, ")": -1}.get(relational[index], 0)
            index += 1
        yield match.group(1), relational[match.end():index - 1]


def split_top_level(body: str) -> list[str]:
    entries, depth, current = [], 0, ""
    for character in body:
        depth += {"(": 1, ")": -1}.get(character, 0)
        if character == "," and depth == 0:
            entries.append(current)
            current = ""
        else:
            current += character
    return [entry.strip() for entry in [*entries, current] if entry.strip()]


def column_of(table: str, entry: str) -> tuple[str, str, bool, bool]:
    match = COLUMN.fullmatch(entry)
    if match is None:
        raise SystemExit(f"model.pure: {table}: a column load.py cannot read: {entry!r}")
    name, kind, constraints = match.groups()
    in_key = "PRIMARY KEY" in constraints
    return name, kind, in_key or "NOT NULL" in constraints, in_key


def create_statement(table: str, columns: list[tuple[str, str, bool, bool]]) -> str:
    # A Pure model declares a Boolean's column BIT; in DuckDB and Postgres that is a bit string, not a boolean.
    parts = [f"{name} {'BOOLEAN' if kind == 'BIT' else kind}{' NOT NULL' if not_null else ''}"
             for name, kind, not_null, _ in columns]
    key = [name for name, _, _, in_key in columns if in_key]
    return f"CREATE TABLE {table} ({', '.join(parts)}, PRIMARY KEY ({', '.join(key)}))"


# ---- the rows ----

def passage_spans(text: str) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    start = end = None
    for line in LINE.finditer(text):
        if start is not None and line.end() - start > TARGET_CHARS:
            spans.append((start, end))
            start = None
        if start is None:
            start = line.start()
        end = line.end()
    if start is not None:
        spans.append((start, end))
    return spans


def read_text(path: str) -> str:
    return (CORPUS_ROOT / path).read_bytes().decode("utf-8")


def edgar_rows(manifest: list[dict]) -> dict[str, list[dict]]:
    tables: dict[str, list[dict]] = {"ED_FILING": [], "ED_SECTION": [], "ED_HOLDING": [], "CO_PASSAGE": []}
    for row in sorted(manifest, key=lambda r: r["path"]):
        if row["kind"] == "raw":
            if row["role"] in ("primary", "primary_doc"):
                tables["ED_FILING"].append({
                    "ACCESSION": row["accession"],
                    "CIK": row["cik"],
                    "FORM": row["form"],
                    "FILED_DATE": datetime.date.fromisoformat(row["filed"]),
                    "URL": row["source_url"],
                })
        elif row["path"].endswith(".csv"):
            with open(CORPUS_ROOT / row["path"], newline="", encoding="utf-8") as file:
                for ordinal, holding in enumerate(csv.DictReader(file)):
                    tables["ED_HOLDING"].append({
                        "ACCESSION": row["accession"],
                        "ROW_ORDINAL": ordinal,
                        "ISSUER": holding["issuer"],
                        "CUSIP": holding["cusip"],
                        "REPORTED_VALUE": int(holding["value"]),
                        "SHARES": int(holding["shares"]),
                        "PUT_CALL": holding["put_call"] or None,  # the file writes an absent putCall as empty
                        "DISCRETION": holding["discretion"],
                    })
        else:
            text = read_text(row["path"])
            if row["section"].startswith("item_"):
                tables["ED_SECTION"].append({
                    "ACCESSION": row["accession"],
                    "ITEM": row["section"][len("item_"):],
                    "SECTION_TEXT": text,
                })
            for ordinal, (start, end) in enumerate(passage_spans(text)):
                tables["CO_PASSAGE"].append({
                    "SOURCE": "EDGAR",
                    "FILE_PATH": row["path"],
                    "SECTION_OR_DOC": row["section"],
                    "ORDINAL": ordinal,
                    "CHAR_START": start,
                    "CHAR_END": end,
                    "SHA256": sha256_of(text[start:end].encode("utf-8")),
                    "PASSAGE_TEXT": text[start:end],
                    "AS_OF_DATE": datetime.date.fromisoformat(row["filed"]),
                })
    return tables


# ---- what refuses a load ----

def refuse_bad_rows(model: Model, tables: dict[str, list[dict]]) -> None:
    for table, rows in tables.items():
        columns = model.tables[table]
        names = {name for name, _, _, _ in columns}
        for row in rows:
            if set(row) != names:
                raise SystemExit(f"refused: {table}: a row's columns differ from the model's: {sorted(set(row) ^ names)}")
            for name, kind, not_null, _ in columns:
                value = row[name]
                if value is None:
                    if not_null:
                        raise SystemExit(f"refused: {table}.{name} is NOT NULL and a row has no value")
                    continue
                allowed = model.enums.get((table, name))
                if allowed is not None and value not in allowed:
                    raise SystemExit(f"refused: {table}.{name}: value {value!r} is not one model.pure declares")
                sized = SIZED_VARCHAR.fullmatch(kind)
                if sized is not None and len(value) > int(sized.group(1)):
                    raise SystemExit(f"refused: {table}.{name}: {len(value)} characters, the column holds {sized.group(1)}")
    passages = {row["SHA256"] for row in tables["CO_PASSAGE"]}
    for table, rows in tables.items():
        for row in rows:
            if "PASSAGE_SHA256" in row and row["PASSAGE_SHA256"] not in passages:
                raise SystemExit(f"refused: {table}.PASSAGE_SHA256: value {row['PASSAGE_SHA256']!r} has no CO_PASSAGE row")


# ---- writing, and reading back what was written ----

def load(connection, model: Model, tables: dict[str, list[dict]], placeholder: str) -> None:
    cursor = connection.cursor()
    for table, columns in model.tables.items():
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
        cursor.execute(create_statement(table, columns))
        rows = tables.get(table, [])
        if rows:
            names = [name for name, _, _, _ in columns]
            cursor.executemany(
                f"INSERT INTO {table} ({', '.join(names)}) VALUES ({', '.join([placeholder] * len(names))})",
                [[row[name] for name in names] for row in rows],
            )


def report(label: str, connection, model: Model) -> None:
    print(label)
    cursor = connection.cursor()
    for table in model.tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        print(f"  {table:<24}{cursor.fetchone()[0]:>8}")
    cursor.execute("SELECT DISTINCT SHA256 FROM CO_PASSAGE")
    shas = sorted(row[0] for row in cursor.fetchall())
    print(f"  CO_PASSAGE.SHA256: {len(shas)} distinct, set digest {hashlib.sha256(chr(10).join(shas).encode()).hexdigest()}")


def load_duckdb(model: Model, tables: dict[str, list[dict]]) -> None:
    version = tuple(int(part) for part in duckdb.__version__.split(".")[:3])
    if version > NEWEST_DUCKDB_THE_JDBC_DRIVER_READS:
        raise SystemExit(f"refused: duckdb {duckdb.__version__} writes a file the JDBC driver cannot open; "
                         f"use at most {'.'.join(map(str, NEWEST_DUCKDB_THE_JDBC_DRIVER_READS))}")
    target = model.duckdb_path
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + ".tmp")
    for stale in (temporary, temporary.with_name(temporary.name + ".wal")):
        stale.unlink(missing_ok=True)
    connection = duckdb.connect(str(temporary))
    try:
        load(connection, model, tables, "?")
    finally:
        connection.close()
    os.replace(temporary, target)
    connection = duckdb.connect(str(target), read_only=True)
    try:
        report(f"duckdb: {target.relative_to(REPO_ROOT).as_posix()} (package {duckdb.__version__})", connection, model)
    finally:
        connection.close()


def load_postgres(url: str, model: Model, tables: dict[str, list[dict]]) -> None:
    import psycopg

    with psycopg.connect(url) as connection:  # one transaction: a failure leaves the tables as they were
        load(connection, model, tables, "%s")
        connection.commit()
        report("postgres: PG_URL", connection, model)


def main() -> int:
    model = Model(MODEL_PATH.read_text(encoding="utf-8"))
    manifest = load_manifest()
    if not manifest:
        raise SystemExit(f"no EDGAR manifest rows. Rebuild the corpus from the repository root:\n  {REBUILD}")
    if verify_disk(manifest) != 0:
        raise SystemExit(f"the EDGAR corpus on disk does not match its manifest. Rebuild it from the repository root:\n  {REBUILD}")
    tables = edgar_rows(manifest)
    refuse_bad_rows(model, tables)
    sizes = sorted(row["CHAR_END"] - row["CHAR_START"] for row in tables["CO_PASSAGE"])
    print(f"passages: {len(sizes)}; characters median {sizes[len(sizes) // 2]}, max {sizes[-1]}; "
          f"{sum(size > TARGET_CHARS for size in sizes)} longer than {TARGET_CHARS} (one paragraph or row each)")
    load_duckdb(model, tables)
    url = os.environ.get("PG_URL")
    if url is not None:
        if not url.strip():
            raise SystemExit("PG_URL is set and empty")
        load_postgres(url, model, tables)
    return 0


if __name__ == "__main__":
    sys.exit(main())
