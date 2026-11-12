#!/usr/bin/env python3
"""Load the unstructured study's three corpora into DuckDB and, if PG_URL is set, into Postgres.

    python tools/unstructured-corpus/load.py
    PG_URL=postgresql://host/database python tools/unstructured-corpus/load.py

Needs duckdb, pg8000 (through postgres.py) when PG_URL is set, the EDGAR corpus on disk (projects/unstructured/corpus/edgar/README.md), and the
synthetic corpora, which are committed (tools/synth-corpus rebuilds them).

The tables, their keys, the stored value of every enum and the DuckDB file's path are all read from
projects/unstructured/model.pure, so there is no second copy here to drift. Everything is checked before anything is
written; a refusal names what it refused.

A run replaces the DuckDB file, and in Postgres drops and recreates the model's tables and only those. Deterministic:
two runs print the same row counts and the same digest of the passages' sha256 set.

An EDGAR passage is whole lines of one extracted .txt file, about TARGET_CHARS long: lines are added until the next would
pass the target, so a paragraph or table row is never split and one longer than the target stands alone. A SALES or
LENDING passage is one blank-line-delimited block (corpus_schema.block_spans), an email's paragraph or an agreement's
clause, because a row's passageSha256 must be the hash of exactly the text that states it. Passages do not overlap.
charStart and charEnd count Unicode code points of the decoded file; sha256 is of exactly that span, as UTF-8. FILE_PATH is
relative to the corpus's own directory, and (FILE_PATH, CHAR_START) identifies a passage across all three.
"""
from __future__ import annotations

import csv
import datetime
import decimal
import hashlib
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "edgar-corpus"))
from fetch import CORPUS_ROOT, load_manifest, sha256_of, verify_disk  # noqa: E402
from corpus_schema import EMAILS_ROOT, LOANS_ROOT, REPO_ROOT, Model, block_spans, load_model  # noqa: E402

TARGET_CHARS = 800

# The newest DuckDB whose files //projects/unstructured:corpus_test can open: its JDBC driver is this version
# (tools/deps/jars_table.bzl, duckdb_jdbc). DuckDB reads older files, so an older Python package is fine.
NEWEST_DUCKDB_THE_JDBC_DRIVER_READS = (1, 5, 6)

REBUILD = (
    'EDGAR_USER_AGENT="Name email@example.org" python tools/edgar-corpus/fetch.py\n'
    "  python tools/edgar-corpus/extract.py"
)

SIZED_VARCHAR = re.compile(r"VARCHAR\((\d+)\)")
LINE = re.compile(r"[^\n]+")
SECTION_HEADING = re.compile(r"Section (\d+\.\d+)\b")
CLAUSE_LETTER = re.compile(r"\(([a-z])\)")


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


# ---- the synthetic corpora: one CSV per table, named for it, and one text file per document ----

def coerce(table: str, column: str, kind: str, raw: str):
    if raw == "":
        return None
    try:
        if kind == "BIGINT":
            return int(raw)
        if kind == "DATE":
            return datetime.date.fromisoformat(raw)
        if kind == "TIMESTAMP":
            return datetime.datetime.fromisoformat(raw)
        if kind.startswith("DECIMAL"):
            return decimal.Decimal(raw)
    except (ValueError, decimal.InvalidOperation):
        raise SystemExit(f"refused: {table}.{column}: value {raw!r} is not a {kind}") from None
    if kind == "BIT":
        if raw not in ("true", "false"):
            raise SystemExit(f"refused: {table}.{column}: value {raw!r} is not true or false")
        return raw == "true"
    if kind.startswith("VARCHAR"):
        return raw
    raise SystemExit(f"{table}.{column}: load.py cannot read a {kind} column from a CSV")


def csv_rows(root: Path, model: Model, table: str, derived: tuple[str, ...] = ()) -> list[dict]:
    """A ground-truth table, each value typed by its column in model.pure; derived columns are the caller's to fill."""
    path = root / "ground_truth" / f"{table}.csv"
    if not path.is_file():
        generator = "gen_emails.py" if root == EMAILS_ROOT else "gen_loan_docs.py"
        raise SystemExit(f"no {path.relative_to(REPO_ROOT).as_posix()}. Rebuild it: python tools/synth-corpus/{generator}")
    kinds = {name: kind for name, kind, _, _ in model.tables[table]}
    with open(path, newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        expected = [c for c in model.columns(table) if c not in derived]
        if reader.fieldnames != expected:
            raise SystemExit(f"refused: {table}: the CSV's columns {reader.fieldnames} are not the model's {expected}")
        return [{c: coerce(table, c, kinds[c], row[c]) for c in expected} for row in reader]


def spans_of(root: Path) -> list[dict]:
    with open(root / "ground_truth" / "spans.csv", newline="", encoding="utf-8") as file:
        return [{**row, "CHAR_START": int(row["CHAR_START"]), "CHAR_END": int(row["CHAR_END"])} for row in csv.DictReader(file)]


def document_text(root: Path, relative: str, recorded_sha: str, table: str) -> str:
    data = (root / relative).read_bytes()
    actual = sha256_of(data)
    if actual != recorded_sha:
        raise SystemExit(f"refused: {table}: {relative} hashes to {actual}, its row says {recorded_sha}")
    return data.decode("utf-8")


def block_passages(source: str, path: str, text: str, label_of, as_of: datetime.date) -> list[dict]:
    rows = []
    for ordinal, (start, end) in enumerate(block_spans(text)):
        rows.append({"SOURCE": source, "FILE_PATH": path, "SECTION_OR_DOC": label_of(text[start:end]), "ORDINAL": ordinal, "CHAR_START": start,
                     "CHAR_END": end, "SHA256": sha256_of(text[start:end].encode("utf-8")), "PASSAGE_TEXT": text[start:end], "AS_OF_DATE": as_of})
    return rows


def sales_rows(model: Model) -> tuple[dict[str, list[dict]], list[dict]]:
    tables = {t: csv_rows(EMAILS_ROOT, model, t) for t in ("SA_CLIENT", "SA_CONTACT", "SA_SALESPERSON", "SA_DEAL", "SA_ALLOCATION_REQUEST")}
    tables["SA_EMAIL"] = csv_rows(EMAILS_ROOT, model, "SA_EMAIL", derived=("BODY",))
    passages = []
    for email in tables["SA_EMAIL"]:
        path = f"bodies/{email['MESSAGE_ID']}.txt"
        email["BODY"] = document_text(EMAILS_ROOT, path, email["BODY_SHA256"], "SA_EMAIL")
        message_id = email["MESSAGE_ID"]
        passages.extend(block_passages("SALES", path, email["BODY"], lambda _: message_id, email["SENT_AT"].date()))
    tables["CO_PASSAGE"] = passages
    return tables, spans_of(EMAILS_ROOT)


def clause_labels():
    """A clause reference for each block of an agreement in order: Section 7.11(a), Section 7.11, or the front matter."""
    section = None

    def label(block: str) -> str:
        nonlocal section
        heading = SECTION_HEADING.match(block)
        if heading is not None:
            section = f"Section {heading.group(1)}"
            return section
        if block.startswith("ARTICLE "):
            return block.split("\n", 1)[0]
        if section is None:
            return "Cover"
        letter = CLAUSE_LETTER.match(block)
        return f"{section}({letter.group(1)})" if letter is not None else section

    return label


def lending_rows(model: Model) -> tuple[dict[str, list[dict]], list[dict]]:
    names = ("LN_BORROWER", "LN_CREDIT_AGREEMENT", "LN_FACILITY", "LN_COLLATERAL", "LN_COVENANT", "LN_COVENANT_STEP", "LN_EVENT_OF_DEFAULT")
    tables = {t: csv_rows(LOANS_ROOT, model, t) for t in names}
    passages = []
    for agreement in tables["LN_CREDIT_AGREEMENT"]:
        if not (LOANS_ROOT / agreement["PDF_PATH"]).is_file():
            raise SystemExit(f"refused: LN_CREDIT_AGREEMENT: no file at {agreement['PDF_PATH']}")
        path = agreement["PDF_PATH"][:-len(".pdf")] + ".txt"
        text = document_text(LOANS_ROOT, path, agreement["TEXT_SHA256"], "LN_CREDIT_AGREEMENT")
        passages.extend(block_passages("LENDING", path, text, clause_labels(), agreement["AGREEMENT_DATE"]))
    tables["CO_PASSAGE"] = passages
    return tables, spans_of(LOANS_ROOT)


def merge(into: dict[str, list[dict]], tables: dict[str, list[dict]]) -> None:
    for table, rows in tables.items():
        into.setdefault(table, []).extend(rows)


# ---- what refuses a load ----

def refuse_bad_spans(model: Model, tables: dict[str, list[dict]], spans: list[dict]) -> None:
    """A row's pointer has to land on one passage exactly, and a clause's on the clause it names."""
    passages = {(row["FILE_PATH"], row["CHAR_START"], row["CHAR_END"]): row for row in tables["CO_PASSAGE"]}
    declared = {}
    for span in spans:
        where = f"{span['TABLE_NAME']} {span['ROW_KEY']}"
        passage = passages.get((span["FILE_PATH"], span["CHAR_START"], span["CHAR_END"]))
        if passage is None:
            raise SystemExit(f"refused: {where}: no passage at {span['FILE_PATH']}[{span['CHAR_START']}:{span['CHAR_END']}]")
        if passage["SHA256"] != span["PASSAGE_SHA256"]:
            raise SystemExit(f"refused: {where}: the passage at {span['FILE_PATH']}[{span['CHAR_START']}] hashes to {passage['SHA256']}, its span says {span['PASSAGE_SHA256']}")
        declared[(span["TABLE_NAME"], span["ROW_KEY"])] = passage
    for table, rows in tables.items():
        if table == "CO_PASSAGE" or "PASSAGE_SHA256" not in model.columns(table):
            continue
        key = model.key(table)
        for row in rows:
            row_key = "|".join(str(row[c]) for c in key)
            passage = declared.get((table, row_key))
            if passage is None:
                raise SystemExit(f"refused: {table} {row_key}: no span says which passage states it")
            if passage["SHA256"] != row["PASSAGE_SHA256"]:
                raise SystemExit(f"refused: {table} {row_key}: PASSAGE_SHA256 {row['PASSAGE_SHA256']} is not its span's {passage['SHA256']}")
            if "CLAUSE_REF" in row and row["CLAUSE_REF"] != passage["SECTION_OR_DOC"]:
                raise SystemExit(f"refused: {table} {row_key}: CLAUSE_REF {row['CLAUSE_REF']!r} but its passage is {passage['SECTION_OR_DOC']!r}")


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
    identities = [(row["FILE_PATH"], row["CHAR_START"]) for row in tables["CO_PASSAGE"]]
    if len(set(identities)) != len(identities):
        raise SystemExit("refused: CO_PASSAGE: two passages share (FILE_PATH, CHAR_START), which identifies a result")
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
        rows = tables[table]
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
    import duckdb

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
    import postgres

    connection = postgres.connect(url, autocommit=False)
    try:  # one transaction: a failure leaves the tables as they were
        load(connection, model, tables, "%s")
        connection.commit()
        report("postgres: PG_URL", connection, model)
    finally:
        connection.close()


def main() -> int:
    model = load_model()
    manifest = load_manifest()
    if not manifest:
        raise SystemExit(f"no EDGAR manifest rows. Rebuild the corpus from the repository root:\n  {REBUILD}")
    if verify_disk(manifest) != 0:
        raise SystemExit(f"the EDGAR corpus on disk does not match its manifest. Rebuild it from the repository root:\n  {REBUILD}")
    tables = edgar_rows(manifest)
    spans: list[dict] = []
    for rows, its_spans in (sales_rows(model), lending_rows(model)):
        merge(tables, rows)
        spans.extend(its_spans)
    missing = [table for table in model.tables if table not in tables]
    if missing:
        raise SystemExit(f"model.pure declares tables nothing loads: {missing}")
    refuse_bad_rows(model, tables)
    refuse_bad_spans(model, tables, spans)
    for source in model.enum_values("CO_PASSAGE", "SOURCE"):
        sizes = sorted(row["CHAR_END"] - row["CHAR_START"] for row in tables["CO_PASSAGE"] if row["SOURCE"] == source)
        print(f"{source} passages: {len(sizes)}; characters median {sizes[len(sizes) // 2]}, max {sizes[-1]}")
    load_duckdb(model, tables)
    url = os.environ.get("PG_URL")
    if url is not None:
        if not url.strip():
            raise SystemExit("PG_URL is set and empty")
        load_postgres(url, model, tables)
    return 0


if __name__ == "__main__":
    sys.exit(main())
