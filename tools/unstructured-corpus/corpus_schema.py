"""What the unstructured study's loader and generators share: model.pure as data, and the rule for a synthetic passage.

Standard library only, so a generator needs neither a database driver nor the EDGAR tools.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
MODEL_PATH = REPO_ROOT / "projects" / "unstructured" / "model.pure"
CORPUS_DIRECTORY = REPO_ROOT / "projects" / "unstructured" / "corpus"
EMAILS_ROOT = CORPUS_DIRECTORY / "emails"
LOANS_ROOT = CORPUS_DIRECTORY / "loans"

COLUMN = re.compile(r"(\w+)\s+([A-Z]+(?:\([0-9, ]+\))?)((?:\s+PRIMARY KEY|\s+NOT NULL)*)")
ENUM_MAPPING = re.compile(r"\S+: EnumerationMapping (\w+)\s*\{([^}]*)\}")
ENUM_USE = re.compile(r"EnumerationMapping (\w+): \[store::\w+\](\w+)\.(\w+)")
DUCKDB_PATH = re.compile(r"specification: DuckDB \{ path: '([^']+)'; \}")
BLOCK = re.compile(r"[^\n]+(?:\n[^\n]+)*")


class Model:
    """The tables, their keys, every enum's stored values and the DuckDB file's path, as model.pure declares them."""

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

    def columns(self, table: str) -> list[str]:
        return [name for name, _, _, _ in self.tables[table]]

    def key(self, table: str) -> list[str]:
        return [name for name, _, _, in_key in self.tables[table] if in_key]

    def enum_values(self, table: str, column: str) -> list[str]:
        """Sorted, so that a seeded choice among them does not depend on a set's order."""
        return sorted(self.enums[(table, column)])

    def enum(self, table: str, column: str, value: str) -> str:
        """value, if model.pure declares it for that column; a generator spells no enum value the model does not."""
        if value not in self.enums[(table, column)]:
            raise SystemExit(f"{table}.{column}: {value!r} is not one of {self.enum_values(table, column)} in model.pure")
        return value


def load_model() -> Model:
    return Model(MODEL_PATH.read_text(encoding="utf-8"))


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
        raise SystemExit(f"model.pure: {table}: a column this tool cannot read: {entry!r}")
    name, kind, constraints = match.groups()
    in_key = "PRIMARY KEY" in constraints
    return name, kind, in_key or "NOT NULL" in constraints, in_key


def block_spans(text: str) -> list[tuple[int, int]]:
    """A synthetic corpus's passages: one per blank-line-delimited block, unmerged, in code points.

    An email's paragraph or an agreement's clause is one block, however long, because a ground-truth row's
    passageSha256 has to be the hash of exactly the paragraph or clause that states it.
    """
    return [match.span() for match in BLOCK.finditer(text)]
