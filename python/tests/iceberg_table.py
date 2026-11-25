"""A small Iceberg table written for the tests (format version 2, unpartitioned, one snapshot per write), its files
named by the URLs they will be served at: the metadata, the manifest list and the manifests by the Iceberg spec's own
Avro schemas, the data as Parquet with the schema's field ids. Nothing here downloads or installs anything; the tests
serve the folder over HTTP on this machine.

A table's files are named by whole URLs inside its metadata, which is what a page's Iceberg table is checked for on
Python (engine.py): ``data_url``, ``manifest_url`` and ``list_url`` let a test name one somewhere else (a file on this
machine)."""

from __future__ import annotations

import io
import json
import os
import uuid
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

# the table's schema as Iceberg writes it, its field ids those the Parquet columns carry
_SCHEMA = {
    "type": "struct",
    "schema-id": 0,
    "fields": [
        {"id": 1, "name": "desk", "required": False, "type": "string"},
        {"id": 2, "name": "qty", "required": False, "type": "long"},
    ],
}

# the Iceberg spec's manifest list (v2), field by field
_MANIFEST_FILE = {
    "type": "record",
    "name": "manifest_file",
    "fields": [
        {"name": "manifest_path", "type": "string", "field-id": 500},
        {"name": "manifest_length", "type": "long", "field-id": 501},
        {"name": "partition_spec_id", "type": "int", "field-id": 502},
        {"name": "content", "type": "int", "field-id": 517},
        {"name": "sequence_number", "type": "long", "field-id": 515},
        {"name": "min_sequence_number", "type": "long", "field-id": 516},
        {"name": "added_snapshot_id", "type": "long", "field-id": 503},
        {"name": "added_files_count", "type": "int", "field-id": 504},
        {"name": "existing_files_count", "type": "int", "field-id": 505},
        {"name": "deleted_files_count", "type": "int", "field-id": 506},
        {"name": "added_rows_count", "type": "long", "field-id": 512},
        {"name": "existing_rows_count", "type": "long", "field-id": 513},
        {"name": "deleted_rows_count", "type": "long", "field-id": 514},
    ],
}

# the Iceberg spec's manifest entry (v2), with the data file's required fields
_MANIFEST_ENTRY = {
    "type": "record",
    "name": "manifest_entry",
    "fields": [
        {"name": "status", "type": "int", "field-id": 0},
        {"name": "snapshot_id", "type": ["null", "long"], "default": None, "field-id": 1},
        {"name": "sequence_number", "type": ["null", "long"], "default": None, "field-id": 3},
        {"name": "file_sequence_number", "type": ["null", "long"], "default": None, "field-id": 4},
        {
            "name": "data_file",
            "field-id": 2,
            "type": {
                "type": "record",
                "name": "r2",
                "fields": [
                    {"name": "content", "type": "int", "field-id": 134},
                    {"name": "file_path", "type": "string", "field-id": 100},
                    {"name": "file_format", "type": "string", "field-id": 101},
                    {"name": "partition", "type": {"type": "record", "name": "r102", "fields": []}, "field-id": 102},
                    {"name": "record_count", "type": "long", "field-id": 103},
                    {"name": "file_size_in_bytes", "type": "long", "field-id": 104},
                ],
            },
        },
    ],
}


def _long(n: int) -> bytes:
    """Avro's long: zigzag, then base-128 varint."""
    z = (n << 1) ^ (n >> 63)
    out = bytearray()
    while True:
        byte = z & 0x7F
        z >>= 7
        if z:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _bytes(b: bytes) -> bytes:
    return _long(len(b)) + b


def _datum(schema: Any, value: Any) -> bytes:
    """One value in Avro's binary encoding, by its schema (the types the two schemas above use)."""
    if isinstance(schema, list):  # a union: the branch's index, then the value
        branch = 0 if value is None else 1
        return _long(branch) + _datum(schema[branch], value)
    if isinstance(schema, dict) and schema.get("type") == "record":
        return b"".join(_datum(f["type"], value[f["name"]]) for f in schema["fields"])
    kind = schema["type"] if isinstance(schema, dict) else schema
    if kind == "null":
        return b""
    if kind in ("int", "long"):
        return _long(value)
    if kind == "string":
        return _bytes(value.encode("utf-8"))
    raise ValueError(f"no encoding here for {kind}")


def _avro(schema: dict, rows: list[dict], meta: dict[str, str]) -> bytes:
    """An Avro object container file: the header (its schema and metadata), one block of the rows, uncompressed."""
    sync = os.urandom(16)
    header = {"avro.schema": json.dumps(schema), "avro.codec": "null", **meta}
    out = io.BytesIO()
    out.write(b"Obj\x01")
    out.write(_long(len(header)))
    for key, value in header.items():
        out.write(_bytes(key.encode("utf-8")) + _bytes(value.encode("utf-8")))
    out.write(_long(0))
    out.write(sync)
    block = b"".join(_datum(schema, row) for row in rows)
    out.write(_long(len(rows)) + _long(len(block)) + block + sync)
    return out.getvalue()


def write(
    folder: Path,
    name: str,
    base_url: str,
    writes: list[list[tuple[str, int]]],
    data_url: str | None = None,
    manifest_url: str | None = None,
    list_url: str | None = None,
) -> str:
    """The table ``name`` in ``folder``, each of ``writes`` a snapshot appending those rows; every file named by its URL
    under ``base_url`` (where ``folder`` is served), but the last write's data file named ``data_url``, its manifest
    ``manifest_url`` and its manifest list ``list_url`` when given. Answers the table's URL, as a page names it."""
    table = folder / name
    (table / "data").mkdir(parents=True)
    (table / "metadata").mkdir()
    location = f"{base_url}/{name}"
    snapshots: list[dict] = []
    manifests: list[dict] = []
    for n, rows in enumerate(writes, start=1):
        last = n == len(writes)
        snapshot_id = 1000 + n
        data = table / "data" / f"{n:05d}.parquet"
        columns = pa.table({"desk": [r[0] for r in rows], "qty": [r[1] for r in rows]})
        columns = columns.cast(
            pa.schema(
                [
                    pa.field("desk", pa.string(), metadata={b"PARQUET:field_id": b"1"}),
                    pa.field("qty", pa.int64(), metadata={b"PARQUET:field_id": b"2"}),
                ]
            )
        )
        pq.write_table(columns, data)
        entry = {
            "status": 1,
            "snapshot_id": snapshot_id,
            "sequence_number": n,
            "file_sequence_number": n,
            "data_file": {
                "content": 0,
                "file_path": (data_url if last and data_url else f"{location}/data/{data.name}"),
                "file_format": "PARQUET",
                "partition": {},
                "record_count": len(rows),
                "file_size_in_bytes": data.stat().st_size,
            },
        }
        manifest = table / "metadata" / f"manifest-{n}.avro"
        manifest.write_bytes(
            _avro(
                _MANIFEST_ENTRY,
                [entry],
                {
                    "schema": json.dumps(_SCHEMA),
                    "schema-id": "0",
                    "partition-spec": "[]",
                    "partition-spec-id": "0",
                    "format-version": "2",
                    "content": "data",
                },
            )
        )
        manifests.append(
            {
                "manifest_path": (manifest_url if last and manifest_url else f"{location}/metadata/{manifest.name}"),
                "manifest_length": manifest.stat().st_size,
                "partition_spec_id": 0,
                "content": 0,
                "sequence_number": n,
                "min_sequence_number": n,
                "added_snapshot_id": snapshot_id,
                "added_files_count": 1,
                "existing_files_count": 0,
                "deleted_files_count": 0,
                "added_rows_count": len(rows),
                "existing_rows_count": 0,
                "deleted_rows_count": 0,
            }
        )
        manifest_list = table / "metadata" / f"snap-{snapshot_id}.avro"
        manifest_list.write_bytes(
            _avro(
                _MANIFEST_FILE,
                list(manifests),
                {"snapshot-id": str(snapshot_id), "sequence-number": str(n), "format-version": "2"},
            )
        )
        snapshots.append(
            {
                "sequence-number": n,
                "snapshot-id": snapshot_id,
                "timestamp-ms": 1760000000000 + n,
                "summary": {"operation": "append"},
                "manifest-list": (list_url if last and list_url else f"{location}/metadata/{manifest_list.name}"),
                "schema-id": 0,
                **({"parent-snapshot-id": snapshot_id - 1} if n > 1 else {}),
            }
        )
    current = snapshots[-1]["snapshot-id"]
    metadata = {
        "format-version": 2,
        "table-uuid": str(uuid.UUID(int=len(writes))),
        "location": location,
        "last-sequence-number": len(writes),
        "last-updated-ms": snapshots[-1]["timestamp-ms"],
        "last-column-id": 2,
        "current-schema-id": 0,
        "schemas": [_SCHEMA],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 999,
        "default-sort-order-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "properties": {},
        "current-snapshot-id": current,
        "refs": {"main": {"snapshot-id": current, "type": "branch"}},
        "snapshots": snapshots,
        "snapshot-log": [{"snapshot-id": s["snapshot-id"], "timestamp-ms": s["timestamp-ms"]} for s in snapshots],
        "metadata-log": [],
    }
    (table / "metadata" / "v1.metadata.json").write_text(json.dumps(metadata))
    (table / "metadata" / "version-hint.text").write_text("1")
    return location
