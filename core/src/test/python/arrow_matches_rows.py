"""core's Arrow stream carries the same values as its JSON rows (the result boundary program's track A, 2026-10-29).

Usage: arrow_matches_rows.py DIR. DIR holds rows.json (JsonRowsWriter's answer: a JSON array of one object per row,
each cell in Pure's text or token), stream.arrows (ArrowWriter's Arrow IPC stream of the same batches) and, when
present, frame.zst (the stream as the one zstd frame the ARROW_IPC answer wraps it in). pyarrow, a standard Arrow
reader, reads the stream; each Arrow value is spelled by Pure's rules (the same rules the JSON writer used) and
compared with the JSON cell. Prints every difference; exits 1 if there is one.
"""
import datetime
import decimal
import json
import math
import pathlib
import re
import sys

import pyarrow as pa
import pyarrow.ipc as ipc

EPOCH = datetime.datetime(1970, 1, 1)


def instant_nanos(text):
    """A JSON instant text (+0000, any count of fraction digits: a tagged cell's written digits) as nanoseconds since
    the epoch, or None where the text is no instant."""
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2}))?(?:\.(\d{1,9}))?\+0000", text)
    if not m:
        return None
    y, mo, d, h, mi = (int(m.group(i)) for i in range(1, 6))
    s = int(m.group(6) or 0)
    frac = (m.group(7) or "").ljust(9, "0")
    days = (datetime.date(y, mo, d) - datetime.date(1970, 1, 1)).days
    return ((days * 86_400 + h * 3_600 + mi * 60 + s) * 1_000_000_000) + int(frac)


def instant_text(nanos):
    """Pure's text of an INSTANT: nine fraction digits and +0000 (legend-engine's TDS spelling)."""
    seconds, fraction = divmod(nanos, 1_000_000_000)
    t = EPOCH + datetime.timedelta(seconds=seconds)
    return "%d-%02d-%02dT%02d:%02d:%02d.%09d+0000" % (t.year, t.month, t.day, t.hour, t.minute, t.second, fraction)


def time_nanos(text):
    """Pure's time text (HH:MM, HH:MM:SS, HH:MM:SS.fraction, as written) as nanoseconds of the day: a time64 holds
    the instant of the day, not the digits written, so a time is compared by its value."""
    parts = text.split(":")
    h, m = int(parts[0]), int(parts[1])
    s, fraction = 0, 0
    if len(parts) > 2:
        whole, _, digits = parts[2].partition(".")
        s = int(whole)
        fraction = int((digits + "000000000")[:9]) if digits else 0
    return ((h * 3600 + m * 60 + s) * 1_000_000_000) + fraction


def spelled(value, t, field):
    """An Arrow value as the JSON rows writer spells the same cell, for the comparison."""
    if value is None:
        return None
    if pa.types.is_list(t):
        return [spelled(v, t.value_type, t.value_field) for v in value]
    if pa.types.is_struct(t):
        return {t.field(i).name: spelled(value.get(t.field(i).name), t.field(i).type, t.field(i)) for i in range(t.num_fields)}
    if pa.types.is_map(t):
        return {spelled(k, t.key_type, t.key_field): spelled(v, t.item_type, t.item_field) for k, v in value}
    if pa.types.is_boolean(t):
        return value
    if pa.types.is_integer(t):
        return value
    if pa.types.is_decimal(t):
        return decimal.Decimal(value)
    if pa.types.is_floating(t):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        return value
    if pa.types.is_string(t):
        return value
    if pa.types.is_date32(t):
        return value.isoformat()
    if pa.types.is_timestamp(t):
        return ("timestamp", value)
    if pa.types.is_time64(t):
        return ("time", value)
    raise ValueError(f"no rule for Arrow type {t}")


def same(got, want, column, metadata):
    """Whether an Arrow value (spelled) is the JSON cell."""
    if got is None or want is None:
        return got is None and want is None
    if isinstance(got, tuple) and got[0] == "timestamp":
        # the INSTANT spelling (nine digits), or the same instant at the digits a tagged cell was written with
        return isinstance(want, str) and (instant_text(got[1]) == want or instant_nanos(want) == got[1])
    if isinstance(got, tuple) and got[0] == "time":
        return isinstance(want, str) and got[1] == time_nanos(want)
    if isinstance(got, str) and isinstance(want, (dict, list)):
        # a class-typed column's object: its compact JSON as utf8 (Arrow has no object type)
        try:
            got = json.loads(got)
        except ValueError:
            return False
    if isinstance(got, decimal.Decimal):
        # a decimal at the column's scale against Pure's digits at the value's own: the same number
        try:
            return got == decimal.Decimal(str(want))
        except decimal.InvalidOperation:
            return False
    if isinstance(got, float) or isinstance(want, float):
        return float(got) == float(want) and math.copysign(1.0, float(got)) == math.copysign(1.0, float(want))
    if isinstance(got, dict) and isinstance(want, dict):
        return set(got) == set(want) and all(same(got[k], want[k], column, metadata) for k in got)
    if isinstance(got, list) and isinstance(want, list):
        return len(got) == len(want) and all(same(g, w, column, metadata) for g, w in zip(got, want))
    return got == want


def raw_values(array, t, field):
    """An array's values with timestamps and times as their integers (a Python datetime drops nanoseconds), a union's
    each out of its member at the member's own precision."""
    if pa.types.is_timestamp(t) or pa.types.is_time64(t):
        tag = "timestamp" if pa.types.is_timestamp(t) else "time"
        return [None if x is None else (tag, x) for x in array.cast(pa.int64()).to_pylist()]
    if pa.types.is_union(t):
        union = array.combine_chunks() if isinstance(array, pa.ChunkedArray) else array
        codes = union.type_codes.to_pylist()
        offsets = union.offsets.to_pylist()
        members = [raw_values(union.field(k), t[k].type, t[k]) for k in range(t.num_fields)]
        by_id = {t.type_codes[k]: k for k in range(t.num_fields)}
        return [members[by_id[code]][offset] for code, offset in zip(codes, offsets)]
    return [spelled(v, t, field) for v in array.to_pylist()]


def raw_column(table, i, field):
    return raw_values(table.column(i), field.type, field)


def main():
    d = pathlib.Path(sys.argv[1])
    sys.stdout.reconfigure(encoding="utf-8")
    want_rows = json.loads((d / "rows.json").read_text(encoding="utf-8"))
    stream = (d / "stream.arrows").read_bytes()
    table = ipc.open_stream(stream).read_all()
    differences = 0
    frame = d / "frame.zst"
    if frame.exists():
        with pa.input_stream(pa.py_buffer(frame.read_bytes()), compression="zstd") as inside:
            unframed = inside.read()
        if bytes(unframed) != stream:
            print("the zstd frame does not hold the stream's bytes")
            differences += 1
    names = [f.name for f in table.schema]
    if want_rows and list(want_rows[0]) != names:
        print(f"columns: Arrow {names}, JSON {list(want_rows[0])}")
        differences += 1
    if table.num_rows != len(want_rows):
        print(f"rows: Arrow {table.num_rows}, JSON {len(want_rows)}")
        differences += 1
    columns = [raw_column(table, i, f) for i, f in enumerate(table.schema)]
    for r, want in enumerate(want_rows[:table.num_rows]):
        for c, f in enumerate(table.schema):
            g, w = columns[c][r], want.get(f.name)
            if not same(g, w, f.name, f.metadata):
                differences += 1
                if differences <= 20:
                    print(f"row {r} column {f.name} ({f.type}): Arrow {g!r}, JSON {w!r}")
    for f in table.schema:
        meta = {k.decode(): v.decode() for k, v in (f.metadata or {}).items()}
        if "legend.type" not in meta:
            print(f"column {f.name}: no legend.type in its metadata")
            differences += 1
    print(f"{len(table.schema)} columns, {table.num_rows} rows, {differences} differences")
    sys.exit(1 if differences else 0)


if __name__ == "__main__":
    main()
