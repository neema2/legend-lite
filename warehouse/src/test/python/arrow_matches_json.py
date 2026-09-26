"""The warehouse's Arrow chunks carry the same values as its JSON rows (docs/WAREHOUSE_W1_DESIGN_2026_09_26.md, W1d).

Usage: arrow_matches_json.py DIR. DIR holds json.json (the JSON API's rows, every chunk, in order) and
arrow-0.arrows, arrow-1.arrows, ... (the same statement's Arrow chunks). pyarrow, a standard Arrow reader,
reads every chunk alone; each Arrow value is written back by the JSON API's own rules and compared with the
JSON value. Prints every difference; exits 1 if there is one.
"""
import base64, datetime, decimal, json, math, pathlib, re, sys
import pyarrow as pa
import pyarrow.ipc as ipc

EPOCH = datetime.datetime(1970, 1, 1)


def java_fraction(nanos):
    """Java's LocalTime/LocalDateTime toString: the fraction in groups of three digits, none when zero."""
    if nanos == 0:
        return ""
    if nanos % 1_000_000 == 0:
        return ".%03d" % (nanos // 1_000_000)
    if nanos % 1_000 == 0:
        return ".%06d" % (nanos // 1_000)
    return ".%09d" % nanos


def java_time(seconds_of_day, nanos):
    h, rest = divmod(seconds_of_day, 3600)
    m, s = divmod(rest, 60)
    text = "%02d:%02d" % (h, m)
    if s or nanos:
        text += ":%02d" % s + java_fraction(nanos)
    return text


def java_date_time(units, per_second):
    seconds, fraction = divmod(units, per_second)
    nanos = fraction * (1_000_000_000 // per_second)
    day = EPOCH + datetime.timedelta(seconds=seconds)
    return day.date().isoformat() + "T" + java_time(day.hour * 3600 + day.minute * 60 + day.second, nanos)


def floating(x):
    if math.isnan(x):
        return "NaN"
    if math.isinf(x):
        return "Infinity" if x > 0 else "-Infinity"
    if x == 0.0 and math.copysign(1.0, x) < 0:
        return "-0.0"
    return x


UNITS = {"s": 1, "ms": 1_000, "us": 1_000_000, "ns": 1_000_000_000}


def api(value, t, raw=None):
    """An Arrow value as the JSON API writes it. raw: the array's integer view, for timestamps and times."""
    if value is None:
        return None
    if pa.types.is_list(t) or pa.types.is_fixed_size_list(t):
        return [api(v, t.value_type) for v in value]
    if pa.types.is_struct(t):
        return {t.field(i).name: api(value.get(t.field(i).name), t.field(i).type) for i in range(t.num_fields)}
    if pa.types.is_map(t):
        return [[api(k, t.key_type), api(v, t.item_type)] for k, v in value]
    if pa.types.is_boolean(t):
        return value
    if pa.types.is_integer(t):
        return str(value) if t.bit_width == 64 else value
    if pa.types.is_decimal(t):
        return format(value, "f")
    if pa.types.is_floating(t):
        return floating(value)
    if pa.types.is_string(t):
        return value
    if pa.types.is_binary(t):
        return base64.b64encode(value).decode()
    if pa.types.is_date32(t):
        return value.isoformat()
    if pa.types.is_time64(t):
        micros = raw if raw is not None else (value.hour * 3600 + value.minute * 60 + value.second) * 1_000_000 + value.microsecond
        return java_time(micros // 1_000_000, (micros % 1_000_000) * 1_000)
    if pa.types.is_timestamp(t):
        if raw is None:   # nested: a datetime (microseconds at most)
            naive = value.astimezone(datetime.timezone.utc).replace(tzinfo=None) if value.tzinfo else value
            delta = naive - EPOCH
            raw = (delta.days * 86_400 + delta.seconds) * UNITS[t.unit] + delta.microseconds * UNITS[t.unit] // 1_000_000
        text = java_date_time(raw, UNITS[t.unit])
        return text + "Z" if t.tz else text
    if t == pa.month_day_nano_interval():
        return ("interval", value.months, value.days, value.nanoseconds)
    raise ValueError(f"no rule for Arrow type {t}")


INTERVAL_PART = re.compile(r"(-?\d+) (year|years|mon|mons|month|months|day|days)")


def duckdb_interval(text):
    """DuckDB's interval text ("1 year 2 months 3 days 04:05:06.5") as months, days, nanoseconds."""
    months = days = nanos = 0
    for n, unit in INTERVAL_PART.findall(text):
        n = int(n)
        if unit.startswith("year"):
            months += 12 * n
        elif unit.startswith("mon"):
            months += n
        else:
            days += n
    clock = re.search(r"(-?)(\d+):(\d\d):(\d\d)(?:\.(\d+))?", text)
    if clock:
        sign = -1 if clock.group(1) else 1
        frac = (clock.group(5) or "").ljust(9, "0")[:9]
        nanos = sign * ((int(clock.group(2)) * 3600 + int(clock.group(3)) * 60 + int(clock.group(4))) * 1_000_000_000 + int(frac))
    return ("interval", months, days, nanos)


def main():
    d = pathlib.Path(sys.argv[1])
    # UTF-8 both ways, whatever the platform's code page (Windows reads and prints cp1252 by default)
    sys.stdout.reconfigure(encoding="utf-8")
    doc = json.loads((d / "json.json").read_text(encoding="utf-8"))
    want_rows, types = doc["rows"], doc["types"]
    got_rows = []
    chunks = sorted(d.glob("arrow-*.arrows"), key=lambda p: int(p.stem.split("-")[1]))
    for f in chunks:
        table = ipc.open_stream(f.read_bytes()).read_all()   # every chunk alone: a whole stream
        cols = []
        for i, field in enumerate(table.schema):
            col = table.column(i)
            if pa.types.is_timestamp(field.type) or pa.types.is_time64(field.type):
                # top-level times and timestamps as integers: nanoseconds survive (a Python datetime drops them)
                cols.append([None if x is None else api(x, field.type, x) for x in col.cast(pa.int64()).to_pylist()])
            else:
                cols.append([api(v, field.type) for v in col.to_pylist()])
        got_rows += [list(row) for row in zip(*cols)]
    differences = 0
    if len(got_rows) != len(want_rows):
        print(f"rows: Arrow {len(got_rows)}, JSON {len(want_rows)}")
        differences += 1
    for r, (got, want) in enumerate(zip(got_rows, want_rows)):
        for c, (g, w) in enumerate(zip(got, want)):
            if isinstance(w, dict) and set(w) == {"value", "text"}:
                w = w["value"]   # a nested cell's DuckDB text is JSON-only
            if types[c] == "INTERVAL" and w is not None:
                w = duckdb_interval(w)
            if g != w:
                differences += 1
                if differences <= 20:
                    print(f"row {r} column {c} ({types[c]}): Arrow {g!r}, JSON {w!r}")
    print(f"{len(chunks)} chunks, {len(got_rows)} rows, {differences} differences")
    sys.exit(1 if differences else 0)


if __name__ == "__main__":
    main()
