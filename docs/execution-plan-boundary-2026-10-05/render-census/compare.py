"""E-0 render census: summarise one census directory, or compare two (before, after) byte for byte.

  python3 -I compare.py <dir>            # per-lane totals: renders, distinct texts, by dialect and kind
  python3 -I compare.py <before> <after> # every (lane, dialect, kind, text) whose count differs, with its text
"""
import collections
import pathlib
import re
import sys

# the one run-to-run difference measured on unchanged code: an absolute temporary path quoted in the SQL (sandbox
# folders and random temp names, e.g. read_json_objects('.../json-m2m<random>/persons.txt'));
PATH = re.compile(r"'(/Users|/private|/var|/tmp)[^']*'")
# and a random UUID: the activity comment's "executionTraceID" (the engine's per-execution trace id)
UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
# and a lambda's scope id, fn:<hex>:<length> (resolver/FunctionBodyRows.scopeId hashes the typed lambda's toString,
# which differs between runs at the same length: a PRODUCT nondeterminism, reported to its owner, masked here only)
FN_ID = re.compile(r"fn:[0-9a-f]+:")


def norm(text):
    """Every other byte must match."""
    return FN_ID.sub("fn:<id>:", UUID.sub("<uuid>", PATH.sub("'<path>'", text)))


def load(root):
    """(lane, dialect, kind, normalised text) -> renders, keyed by the normalised text itself."""
    raw = collections.Counter()
    texts = {}
    root = pathlib.Path(root)
    for lane in sorted(p for p in root.iterdir() if p.is_dir()):
        for f in lane.rglob("texts-*.tsv"):
            for line in f.read_text().splitlines():
                h, _, text = line.partition("\t")
                texts[h] = text
        for f in lane.rglob("census-*.tsv"):
            for line in f.read_text().splitlines():
                dialect, kind, h = line.split("\t")
                raw[(lane.name, dialect, kind, h)] += 1
    counts = collections.Counter()
    for (lane, dialect, kind, h), n in raw.items():
        counts[(lane, dialect, kind, norm(texts.get(h, "?" + h)))] += n
    return counts, texts


def summary(root):
    counts, texts = load(root)
    per_lane = collections.defaultdict(lambda: [0, set()])
    per_dialect = collections.Counter()
    for (lane, dialect, kind, h), n in counts.items():
        per_lane[lane][0] += n
        per_lane[lane][1].add(h)
        per_dialect[(dialect, kind)] += n
    print("%-34s %10s %10s" % ("lane", "renders", "distinct"))
    for lane, (n, hs) in sorted(per_lane.items()):
        print("%-34s %10d %10d" % (lane, n, len(hs)))
    print("%-34s %10d %10d" % ("ALL", sum(counts.values()), len({k[3] for k in counts})))
    print()
    for (dialect, kind), n in sorted(per_dialect.items(), key=lambda x: -x[1]):
        print("%-22s %-6s %10d" % (dialect, kind, n))
    missing = [k for k in counts if k[3].startswith("?")]
    if missing:
        print("\n%d renders without a recorded text" % len(missing))


def compare(before, after):
    b, bt = load(before)
    a, at = load(after)
    keys = set(b) | set(a)
    diff = sorted(k for k in keys if b[k] != a[k])
    lanes_b = {k[0] for k in b}
    lanes_a = {k[0] for k in a}
    if lanes_b != lanes_a:
        print("LANES DIFFER: only before %s, only after %s" % (sorted(lanes_b - lanes_a), sorted(lanes_a - lanes_b)))
    print("%d (lane, dialect, kind, text) entries; %d differ" % (len(keys), len(diff)))
    for k in diff[:50]:
        lane, dialect, kind, text = k
        print("%s %s %s before=%d after=%d\n    %s" % (lane, dialect, kind, b[k], a[k], text[:400]))
    return 1 if diff or lanes_b != lanes_a else 0


if __name__ == "__main__":
    if len(sys.argv) == 2:
        summary(sys.argv[1])
    else:
        sys.exit(compare(sys.argv[1], sys.argv[2]))
