# The execution census, compared: usage: bazel run //tools/census:lanes_diff -- <base dir> <head dir> [examples]
# (paths from where it is run; Bazel workplan P3-13)
# Each lane log is compared as a multiset of lines after removing run-to-run noise (ports, sandbox paths,
# timings, UUIDs, per-run verdict ids, the order of an unordered result); what remains is listed per lane.
import collections, re, sys, glob, os
# under bazel run the program starts in its runfiles: the directories are named from where it was run
os.chdir(os.environ.get("BUILD_WORKING_DIRECTORY", "."))
NORM = [
    (re.compile(r'darwin-sandbox/\d+'), 'darwin-sandbox/N'),
    (re.compile(r'_tmp/[0-9a-f]{32}'), '_tmp/H'),
    (re.compile(r'legend-test-\d+'), 'legend-test-N'),
    (re.compile(r'(port|127\.0\.0\.1:)\s?\d+'), r'\1N'),
    (re.compile(r'\b\d+(\.\d+)? ?(ms|s)\b'), 'T'),
    (re.compile(r'after \d+ ms'), 'after T'),
    (re.compile(r'mem:[A-Za-z_]*\d{6,}'), 'mem:N'),
    (re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'), 'UUID'),
    (re.compile(r'^\[suites\] slow .*'), '[suites] slow'),
    (re.compile(r'fn:[0-9a-f]{1,8}:'), 'fn:H:'),
    (re.compile(r'^\[corpus2\] slow .*'), '[corpus2] slow'),
    # the corpus's own counters: the round-trip count is kept; the character total moves with the
    # per-run verdict ids' lengths, and the detach timings are timings
    (re.compile(r'(\[corpus2\] sql-census round-trips=\d+).*'), r'\1'),
    (re.compile(r'"executionTraceID" : "[0-9a-f-]*'), '"executionTraceID" : "ID'),
]
def norm(l):
    for r, s in NORM:
        l = r.sub(s, l)
    if '[LegendLite PCT] TDS: ' in l:   # an unordered result: compare its rows as a set
        head, _, body = l.partition('TDS: ')
        rows = body.split('\\n')
        l = head + 'TDS: ' + rows[0] + ' | ' + ' | '.join(sorted(r for r in rows[1:] if r))
    return l
def lines(p):
    with open(p, errors='replace') as f:
        return [norm(l.rstrip('\n')) for l in f]
def where(a, b):
    i = 0
    while i < min(len(a), len(b)) and a[i] == b[i]:
        i += 1
    return i
for b in sorted(glob.glob(os.path.join(sys.argv[1], '*.log'))):
    h = os.path.join(sys.argv[2], os.path.basename(b))
    cb, ch = collections.Counter(lines(b)), collections.Counter(lines(h))
    only_b = list((cb - ch).elements()); only_h = list((ch - cb).elements())
    print(f"== {os.path.basename(b)}: base-only {len(only_b)}, head-only {len(only_h)}")
    for x, y in list(zip(only_b, only_h))[:int(sys.argv[3]) if len(sys.argv) > 3 else 4]:
        i = where(x, y)
        print(f"  - ...{x[max(0,i-60):i+100]}\n  + ...{y[max(0,i-60):i+100]}")
