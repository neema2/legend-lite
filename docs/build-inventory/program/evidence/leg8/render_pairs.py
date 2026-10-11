"""Pair the render census's differing texts (before -> after, per lane and dialect, by closeness) and print a word
diff of each pair, so every difference can be named. Run from the repository root (it loads the census's compare.py).

  python3 -I docs/build-inventory/program/evidence/leg8/render_pairs.py <census before> <census after>
  ... --mask-scope-ids           the same with every lambda scope id masked (fn:<hex>:<length>)
  ... --mask-scope-ids <lane>    every differing statement of that lane in full, before then after
"""
import difflib
import importlib.util
import pathlib
import sys

here = pathlib.Path("docs/execution-plan-boundary-2026-10-05/render-census/compare.py")
spec = importlib.util.spec_from_file_location("compare", here)
compare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(compare)

b, _ = compare.load(sys.argv[1])
a, _ = compare.load(sys.argv[2])
if len(sys.argv) > 3 and sys.argv[3] == "--mask-scope-ids":
    # a lambda's scope id hashes its typed print (resolver/FunctionBodyRows.scopeId): a type the print shows moves it
    import collections
    import re
    SCOPE = re.compile(r"fn:[0-9a-f]+:\d+")

    def masked(counts):
        out = collections.Counter()
        for (lane, dialect, kind, text), n in counts.items():
            out[(lane, dialect, kind, SCOPE.sub("fn:<id>", text))] += n
        return out

    b, a = masked(b), masked(a)
    print("with scope ids masked: %d entries differ" % sum(1 for k in set(b) | set(a) if b[k] != a[k]))
    if len(sys.argv) > 4:
        # --mask-scope-ids <lane>: every differing statement of that lane in full, before then after
        for k in sorted(set(b) | set(a)):
            if k[0] == sys.argv[4] and b[k] != a[k]:
                print("-- %s before=%d after=%d\n%s\n" % (k[1], b[k], a[k], k[3].replace("\\n", "\n")))
        sys.exit(0)
gone = [k for k in b if b[k] > a.get(k, 0)]
came = [k for k in a if a[k] > b.get(k, 0)]
pairs = []
for k in sorted(gone):
    same_place = [c for c in came if c[:3] == k[:3]]
    if not same_place:
        pairs.append((k, None))
        continue
    best = max(same_place, key=lambda c: difflib.SequenceMatcher(None, k[3], c[3]).ratio())
    came.remove(best)
    pairs.append((k, best))
for k, c in pairs:
    print("== %s %s %s  before=%d after=%d" % (k[0], k[1], k[2], b[k], a.get(k, 0)))
    if c is None:
        print("   (no counterpart after)")
        continue
    bw = k[3].replace("\\n", " ").split()
    aw = c[3].replace("\\n", " ").split()
    for op, i1, i2, j1, j2 in difflib.SequenceMatcher(None, bw, aw).get_opcodes():
        if op != "equal":
            print("   %s: [%s] -> [%s]" % (op, " ".join(bw[i1:i2]), " ".join(aw[j1:j2])))
for c in came:
    print("== %s %s %s  only after (count %d): %s" % (c[0], c[1], c[2], a[c], c[3][:300]))
