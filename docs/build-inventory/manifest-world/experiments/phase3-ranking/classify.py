# Split the reference lane's OVERLOAD rows: is legend-pure's pick in our world at all?
import re, sys, collections
X = sys.argv[1]

def split_top(s, sep=","):
    out, depth, cur, prev = [], 0, "", ""
    for ch in s:
        if ch in "<({[": depth += 1
        if ch in ">)}]" and not (ch == ">" and prev == "-"): depth -= 1
        prev = ch
        if ch == sep and depth == 0:
            out.append(cur); cur = ""
        else:
            cur += ch
    if cur.strip(): out.append(cur)
    return out

def type_id(t):
    t = t.strip()
    if t.startswith("{"): return "Function"
    if t.startswith("("): return "Relation"
    base = t.split("<", 1)[0]
    return base.rsplit("::", 1)[-1]

def mult_id(m):
    m = m.strip()
    if m == "*": return "MANY"
    if ".." in m:
        lo, hi = m.split("..")
        return ("MANY" if lo == "0" else f"${lo}_MANY$") if hi == "*" else f"${lo}_{hi}$"
    return m

def typed(s):  # "Type[mult]" -> (type, mult), the multiplicity is the LAST top-level [...]
    s = s.strip()
    i = s.rindex("[")
    return s[:i], s[i + 1:-1]

ids = set()
src = open("core/src/main/java/com/legend/builtin/Pure.java", encoding="utf-8").read()
for sig in re.findall(r'signature\("native function (.*?);"\)', src):
    m = re.match(r"([A-Za-z0-9_:]+)(<[^(]*>)?\((.*)\):(.*)$", sig)
    if not m: continue
    fqn, params, ret = m.group(1), m.group(3), m.group(4)
    ps = split_top(params) if params.strip() else []
    seg = "".join("_" + type_id(typed(p.split(":", 1)[1])[0]) + "_" + mult_id(typed(p.split(":", 1)[1])[1]) + "_" for p in ps)
    rt, rm = typed(ret)
    ids.add(fqn + ("_" if not ps else "") + seg + "_" + type_id(rt) + "_" + mult_id(rm) + "_")

log = open(X + "/reflane.log").read()
suppressed = set(re.findall(r"(?:PCT\.function|platform-owned function) '([^']+)'", log))
by = collections.Counter(); ex = collections.defaultdict(list)
for line in open(X + "/reflane/core_relational.txt"):
    f = line.rstrip("\n").split("\t")
    if f[0] != "OVERLOAD" or len(f) != 5: continue
    _, spelling, ref, ours, n = f
    name = ref.split("_", 1)[0] if "::" not in ref else re.match(r"(.*::[A-Za-z0-9]+)_", ref).group(1)
    if name in suppressed and ref not in ids:
        cause = "A legend-pure's pick is dropped from our world (suppressed by name)"
    else:
        cause = "B legend-pure's pick is in our world; we chose another"
    by[cause] += int(n); ex[cause].append((int(n), spelling.rsplit("::", 1)[-1], ref.rsplit("::", 1)[-1], ours.rsplit("::", 1)[-1]))
print("our built-in ids parsed:", len(ids), "| the ours-column ids found among them:",
      sum(1 for l in open(X + "/reflane/core_relational.txt") if l.startswith("OVERLOAD\t") and l.count("\t") == 4 and l.split("\t")[3] in ids))
for k in sorted(by):
    print(f"{k}: {by[k]}")
    agg = collections.Counter()
    for n, s, r, o in ex[k]: agg[s] += n
    print("   " + ", ".join(f"{s} {n}" for s, n in agg.most_common()))
