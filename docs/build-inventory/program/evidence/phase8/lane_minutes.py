import json, sys, datetime as d, re, collections
def iso(s): return d.datetime.fromisoformat(s.replace("Z","+00:00"))
jobs = json.load(sys.stdin)["jobs"]
table = collections.defaultdict(dict)
for j in jobs:
    if not j.get("startedAt") or not j.get("completedAt"): continue
    m = re.match(r"(linux-arm|linux|macos|windows) / (.*) \((linux-arm|linux|macos|windows)\)$", j["name"])
    if not m: continue
    mins = round((iso(j["completedAt"])-iso(j["startedAt"])).total_seconds()/60)
    table[m.group(2)][m.group(1)] = mins
for lane, per in sorted(table.items(), key=lambda kv: -max(kv[1].values())):
    print(f'{per.get("linux","-"):>5} {per.get("macos","-"):>5} {per.get("windows","-"):>7} {per.get("linux-arm","-"):>9}  {lane[:70]}')
