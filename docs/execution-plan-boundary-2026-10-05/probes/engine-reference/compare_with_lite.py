import json, os, sys, urllib.request
D = sys.argv[1]; URL = sys.argv[2]
def post(body):
    req = urllib.request.Request(URL, data=body.encode(), headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=300) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
def rows(text):
    # numbers kept as their text: 3.0 and 3 are different answers
    a = json.loads(text, parse_float=lambda s: "#" + s, parse_int=lambda s: "#" + s)
    return [r["values"] for r in a["result"]["rows"]], a["builder"]["columns"]
names = sorted(f[:-len(".request.json")] for f in os.listdir(D) if f.endswith(".request.json"))
for n in names:
    req = open(os.path.join(D, n + ".request.json")).read()
    eng = open(os.path.join(D, n + ".answer.json")).read()
    st, lite = post(req)
    try:
        er, ec = rows(eng)
    except Exception:
        print(n.ljust(28), "engine: no answer |", "lite:", st, (json.dumps(rows(lite)[0]) if st == 200 else lite[:120].replace("\n", " ")))
        continue
    if st != 200:
        print(n.ljust(28), "DIFF engine", json.dumps(er), "| lite", st, lite[:200].replace("\n", " ")); continue
    lr, lc = rows(lite)
    same_rows = er == lr
    same_types = [c.get("type") for c in ec] == [c.get("type") for c in lc]
    print(n.ljust(28), ("SAME" if same_rows else "DIFF"), json.dumps(er), "" if same_rows else "| lite " + json.dumps(lr),
          "" if same_types else "| types engine %s lite %s" % ([c.get("type") for c in ec], [c.get("type") for c in lc]))
