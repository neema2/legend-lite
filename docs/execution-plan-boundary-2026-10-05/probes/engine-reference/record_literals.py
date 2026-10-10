import json, os, sys, urllib.request
E = "http://127.0.0.1:6300/api/pure/v1"
MODEL = open(os.path.join(os.path.dirname(__file__), "model.pure")).read()
OUT = sys.argv[1]
os.makedirs(OUT, exist_ok=True)

def post(path, body, ctype="application/json"):
    req = urllib.request.Request(E + path, data=body.encode(), headers={"Content-Type": ctype}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=300) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()

T = "#>{s::DB.T}#"
def v(t, x): return '{"_type":"%s","value":%s}' % (t, x)
def s(x): return v("string", json.dumps(x))
def coll(*items): return '{"_type":"collection","multiplicity":{"lowerBound":%d,"upperBound":%d},"values":[%s]}' % (len(items), len(items), ",".join(items))
# name, lambda (parameters | body), parameter values (name -> protocol JSON text; absent for none)
CASES = [
    ("lit-strictdate-projected", "", T + "->extend(~d: r|%2024-01-02)->select(~[ID, d])->sort(~ID->ascending())", {}),
    ("lit-float-projected", "", T + "->extend(~f: r|1.1)->select(~[ID, f])->sort(~ID->ascending())", {}),
    ("lit-float-large-projected", "", T + "->extend(~f: r|1.5e15)->select(~[ID, f])->sort(~ID->ascending())", {}),
    ("lit-float-small-projected", "", T + "->extend(~f: r|2.5e-7)->select(~[ID, f])->sort(~ID->ascending())", {}),
    ("lit-decimal-projected", "", T + "->extend(~[p: r|2.50D, x: r|$r.ID * 2.50D])->select(~[ID, p, x])->sort(~ID->ascending())", {}),
    ("lit-datetime-projected", "", T + "->filter(r|$r.ID < 3)->extend(~t: r|%2024-01-02T10:30:00)->select(~[ID, t])->sort(~ID->ascending())", {}),
    ("lit-datetime-nanos-projected", "", T + "->filter(r|$r.ID < 3)->extend(~t: r|%2024-01-02T10:30:00.123456789)->select(~[ID, t])->sort(~ID->ascending())", {}),
    ("lit-float-times-column", "", T + "->extend(~x: r|$r.ID * 1.5)->select(~[ID, x])->sort(~ID->ascending())", {}),
    ("lit-datetime-list", "", T + "->filter(r|%2024-01-02T10:30:00->in([%2024-01-02T10:30:00, %2024-01-05T00:00:00]))->select(~[ID])->sort(~ID->ascending())", {}),
]
summary = []
for name, params, body, values in CASES:
    text = ("{" + params + "|" if params else "|") + body + "->from(s::RT)" + ("}" if params else "")
    st, lam = post("/grammar/grammarToJson/lambda", text, "text/plain")
    if st != 200:
        summary.append((name, "E1 %d" % st)); continue
    pv = "[" + ",".join('{"name":"%s","value":%s}' % (k, x) for k, x in values.items()) + "]"
    request = '{"function":' + lam + ',"model":' + json.dumps({"_type": "text", "code": MODEL}) + ',"context":{"_type":"BaseExecutionContext"},"parameterValues":' + pv + '}'
    st, ans = post("/execution/execute", request)
    open(os.path.join(OUT, name + ".lambda.pure"), "w").write(text + "\n")
    open(os.path.join(OUT, name + ".values.json"), "w").write(pv + "\n")
    open(os.path.join(OUT, name + ".answer.json"), "w").write(ans + "\n")
    open(os.path.join(OUT, name + ".request.json"), "w").write(request + "\n")
    rows = None
    try:
        rows = json.loads(ans)["result"]["rows"]
    except Exception:
        pass
    summary.append((name, "%d %s" % (st, json.dumps([r["values"] for r in rows]) if rows is not None else ans[:300].replace("\n", " "))))
for n, r in summary:
    print(n.ljust(28), r)
