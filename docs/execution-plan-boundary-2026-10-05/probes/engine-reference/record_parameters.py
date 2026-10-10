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
    ("integer-filter", "n: Integer[1]", T + "->filter(r|$r.ID > $n)->select(~[ID, NAME])->sort(~ID->ascending())", {"n": v("integer", "1")}),
    ("string-filter", "s: String[1]", T + "->filter(r|$r.NAME == $s)->select(~[ID, NAME])", {"s": s("O'Brien")}),
    ("decimal-filter", "p: Decimal[1]", T + "->filter(r|$r.PRICE < $p)->select(~[ID])->sort(~ID->ascending())", {"p": v("decimal", "2.00")}),
    ("integer-twice", "n: Integer[1]", T + "->filter(r|$r.ID != $n)->extend(~plus: r|$r.ID + $n)->select(~[ID, plus])->sort(~ID->ascending())", {"n": v("integer", "2")}),
    ("strictdate-projected", "d: StrictDate[1]", T + "->extend(~d: r|$d)->select(~[ID, d])->sort(~ID->ascending())", {"d": v("strictDate", '"2024-01-02"')}),
    ("boolean-filter", "b: Boolean[1]", T + "->filter(r|$b)->select(~[ID])->sort(~ID->ascending())", {"b": v("boolean", "true")}),
    ("float-times-column", "f: Float[1]", T + "->extend(~x: r|$r.ID * $f)->select(~[ID, x])->sort(~ID->ascending())", {"f": v("float", "1.1")}),
    ("float-projected", "f: Float[1]", T + "->extend(~f: r|$f)->select(~[ID, f])->sort(~ID->ascending())", {"f": v("float", "1.1")}),
    ("float-large-projected", "f: Float[1]", T + "->extend(~f: r|$f)->select(~[ID, f])->sort(~ID->ascending())", {"f": v("float", "1.5E15")}),
    ("float-small-projected", "f: Float[1]", T + "->extend(~f: r|$f)->select(~[ID, f])->sort(~ID->ascending())", {"f": v("float", "2.5E-7")}),
    ("decimal-projected", "p: Decimal[1]", T + "->extend(~[p: r|$p, x: r|$r.ID * $p])->select(~[ID, p, x])->sort(~ID->ascending())", {"p": v("decimal", "2.50")}),
    ("datetime-projected", "t: DateTime[1]", T + "->filter(r|$r.ID < 3)->extend(~t: r|$t)->select(~[ID, t])->sort(~ID->ascending())", {"t": v("dateTime", '"2024-01-02T10:30:00"')}),
    ("datetime-nanos-projected", "t: DateTime[1]", T + "->filter(r|$r.ID < 3)->extend(~t: r|$t)->select(~[ID, t])->sort(~ID->ascending())", {"t": v("dateTime", '"2024-01-02T10:30:00.123456789"')}),
    ("date-strict-projected", "d: Date[1]", T + "->extend(~d: r|$d)->select(~[ID, d])->sort(~ID->ascending())", {"d": v("strictDate", '"2024-01-02"')}),
    ("date-datetime-projected", "d: Date[1]", T + "->extend(~d: r|$d)->select(~[ID, d])->sort(~ID->ascending())", {"d": v("dateTime", '"2024-01-02T10:30:00"')}),
    ("number-integer-filter", "n: Number[1]", T + "->filter(r|$r.ID > $n)->select(~[ID])->sort(~ID->ascending())", {"n": v("integer", "1")}),
    ("number-decimal-filter", "n: Number[1]", T + "->filter(r|$r.ID * $n > 2)->select(~[ID])->sort(~ID->ascending())", {"n": v("float", "1.5")}),
    ("number-decimal-projected", "n: Number[1]", T + "->extend(~x: r|$r.ID * $n)->select(~[ID, x])->sort(~ID->ascending())", {"n": v("float", "1.5")}),
    ("two-parameters", "lo: Integer[1], hi: Integer[1]", T + "->filter(r|($r.ID > $lo) && ($r.ID < $hi))->select(~[ID, NAME])", {"lo": v("integer", "1"), "hi": v("integer", "3")}),
    ("optional-string-present", "x: String[0..1]", T + "->filter(r|$r.NAME == $x)->select(~[ID, NAME])->sort(~ID->ascending())", {"x": s("a")}),
    ("optional-string-absent", "x: String[0..1]", T + "->filter(r|$r.NAME == $x)->select(~[ID, NAME])->sort(~ID->ascending())", {}),
    ("optional-integer-present", "x: Integer[0..1]", T + "->filter(r|$r.ID != $x)->select(~[ID])->sort(~ID->ascending())", {"x": v("integer", "1")}),
    ("optional-integer-absent", "x: Integer[0..1]", T + "->filter(r|$r.ID != $x)->select(~[ID])->sort(~ID->ascending())", {}),
    ("optional-float-present", "x: Float[0..1]", T + "->filter(r|$r.ID != $x)->select(~[ID])->sort(~ID->ascending())", {"x": v("float", "2.0")}),
    ("optional-float-absent", "x: Float[0..1]", T + "->filter(r|$r.ID != $x)->select(~[ID])->sort(~ID->ascending())", {}),
    ("list-in", "ns: Integer[*]", T + "->filter(r|$r.ID->in($ns))->select(~[ID, NAME])->sort(~ID->ascending())", {"ns": coll(v("integer", "1"), v("integer", "3"))}),
    ("list-empty", "ns: Integer[*]", T + "->filter(r|$r.ID->in($ns))->select(~[ID])->sort(~ID->ascending())", {"ns": coll()}),
    ("list-not-in", "ns: Integer[*]", T + "->filter(r|!$r.ID->in($ns))->select(~[ID])->sort(~ID->ascending())", {"ns": coll(v("integer", "1"), v("integer", "3"))}),
    ("list-contains", "ns: Integer[*]", T + "->filter(r|$ns->contains($r.ID))->select(~[ID])->sort(~ID->ascending())", {"ns": coll(v("integer", "2"), v("integer", "3"))}),
    ("list-strings", "ss: String[*]", T + "->filter(r|$r.ID->in([1, 2]) && $ss->contains($r.NAME->toOne()))->select(~[ID, NAME])->sort(~ID->ascending())", {"ss": coll(s("a"), s("O'Brien"))}),
    ("list-datetimes", "ts: DateTime[*]", T + "->filter(r|%2024-01-02T10:30:00->in($ts))->select(~[ID])->sort(~ID->ascending())", {"ts": coll(v("dateTime", '"2024-01-02T10:30:00"'), v("dateTime", '"2024-01-05T00:00:00"'))}),
]
summary = []
for name, params, body, values in CASES:
    text = "{" + params + "|" + body + "->from(s::RT)}"
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
