import json, sys, urllib.request
E = "http://127.0.0.1:6300/api/pure/v1"
def post(path, body, ctype="application/json"):
    req = urllib.request.Request(E + path, data=body.encode(), headers={"Content-Type": ctype}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=300) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
for text in sys.argv[1:]:
    st, lam = post("/grammar/grammarToJson/lambda", text, "text/plain")
    if st != 200:
        print(text, "| E1", st, lam[:300]); continue
    body = '{"function":' + lam + ',"model":{"_type":"text","code":""},"context":{"_type":"BaseExecutionContext"}}'
    st, out = post("/execution/execute", body)
    print(text, "|", st, out[:600].replace("\n", " "))
