import json, sys, urllib.request, re
BASE = 'http://localhost:6300/api/pure/v1'
def post(path, data, ctype):
    req = urllib.request.Request(BASE + path, data=data, headers={'Content-Type': ctype}, method='POST')
    try:
        with urllib.request.urlopen(req, timeout=180) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
pmcd = json.load(open(sys.argv[1]))
lam_text = open(sys.argv[2]).read()
st, lam = post('/grammar/grammarToJson/lambda', lam_text.encode(), 'text/plain')
if st != 200:
    print('LAMBDA ERR', st, lam[:2000]); sys.exit(1)
body = {'clientVersion': 'vX_X_X', 'function': json.loads(lam), 'model': pmcd, 'context': {'_type': 'BaseExecutionContext', 'queryTimeOutInSeconds': None, 'enableConstraints': True}}
json.dump(body, open(sys.argv[3], 'w'))
st, out = post('/execution/generatePlan', json.dumps(body).encode(), 'application/json')
print('HTTP', st)
if st != 200:
    print(out[:3000]); sys.exit(1)
plan = json.loads(out)
sqls = []
def walk(n):
    if isinstance(n, dict):
        if n.get('_type') == 'sql' and 'sqlQuery' in n: sqls.append(n['sqlQuery'])
        for v in n.values(): walk(v)
    elif isinstance(n, list):
        for v in n: walk(v)
walk(plan)
for s in sqls: print('SQL:', s)
