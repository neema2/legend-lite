import json, sys, urllib.request, collections, os
API='http://127.0.0.1:6300/api/pure/v1'
def post(path, body, text=False):
    data = body.encode() if text else json.dumps(body).encode()
    req = urllib.request.Request(API+path, data=data, method='POST',
        headers={'Content-Type': 'text/plain' if text else 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=180) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        raise RuntimeError(e.read().decode()[:400])
here=os.path.dirname(__file__)
model=post('/grammar/grammarToJson/model', open(os.path.join(here,'model.pure')).read(), True)
kinds=collections.Counter(); per={}
for line in open(os.path.join(here,'queries.tsv')):
    name, q = line.rstrip('\n').split('\t',1)
    try:
        lam=post('/grammar/grammarToJson/lambda', q, True)
        plan=post('/execution/generatePlan', {'clientVersion':'vX_X_X','function':lam,'model':model,
              'context':{'_type':'BaseExecutionContext','queryTimeOutInSeconds':60,'enableConstraints':True}})
        json.dump(plan, open(os.path.join(here,'out',name+'.json'),'w'), indent=1)
        found=[]
        def walk(n, path=''):
            if isinstance(n, dict):
                t=n.get('_type')
                if t: found.append(t)
                for k,v in n.items(): walk(v)
            elif isinstance(n, list):
                for v in n: walk(v)
        walk(plan)
        c=collections.Counter(found); per[name]=dict(c); kinds.update(c)
        print(f'OK   {name}: {sorted(c)}')
    except Exception as e:
        print(f'FAIL {name}: {str(e)[:300]}')
json.dump({'per_query':per,'all':dict(kinds)}, open(os.path.join(here,'out','_types.json'),'w'), indent=1)
