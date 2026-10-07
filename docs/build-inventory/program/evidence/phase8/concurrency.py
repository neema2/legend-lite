import json, sys, datetime as d
def iso(s): return d.datetime.fromisoformat(s.replace("Z","+00:00"))
jobs=[j for j in json.load(sys.stdin)["jobs"] if j.get("startedAt") and j.get("completedAt") and " / " in j["name"]]
for plat in ("linux","macos","windows"):
    js=sorted([j for j in jobs if j["name"].startswith(plat+" /")], key=lambda j: j["startedAt"])
    t0=iso(js[0]["startedAt"]); peak=0
    events=sorted([(iso(j["startedAt"]),1) for j in js]+[(iso(j["completedAt"]),-1) for j in js])
    cur=0
    for _,e in events:
        cur+=e; peak=max(peak,cur)
    last=max(iso(j["completedAt"]) for j in js)
    print(f"{plat:8} {len(js):2} jobs  peak concurrent {peak:2}  first start {js[0]['startedAt'][11:16]}  last start {js[-1]['startedAt'][11:16]}  all done {last.strftime('%H:%M')}  span {int((last-t0).total_seconds()//60)} min")
