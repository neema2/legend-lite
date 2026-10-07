import json, sys, datetime as d
def iso(s): return d.datetime.fromisoformat(s.replace("Z","+00:00"))
for s in json.load(sys.stdin)["steps"]:
    if s.get("started_at") and s.get("completed_at"):
        secs = int((iso(s["completed_at"])-iso(s["started_at"])).total_seconds())
        print(f"{secs//60:3}m{secs%60:02d}s  {s['conclusion']:8} {s['name'][:80]}")
