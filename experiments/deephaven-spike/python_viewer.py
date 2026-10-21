"""The same handoff as the page, from Python: ask the connector for a plan's live table, fetch it
by its shared ticket, and listen to its ticks (pydeephaven-ticking, which subscribes to the whole
table; the browser's JS API subscribes to a viewport).

    pip install pydeephaven==42.7 pydeephaven-ticking==42.7
    python python_viewer.py [--plan by_symbol] [--seconds 10]
"""

import argparse
import json
import time
import urllib.request

from pydeephaven import Session
from pydeephaven.ticket import SharedTicket
import pydeephaven_ticking.table_listener as tl

parser = argparse.ArgumentParser()
parser.add_argument("--connector", default="http://localhost:8090")
parser.add_argument("--plan", default="by_symbol")
parser.add_argument("--seconds", type=float, default=10)
args = parser.parse_args()

with urllib.request.urlopen(urllib.request.Request(f"{args.connector}/api/config")) as r:
    config = json.load(r)
with urllib.request.urlopen(urllib.request.Request(
        f"{args.connector}/api/live?plan={args.plan}", method="POST")) as r:
    handle = json.load(r)

host, port = config["deephaven"].removeprefix("http://").split(":")
session = Session(host=host, port=int(port),
                  auth_type=config["login"]["type"], auth_token=config["login"]["token"])
# The JS API spells a shared ticket "h/" + id; the connector publishes that spelling, so Python asks for it too.
table = session.fetch_table(SharedTicket(b"h/" + handle["sharedTicket"].encode()))

stats = {"updates": 0, "added": 0, "modified": 0, "removed": 0, "freshness_ms": []}


def on_update(update: tl.TableUpdate) -> None:
    now_ms = time.time() * 1000
    stats["updates"] += 1
    added = update.added()
    modified = update.modified()
    stats["added"] += len(next(iter(added.values()), []))
    stats["modified"] += len(next(iter(modified.values()), []))
    stats["removed"] += len(next(iter(update.removed().values()), []))
    stamps = [s for cols in (added, modified) for s in cols.get("LastTs", []) if s is not None]
    if stamps:
        newest = max(s.as_py() for s in stamps)
        stats["freshness_ms"].append(now_ms - newest.timestamp() * 1000)


listener = tl.listen(table, on_update)
listener.start()
time.sleep(args.seconds)
listener.stop()
session.close()

fresh = sorted(stats["freshness_ms"])
pick = lambda p: round(fresh[min(len(fresh) - 1, int(p * len(fresh)))]) if fresh else None
print(json.dumps({
    "plan": args.plan,
    "sharedTicket": handle["sharedTicket"],
    "reused": handle["reused"],
    "seconds": args.seconds,
    "updates": stats["updates"],
    "rowsAdded": stats["added"],
    "rowsModified": stats["modified"],
    "rowsRemoved": stats["removed"],
    "freshnessMs": {"p50": pick(0.5), "p95": pick(0.95)},
}, indent=2))
