#!/usr/bin/env python3
"""The planned-failure census: reads [failure-probe] lines from each lane's log and prints the tables."""
import sys, collections, re, os
LIMIT = int(os.environ.get("LIMIT", "25"))

CAT = {}
for k in "MODEL RESOLUTION MAPPING_RESOLUTION ISLAND_SYNTAX TYPE_INFERENCE SCHEMA_INVARIANT".split():
    CAT[k] = "a"
for k in "NOT_IMPLEMENTED DIALECT_CAPABILITY RAW_SQL_REFUSAL WALLED_BODY NOT_A_CONSTANT".split():
    CAT[k] = "b"
for k in "DATA NUMBER_FORMAT DATE_TIME ARITHMETIC INDEX_OUT_OF_BOUNDS NO_SUCH_ELEMENT ASSERT_FAILED UNCHECKED_IO".split():
    CAT[k] = "c"
for k in "ILLEGAL_STATE NULL_POINTER CLASS_CAST ILLEGAL_ARGUMENT UNSUPPORTED_OPERATION".split():
    CAT[k] = "d"
PASSING = {"PASS", "SKIPPED", "ACCEPTED"}


def parse(path):
    events = []
    with open(path, errors="replace") as f:
        for line in f:
            i = line.find("[failure-probe]\t")
            if i < 0:
                continue
            parts = line[i:].rstrip("\n").split("\t")
            ev = {"event": parts[1]}
            for p in parts[2:]:
                k, _, v = p.partition("=")
                if k == "message" or k == "detail":
                    v = "\t".join([v] + parts[parts.index(p) + 1:]) if k in ("message", "detail") else v
                    ev[k] = v
                    break
                ev[k] = v
            events.append(ev)
    return events


def lane(name, paths):
    events = []
    for p in paths:
        events += parse(p)
    planned = collections.OrderedDict()   # (test, kind, where, message) -> {count, class, site}
    raised = collections.defaultdict(list)
    verdict = {}
    unnamed = []
    for e in events:
        t = e.get("test", "?")
        if e["event"] == "PLANNED":
            key = (t, e["kind"], e["where"], e["message"])
            d = planned.setdefault(key, {"count": 0, "class": e["class"], "site": e.get("site")})
            d["count"] += 1
        elif e["event"] == "RAISED":
            raised[(t, e["kind"], e["where"], e["message"])].append(e)
        elif e["event"] == "VERDICT":
            verdict[t] = (e["status"], e.get("detail", ""))
        elif e["event"] == "UNNAMED":
            unnamed.append(e)
    return planned, raised, verdict, unnamed


def report(name, paths, out):
    planned, raised, verdict, unnamed = lane(name, paths)
    w = out.write
    w(f"\n## {name}\n\ntests with a verdict line: {len(verdict)}; planned failures (distinct per test): {len(planned)};"
      f" tests with any: {len({k[0] for k in planned})}; UNNAMED (planning crashed in Failures.of): {len(unnamed)}\n\n")
    groups = collections.OrderedDict()
    for key, d in planned.items():
        t, kind, where, msg = key
        g = groups.setdefault((CAT.get(kind, "?"), kind, d["class"]), {"n": 0, "tests": set(), "raisedArm": 0,
                                                                         "raisedSides": 0, "raisedLoud": 0,
                                                                         "never": 0, "wheres": collections.Counter()})
        g["n"] += 1
        g["tests"].add(t)
        g["wheres"][where] += 1
        rs = raised.get(key, [])
        if not rs:
            g["never"] += 1
        elif any(r.get("assertErrorArm") == "true" for r in rs):
            g["raisedArm"] += 1
        elif any(r.get("inAssertErrorSides") == "true" for r in rs):
            g["raisedSides"] += 1
        else:
            g["raisedLoud"] += 1
    w("| cat | kind | class | planned | tests | raised, caught by assertError (arm) | raised inside assertError's sides |"
      " raised, uncaught | never raised |\n|---|---|---|---|---|---|---|---|---|\n")
    for (cat, kind, cls), g in sorted(groups.items()):
        w(f"| {cat} | {kind} | {cls.split('.')[-1]} | {g['n']} | {len(g['tests'])} | {g['raisedArm']} |"
          f" {g['raisedSides']} | {g['raisedLoud']} | {g['never']} |\n")
    w("\nwhere (per kind):\n")
    for (cat, kind, cls), g in sorted(groups.items()):
        w(f"- {kind}/{cls.split('.')[-1]}: " + "; ".join(f"{wh} x{n}" for wh, n in g["wheres"].most_common(6)) + "\n")
    # per test: the verdict-change question
    by_test = collections.OrderedDict()
    for key, d in planned.items():
        by_test.setdefault(key[0], []).append((key, d))
    changes = []
    msg_changes = []
    for t, items in by_test.items():
        loud = [(k, d) for k, d in items if CAT.get(k[1]) in ("a", "b", "d")]
        if not loud:
            continue
        status, detail = verdict.get(t, ("?", ""))
        first = loud[0]
        why = []
        for k, d in loud:
            rs = raised.get(k, [])
            if not rs:
                why.append(f"{k[1]} never raised")
            elif any(r.get("assertErrorArm") == "true" for r in rs):
                why.append(f"{k[1]} caught by assertError")
            elif any(r.get("inAssertErrorSides") == "true" for r in rs):
                why.append(f"{k[1]} raised inside an assertError's f")
            else:
                why.append(f"{k[1]} raised")
        if status in PASSING:
            changes.append((t, status, first, sorted(set(why))))
        else:
            k = first[0]
            if k[3][:80] not in detail:
                msg_changes.append((t, status, first, detail, sorted(set(why))))
    w(f"\n### verdict changes if every (a), (b), (d) failure failed the test loud at planning: {len(changes)}\n\n")
    for t, status, (k, d), why in changes:
        w(f"- {t} (today {status}) -- {k[1]} {d['class'].split('.')[-1]} at {k[2]}: {k[3][:160]} [{'; '.join(why)}]\n")
    w(f"\n### failing today, would fail with a different message (the first (a)/(b)/(d) planned failure's): "
      f"{len(msg_changes)}\n\n")
    for t, status, (k, d), detail, why in msg_changes[:LIMIT]:
        w(f"- {t} (today {status}: {detail[:120]}) -- would be {k[1]}: {k[3][:120]} [{'; '.join(why)}]\n")
    if len(msg_changes) > LIMIT:
        w(f"- ... {len(msg_changes) - LIMIT} more\n")
    if unnamed:
        w("\n### UNNAMED\n")
        for e in unnamed:
            w(f"- {e.get('test')} {e.get('class')} at {e.get('where')}: {e.get('message', '')[:160]}\n")
    # the four classes, with the tests
    w("\n### by class, with tests\n")
    for cat in "abcd?":
        tests = collections.defaultdict(set)
        n = 0
        for key, d in planned.items():
            if CAT.get(key[1], "?") == cat:
                n += 1
                tests[key[1]].add(key[0])
        if n:
            w(f"\n({cat}) {n} planned failure(s), {len(set().union(*tests.values()))} test(s)\n")
            for kind, ts in sorted(tests.items()):
                shown = sorted(ts)
                w(f"  - {kind}: {len(ts)} test(s): " + ", ".join(x.split('::')[-1] for x in shown[:LIMIT])
                  + (f", ... {len(ts) - LIMIT} more" if len(ts) > LIMIT else "") + "\n")
    return planned, raised, verdict, unnamed


if __name__ == "__main__":
    out = sys.stdout
    args = sys.argv[1:]
    # name=path[,path...]
    for a in args:
        name, _, ps = a.partition("=")
        report(name, ps.split(","), out)
