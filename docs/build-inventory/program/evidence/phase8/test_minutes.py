import sys, os, re
rows=[]
for root, _, files in os.walk(sys.argv[1]):
    for f in files:
        if f != "test.xml": continue
        t=open(os.path.join(root,f), errors="replace").read()
        cases=[float(x) for x in re.findall(r'<testcase[^>]*\btime="([0-9.]+)"', t)]
        suites=[float(x) for x in re.findall(r'<testsuite[^>]*\btime="([0-9.]+)"', t)]
        secs = sum(cases) if cases else (max(suites) if suites else 0.0)
        rows.append((secs, os.path.relpath(root, sys.argv[1]).replace("bazel-testlogs/",""), len(cases)))
rows.sort(reverse=True)
tot=sum(r[0] for r in rows); ncase=sum(r[2] for r in rows)
top=", ".join(f"{n} {s:.0f}s" for s,n,_ in rows[:3])
print(f"{tot/60:5.1f} min in {len(rows):3} targets ({ncase:5} cases); longest: {top}")
