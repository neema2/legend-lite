"""Compare the six corpus passes' RESULT files with a baseline (START_HERE.md section 5).

usage: python3 -I compare_judges.py <baseline dir with judge_*/> <current bazel-bin/spec dir>
Result files are compared as sorted lines without comment lines; logs (*.log) and params files are skipped.
"""
import os
import sys

base, cur = sys.argv[1], sys.argv[2]
judges = sorted(d for d in os.listdir(base) if d.startswith('judge_'))
differ = 0
for j in judges:
    for name in sorted(os.listdir(os.path.join(base, j))):
        if name.endswith('.log') or name.endswith('.params'):
            continue
        b = os.path.join(base, j, name)
        if os.path.isdir(b):
            # a pass's census subdirectory (the H2 passes write one): not a result file (2026-10-10)
            continue
        c = os.path.join(cur, j, name)
        if not os.path.exists(c):
            print('MISSING  %s/%s' % (j, name))
            differ += 1
            continue
        def lines(p):
            with open(p, encoding='utf-8', errors='replace') as f:
                return sorted(l.rstrip('\n') for l in f if not l.startswith('#'))
        lb, lc = lines(b), lines(c)
        if lb == lc:
            print('same     %s/%s (%d lines)' % (j, name, len(lb)))
        else:
            differ += 1
            only_b = sorted(set(lb) - set(lc))
            only_c = sorted(set(lc) - set(lb))
            print('DIFFERS  %s/%s: %d only in baseline, %d only now' % (j, name, len(only_b), len(only_c)))
            for l in only_b[:5]:
                print('   - ' + l[:200])
            for l in only_c[:5]:
                print('   + ' + l[:200])
print('result files differing: %d' % differ)
sys.exit(1 if differ else 0)
