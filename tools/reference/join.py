#!/usr/bin/env python3
"""Join the reference's resolutions (RefResolutions.java) with ours (OurResolutionsTest):
per function and spelling, exact-overload agreement, package disagreement, overload disagreement,
and what is absent on our side (forms, property reads, let/new/cast/if/match).

usage: join.py ref-resolutions.tsv our-resolutions-<module>.txt
"""
import collections, sys

ref = collections.defaultdict(lambda: collections.defaultdict(collections.Counter))
ref_fns = set()
for line in open(sys.argv[1], encoding='utf-8'):
    c = line.rstrip('\n').split('\t')
    if c[0] == 'sourceId' or len(c) < 8:
        continue
    ref_fns.add(c[6])
    if c[5]:
        ref[c[6]][c[3]][c[5]] += 1
our = collections.defaultdict(lambda: collections.defaultdict(collections.Counter))
our_failed, our_fns = set(), set()
for line in open(sys.argv[2], encoding='utf-8'):
    c = line.rstrip('\n').split('\t')
    if c[0] == 'source' or len(c) < 4:
        continue
    our_fns.add(c[1])
    if c[2] == 'FAILED':
        our_failed.add(c[1])
    elif c[2] == 'CALL' and len(c) > 4 and c[4]:
        our[c[1]][c[3][c[3].rfind('::') + 2:]][c[4]] += 1
common = [f for f in ref_fns if f in our_fns and f not in our_failed]
agree = pkg = ovl = absent = 0
dis = collections.Counter(); example = {}
for f in common:
    for sp, refc in ref[f].items():
        ourc = our[f].get(sp)
        if not ourc:
            absent += sum(refc.values()); continue
        for rid, n in refc.items():
            if rid in ourc:
                agree += n; continue
            same_pkg = any(o.startswith(rid[:rid.rfind('::') + 2] + sp + '_') for o in ourc)
            kind = 'OVERLOAD' if same_pkg else 'PACKAGE'
            if same_pkg: ovl += n
            else: pkg += n
            key = (kind, sp, rid, ','.join(sorted(ourc))); dis[key] += n; example.setdefault(key, f)
print('functions: reference %d, ours %d (failed %d), comparable %d' % (len(ref_fns), len(our_fns), len(our_failed), len(common)))
print('calls: agree %d | package disagreement %d | overload disagreement %d | absent on our side %d' % (agree, pkg, ovl, absent))
for (kind, sp, rid, ours), n in dis.most_common():
    print('%s\t%s\t%s\t%s\t%d\t%s' % (kind, sp, rid, ours, n, example[(kind, sp, rid, ours)]))
