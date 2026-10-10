"""L7 step 2's candidate-set judge: the shadow probe's CANDIDATES rows (LL_SHADOW=1 on //spec:manifest_world_census,
shadow.tsv in the test's outputs) from main's core and from the change, compared two ways: as a multiset of
(name as the typer saw it, candidate ids), and as a multiset of candidate ids alone (the names the call can mean,
whatever its spelling: a bare call with a one-name record and the same call qualified to that name are one meaning).

  python3 -I compare_candidates.py <shadow_main.tsv> <shadow_change.tsv>
"""
import collections, sys

def rows(path):
    by_name, by_ids = collections.Counter(), collections.Counter()
    with open(path, encoding='utf-8') as f:
        for line in f:
            p = line.rstrip('\n').split('\t')
            if p and p[0] == 'CANDIDATES':
                by_name[(p[1], p[3])] += 1
                by_ids[p[3]] += 1
    return by_name, by_ids

(an, ai), (bn, bi) = rows(sys.argv[1]), rows(sys.argv[2])
print('CANDIDATES rows: main %d (distinct (name, ids) %d), change %d (distinct %d)' % (sum(an.values()), len(an), sum(bn.values()), len(bn)))
diff = [(k, an.get(k, 0), bn.get(k, 0)) for k in sorted(set(an) | set(bn)) if an.get(k, 0) != bn.get(k, 0)]
print('differing (name, ids) rows: %d' % len(diff))
for (name, ids), x, y in diff[:60]:
    print('  %s\tmain=%d\tchange=%d\t%s' % (name, x, y, ids[:160]))
d2 = [(k, ai.get(k, 0), bi.get(k, 0)) for k in sorted(set(ai) | set(bi)) if ai.get(k, 0) != bi.get(k, 0)]
print('differing ids rows (the spelling ignored): %d' % len(d2))
for ids, x, y in d2[:60]:
    print('  main=%d\tchange=%d\t%s' % (x, y, ids[:200]))
