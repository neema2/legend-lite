#!/usr/bin/env python3
"""THE PLAN. depth-split -> merge back -> UNDO every move that wasn't needed.

Phase 1  split every package by dependency depth        -> acyclic by construction
Phase 2  merge bands back while acyclicity holds        -> readable package count
Phase 3  for each relocated class, try putting it BACK in its original package;
         keep it there if the graph is still acyclic.   -> the refined rule:
         a leaf only moves when something below it actually reaches it
"""
import collections,json,sys,os
_HERE=os.path.dirname(os.path.abspath(__file__))
exec(open(os.path.join(_HERE,"minimal.py"),encoding="utf-8").read().split("# genuine cross-package SCCs")[0])
ORIG={c:c.rsplit(".",1)[0] for c in ALL}
def key(c):
    g=csc[comp[c]]
    if len(g)>1:
        home=collections.Counter(ORIG[x] for x in g).most_common(1)[0][0]
        return (home,DEP[c],"scc%d"%comp[c])
    return (ORIG[c],DEP[c],"")
A={c:key(c) for c in ALL}
def cyc(A):
    pe=collections.defaultdict(set)
    for a,bs in E.items():
        for b in bs:
            if A[a]!=A[b]: pe[A[a]].add(A[b])
    return [g for g in sccs(set(A.values()),pe) if len(g)>1]
print(f"phase 1  {len(set(A.values()))} packages, {len(cyc(A))} cycles")
imp2=True; r=0
while imp2 and r<300:
    imp2=False; r+=1
    ks=sorted({A[c] for c in ALL},key=lambda k:(k[0],k[1]))
    for x in ks:
        for y in ks:
            if y==x or x[0]!=y[0]: continue
            B={c:(y if A[c]==x else A[c]) for c in ALL}
            if not cyc(B): A=B; imp2=True; break
        if imp2: break
print(f"phase 2  {len(set(A.values()))} packages, {len(cyc(A))} cycles")
fam=collections.defaultdict(set)
for c in ALL: fam[A[c][0]].add(A[c])
def nm(k):
    sibs=sorted(fam[k[0]],key=lambda t:t[1])
    return k[0] if len(sibs)==1 or sibs.index(k)==len(sibs)-1 else f"{k[0]}.l{sibs.index(k)}"
N={c:nm(A[c]) for c in ALL}
moved=[c for c in ALL if N[c]!=ORIG[c]]
print(f"         {len(moved)} classes relocated")
# phase 3: undo unnecessary moves, cheapest-to-keep first
undone=0
for c in sorted(moved,key=lambda c:-REF[c]):
    if N[c]==ORIG[c]: continue
    T=dict(N); T[c]=ORIG[c]
    if not cyc(T): N=T; undone+=1
moved=[c for c in ALL if N[c]!=ORIG[c]]
print(f"phase 3  undid {undone} unnecessary moves -> {len(moved)} classes relocated, "
      f"{len(set(N.values()))} packages, {len(cyc(N))} cycles")
print(f"         edit surface ~{sum(REF[c]+1 for c in moved)} files")
g=collections.defaultdict(list)
for c in ALL: g[N[c]].append(c)
json.dump({"packages":{k:sorted(v) for k,v in g.items()},
           "moves":{c:{"from":ORIG[c],"to":N[c],"refs":REF[c],"depth":DEP[c]} for c in moved}},
          open("docs/standard-build-audit-2026-09-22/final-packages.json","w"),indent=1)
sh=lambda x:x.replace("com.legend.","").replace("com.legend","(root)")
print("\n"+"="*80)
for k in sorted(g,key=lambda k:(-len(g[k]),k)):
    cs=sorted(g[k],key=lambda c:-REF[c]); ds=[DEP[c] for c in cs]
    inc=[c for c in cs if ORIG[c]!=k]
    print(f"\n### {sh(k)}   {len(cs)} classes, depth {min(ds)}-{max(ds)}"
          + (f"   [{len(inc)} moved in]" if inc else ""))
    for c in cs[:7]:
        t="   <- "+sh(ORIG[c]) if ORIG[c]!=k else ""
        print(f"    d{DEP[c]:<3} refs{REF[c]:>4}  {c.rsplit('.',1)[1]}{t}")
    if len(cs)>7: print(f"    ... +{len(cs)-7} more")
