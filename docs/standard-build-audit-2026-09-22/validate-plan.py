import sys,collections,json,statistics,os
_HERE=os.path.dirname(os.path.abspath(__file__))
_JD=sys.argv[1] if len(sys.argv)>1 else "/tmp/jdeps.txt"
_SRC=sys.argv[2] if len(sys.argv)>2 else "core/src/main/java"
sys.argv=["x",_JD,_SRC]
exec(open(os.path.join(_HERE,"union-graph.py"),encoding="utf-8").read().split('if __name__=="__main__":')[0])
ORIG={c:c.rsplit(".",1)[0] for c in ALL}
csc=sccs(ALL,E); comp={c:i for i,g in enumerate(csc) for c in g}
dg=collections.defaultdict(set)
for a,bs in E.items():
    for b in bs:
        if comp[a]!=comp[b]: dg[comp[a]].add(comp[b])
sys.setrecursionlimit(100000); dep={}
def dd(n):
    if n in dep: return dep[n]
    dep[n]=0; dep[n]=1+max((dd(m) for m in dg.get(n,())),default=-1); return dep[n]
for i in range(len(csc)): dd(i)
DEP={c:dep[comp[c]] for c in ALL}
def key(c):
    g=csc[comp[c]]
    if len(g)>1:
        home=collections.Counter(ORIG[x] for x in g).most_common(1)[0][0]
        return (home,DEP[c],"s%d"%comp[c])
    return (ORIG[c],DEP[c],"")
A={c:key(c) for c in ALL}
def cyc(A):
    pe=collections.defaultdict(set)
    for a,bs in E.items():
        for b in bs:
            if A[a]!=A[b]: pe[A[a]].add(A[b])
    return [g for g in sccs(set(A.values()),pe) if len(g)>1]
print(f"phase 1  {len(set(A.values()))} packages, {len(cyc(A))} cycles")
imp2=True;r=0
while imp2 and r<400:
    imp2=False;r+=1
    ks=sorted({A[c] for c in ALL},key=lambda k:(k[0],k[1]))
    for x in ks:
        for y in ks:
            if y==x or x[0]!=y[0]: continue
            B={c:(y if A[c]==x else A[c]) for c in ALL}
            if not cyc(B): A=B;imp2=True;break
        if imp2: break
print(f"phase 2  {len(set(A.values()))} packages, {len(cyc(A))} cycles")
fam=collections.defaultdict(set)
for c in ALL: fam[A[c][0]].add(A[c])
def nm(k):
    s=sorted(fam[k[0]],key=lambda t:t[1])
    return k[0] if len(s)==1 or s.index(k)==len(s)-1 else f"{k[0]}.l{s.index(k)}"
N={c:nm(A[c]) for c in ALL}
print(f"         {sum(1 for c in ALL if N[c]!=ORIG[c])} classes relocated")
und=0
for c in sorted([c for c in ALL if N[c]!=ORIG[c]],key=lambda c:-REF[c]):
    T=dict(N); T[c]=ORIG[c]
    if not cyc(T): N=T; und+=1
mv=[c for c in ALL if N[c]!=ORIG[c]]
print(f"phase 3  undid {und} -> {len(mv)} moves, {len(set(N.values()))} packages, {len(cyc(N))} cycles")
print(f"         edit surface ~{sum(REF[c]+1 for c in mv)} files")
g=collections.defaultdict(list)
for c,o in ((c,ORIG[c]) for c in mv): g[(o,N[c])].append((REF[c],c.rsplit('.',1)[1]))
sh=lambda x:x.replace("com.legend.","").replace("com.legend","(root)")
print()
for (f,t),cs in sorted(g.items(),key=lambda kv:-sum(r for r,_ in kv[1])):
    cs.sort(reverse=True)
    print(f"  {sh(f):<22} -> {sh(t):<26}{len(cs):>3} cls {sum(r+1 for r,_ in cs):>5} edits  "
          + ", ".join(n for _,n in cs[:3])+(" ..." if len(cs)>3 else ""))
json.dump({"packages":{k:sorted(v) for k,v in
           ((kk,[c for c in ALL if N[c]==kk]) for kk in set(N.values()))},
           "moves":{c:{"from":ORIG[c],"to":N[c],"refs":REF[c],"depth":DEP[c]} for c in mv}},
          open(os.path.join(_HERE,"validated-plan.json"),"w"),indent=1)
