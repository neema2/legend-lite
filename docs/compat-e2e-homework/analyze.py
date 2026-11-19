import sys,collections
seen={}
for f in sys.argv[1:]:
    for line in open(f):
        p=line.rstrip('\n').split('\t')
        if len(p)<4: continue
        key=(p[0],p[1])
        seen[key]=p
st=collections.Counter(); fnb=collections.defaultdict(lambda:[0,0,0]); L=0; Lw=0; Lout=0; Loutw=0; Lin=0
fails=0
for k,p in seen.items():
    if p[2]=='WALKFAIL': fails+=1; continue
    o,n,i,ln,lw=map(int,p[2:7]); fail=p[7]; fns=p[8]
    L+=ln; Lw+=lw
    cls='none' if o+n+i==0 else ('inquery' if i>0 else ('nested' if n>0 else 'output'))
    st[cls]+=1
    if o>0: Lout+=ln; Loutw+=lw
    if i>0: Lin+=ln
    if fail: st['stripfail']+=1
    for kv in fns.split(';'):
        if not kv: continue
        name,v=kv.split('=');a=list(map(int,v.split('/')))
        for j in range(3): fnb[name][j]+=a[j]
print('statements',len(seen),'walkfail',fails, dict(st))
print('sql chars total',L,'| in statements with an OUTPUT print',Lout,'-> without those prints',Loutw,'saved',Lout-Loutw, '(%.1f%% of all)'%(100*(Lout-Loutw)/max(L,1)))
print('sql chars in statements that print IN the query',Lin)
print('per fn output/nested/inquery:')
for k,v in sorted(fnb.items()): print('  ',k,v)
