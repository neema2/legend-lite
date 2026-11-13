import sys,collections
STRFN={'CONCAT','STRPOS','REPLACE','SPLIT','LPAD','RPAD','FORMAT','RTRIM','LTRIM','TRIM','SUBSTRING','UPPER','LOWER','LENGTH','STARTS_WITH','ENDS_WITH','MATCHES','LEFT','RIGHT','REVERSE_STRING','REPEAT_STR','STRING_AGG','LISTAGG','JOIN_STRINGS','UC_FIRST','LC_FIRST','LIKE'}
PRINTFN={'FLOAT_PRINT','DECIMAL_TO_TEXT','BOOL_TO_TEXT','STRFTIME','FLOAT_CANON'}
seen={}
for f in sys.argv[1:]:
    for line in open(f):
        p=line.rstrip('\n').split('\t')
        if len(p)>=10 and p[2]!='WALKFAIL': seen[(p[0],p[1])]=p
cls=collections.Counter(); sites=collections.Counter(); chars=collections.Counter(); fl=collections.Counter()
for p in seen.values():
    o,n,i,ln,lw=map(int,p[2:7])
    genuine=0; typing=0; nulls=0
    for kv in p[9].split(';'):
        if not kv: continue
        k,v=kv.rsplit('=',1); v=int(v)
        pr,rest=k.split('<-',1); parent,operand=rest.split('|',1)
        if operand=='NullLit': nulls+=v; sites['typed NULL']+=v
        elif pr in PRINTFN or parent in STRFN:
            genuine+=v; sites['genuine: '+pr+' in '+('string fn' if parent in STRFN else parent)]+=v
            if pr=='FLOAT_PRINT': fl['FLOAT_PRINT in-query']+=v
        else: typing+=v; sites['typing cast ('+parent+')']+=v
    c='genuine in-query print' if genuine else ('typing/NULL casts only' if typing+nulls else ('output print only' if o else ('nested only' if n else 'no print')))
    cls[c]+=1; chars[c]+=ln
print('statements',len(seen))
for k,v in cls.most_common(): print('  %-26s %5d statements  %9d chars'%(k,v,chars[k]))
print('sites:')
agg=collections.Counter()
for k,v in sites.items():
    agg[k if not k.startswith('typing') else 'typing cast (all parents)']+=v
for k,v in agg.most_common(25): print('  ',v,k)
