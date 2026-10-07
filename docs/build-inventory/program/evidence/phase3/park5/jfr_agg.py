import json, sys, collections
def load(path):
    d = json.load(open(path))
    events = d['recording']['events']
    selfc = collections.Counter(); incl = collections.Counter(); total = 0
    for e in events:
        st = e['values'].get('stackTrace')
        if not st: continue
        frames = st['frames']
        if not frames: continue
        names = []
        for f in frames:
            m = f['method']
            cls = m['type']['name'].replace('/', '.')
            import re as _re
            cls = _re.sub(r'\$\$(Lambda|TypeSwitch)/0x[0-9a-f]+', r'$$\1', cls)
            names.append(cls + '.' + m['name'])
        # only samples within the typer phase are hard to isolate; count all
        total += 1
        selfc[names[0]] += 1
        for n in set(names):
            incl[n] += 1
    return total, selfc, incl
tt, ts, ti = load(sys.argv[1]); mt, ms, mi = load(sys.argv[2])
print('samples tip %d main %d' % (tt, mt))
keys = ['com.legend.compiler.spec.InferenceKernel.resolveOverload', 'com.legend.compiler.spec.InferenceKernel.match',
        'com.legend.compiler.spec.InferenceKernel.typeFit', 'com.legend.compiler.spec.InferenceKernel.nominalTypeFit',
        'com.legend.compiler.spec.InferenceKernel.uniqueBest', 'com.legend.compiler.spec.InferenceKernel.rankNonLambda',
        'com.legend.compiler.spec.InferenceKernel.score', 'com.legend.compiler.spec.InferenceKernel.scoreNonLambda',
        'com.legend.compiler.spec.InferenceKernel.linearizer',
        'com.legend.compiler.spec.FunctionMatch$Linearizer.linearization', 'com.legend.compiler.spec.FunctionMatch$Linearizer.c3',
        'com.legend.compiler.spec.FunctionMatch.<init>', 'com.legend.compiler.spec.FunctionMatch$TypeFit.<init>',
        'com.legend.compiler.ResolvedNames.form', 'com.legend.compiler.ResolvedNames.referents', 'com.legend.compiler.BareNames.catalog',
        'com.legend.compiler.BareNames.tiered', 'com.legend.platform.CoreFn.of',
        'com.legend.compiler.element.FunctionCompiler.functionsAt', 'com.legend.compiler.element.FunctionCompiler.compileAll',
        'com.legend.model.SignatureMangle.mangle', 'com.legend.model.FunctionId.of',
        'com.legend.compiler.spec.Typer.applyFunction', 'com.legend.compiler.spec.Overloads.checkGenericTyped',
        'com.legend.compiler.spec.Overloads.selectRankedByPresentArgs', 'com.legend.compiler.spec.Overloads.functionCandidates',
        'com.legend.compiler.StatementInline$Pass.resolvedDefinition', 'com.legend.builtin.TdsLegacy.matches',
        'com.legend.compiler.spec.Typer.synth', 'com.legend.Compiler.compileAllBodies']
print('%-75s %9s %9s' % ('inclusive samples', 'tip', 'main'))
for k in keys:
    a = ti.get(k, 0); b = mi.get(k, 0)
    if a or b:
        print('%-75s %9d %9d' % (k, a, b))
print()
print('top self (tip):')
for n, c in ts.most_common(25):
    print('  %6d %6d  %s' % (c, ms.get(n, 0), n))
print('biggest inclusive increases tip-main (com.legend only):')
diffs = sorted(((ti[n] - mi.get(n, 0), n) for n in ti if n.startswith('com.legend')), reverse=True)[:30]
for d, n in diffs:
    print('  %+6d  tip %6d main %6d  %s' % (d, ti[n], mi.get(n, 0), n))
