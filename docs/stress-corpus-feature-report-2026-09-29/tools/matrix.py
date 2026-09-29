"""EMIT taxonomy x stress corpus matrix: EMIT models per feature, corpus declarations, services
reaching the feature, and how many of those PASS in lite with a populated expectation.

Usage: matrix.py <legend-engine>/docs/emit/emit.md <emit_models.json> <profile.json> <matrix.json>
The DECL column (what the corpus declares) is hand-entered from analyze.py's output.
"""
import collections
import json
import re
import sys

EMIT_MD, EMIT_MODELS, PROFILE, OUT = sys.argv[1:5]
doc = open(EMIT_MD, encoding='utf-8').read()
tax = []
for m in re.finditer(r'^\| `([a-z-]+:[a-z0-9-]+)` \| (.*?) \|$', doc, re.M):
    tax.append((m.group(1), m.group(2)))
emit_models = json.load(open(EMIT_MODELS, encoding='utf-8'))
emit_count = collections.Counter(f for m in emit_models for f in m['features'])
for f in emit_count:
    if f not in dict(tax):
        tax.append((f, '(used by EMIT models, not in the emit.md table)'))
prof = json.load(open(PROFILE, encoding='utf-8'))
svc = collections.Counter()
ok = collections.Counter()
for r in prof:
    real = r['status'] == 'PASS' and 'ROWS' in r['exp']
    for f in r['feats']:
        svc[f] += 1
        if real:
            ok[f] += 1

# corpus declarations (anything in the model, whether or not a service reaches it) -- from analysis
DECL = {
    'scaffolding:class': '2,330 classes', 'scaffolding:relational-store': '30 databases',
    'scaffolding:relational-connection': '16', 'scaffolding:relational-mapping': '2,358 class mappings',
    'scaffolding:relation-function': '2 functions', 'scaffolding:relation-mapping': '2 class mappings',
    'scaffolding:m2m-mapping': '5 class mappings', 'scaffolding:runtime': '26',
    'scaffolding:model-connection': '3 (2 JSON, 1 XML)', 'scaffolding:model-chain-connection': '1',
    'grammar:association': '250', 'grammar:class-inheritance': '1,849 subclasses (depth <= 2)',
    'grammar:constraint': '8 on 3 classes', 'grammar:derived-property': '206', 'grammar:enumeration': '21',
    'grammar:function': '21', 'grammar:measure': '3', 'grammar:nested-association': 'yes',
    'grammar:profile': '2', 'grammar:qualified-property': '89',
    'mapping:aggregation-aware-mapping': '1', 'mapping:cross-store': '1 XStore association',
    'mapping:enumeration-mapping': '13', 'mapping:m2m-chained-relational': '2 (ModelChainConnection)',
    'mapping:m2m-derived-source-property': '-', 'mapping:m2m-local-property': '1 (surface, no service)',
    'mapping:m2m-target-instantiation': '-', 'mapping:m2m-transform': '17 property mappings',
    'mapping:mapping': '179 mappings', 'mapping:mapping-include': '158 includes',
    'mapping:operation-mapping': '1 inheritance op', 'mapping:operation-mapping-merge': '1',
    'mapping:operation-mapping-merge-validation': '1', 'mapping:relational-association-implementation': '238',
    'mapping:relational-distinct': '3', 'mapping:relational-embedded': '30', 'mapping:relational-group-by': '-',
    'mapping:relational-inline-embedded': '1', 'mapping:relational-joined-table-inheritance': '-',
    'mapping:relational-literal': '138', 'mapping:relational-literal-list': '2',
    'mapping:relational-main-table-alias': 'scope blocks: DenseMapping only', 'mapping:relational-otherwise-embedded': '4',
    'mapping:relational-polymorphic-query': 'inheritance op, no subType()', 'mapping:relational-primary-key': '276',
    'mapping:relational-single-table-inheritance': '1,848', 'mapping:relational-table-alias-column': '-',
    'mapping:router-union': '1', 'mapping:store-union': '1',
    'mapping:relation-model-join': '1', 'mapping:relation-local-property': '-',
    'store:relational-cross-schema': '5 named schemas', 'store:relational-cross-table-filter': '6',
    'store:relational-dyna-function': 'joins 17, views, 440 property mappings', 'store:relational-filter': '1,856 (+2 multigrain)',
    'store:relational-inline-view': '7 views', 'store:relational-inner-join': '390 joins',
    'store:relational-left-outer-join': 'default navigation', 'store:relational-multi-table': '437 tables',
    'store:relational-nested-join': '314 chains', 'store:relational-outer-join': '1 typed hop',
    'store:relational-semistructured': '2 columns', 'milestoning:business-temporal': '6 tables, 3 classes',
    'milestoning:processing-temporal': '3 tables', 'milestoning:bi-temporal': '2 classes',
    'milestoning:point-in-time-query': 'yes', 'milestoning:all-versions-query': '1',
    'execution:binding': '1', 'execution:external-format-binding': '1', 'execution:data-element': '14',
    'execution:multi-execution-service': '2', 'execution:post-validation': '1',
    'execution:service': '4,742', 'execution:service-parameter': '1', 'execution:service-test': '4,737 suites',
    'execution:shared-test-data': 'yes', 'execution:test-data': 'yes', 'execution:plan-generation': 'lite plans every service',
    'store:relational-semistructured-binding': '1 binding transformer',
}
rows = []
for f, desc in tax:
    rows.append(dict(feature=f, desc=desc, emit=emit_count.get(f, 0), decl=DECL.get(f, '-'),
                     services=svc.get(f, 0), pass_rows=ok.get(f, 0)))
json.dump(rows, open(OUT, 'w', encoding='utf-8'), indent=1)
print('taxonomy features:', len(tax))
for r in rows:
    print('%-52s emit %3d | decl %-32s | svc %5d | ok %5d' % (r['feature'], r['emit'], r['decl'][:32], r['services'], r['pass_rows']))
