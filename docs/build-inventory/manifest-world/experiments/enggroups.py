import csv, re, sys, collections, json
H = sys.argv[1]
rows = list(csv.DictReader(open(f'{H}/englist.tsv'), delimiter='\t'))
G = [('A connections & runtimes (user models declare these)', r'^meta::(external::store::relational::runtime|pure::alloy::connections|external::store::model|pure::runtime|core::runtime|relational::runtime)::|^meta::relational::metamodel::(DatabaseMapper|RelationalMapper|SchemaMapper|TableMapper)$'),
     ('B standard-library functions (relational date/string functions)', r'^meta::pure::functions::(date|string|boolean)::|^meta::pure::functions::collection::removeAll$'),
     ('C legacy TDS API', r'^meta::pure::tds::(?!toRelation)|^meta::pure::functions::collection::AggregateValue$|^meta::relational::mapping::TableTDS$'),
     ('D SQL text, DDL, test-database setup', r'^meta::relational::functions::|^meta::relational::metamodel::(Create|Drop|LoadTable|Commit|Upsert|ObjectQuery|ParameterizedQuery|RelationalLambda|SQLParameter|VariableDeclaration|execute::)|^meta::alloy::service::execution::|^meta::relational::mapping::PreAndFinally|^meta::relational::(postProcessor|milestoning)::'),
     ('E execution plans, routing, graph fetch', r'^meta::pure::(executionPlan|router|graphFetch)::|^meta::relational::mapping::|^meta::pure::mapping::'),
     ('F engine extension framework (what execute() takes)', r'^meta::pure::(extension|store|model::unit|dataQuality|tds::toRelation)::|^meta::external::format::shared::'),
     ('G test-data generation + lineage', r'^meta::relational::(testDataGeneration|metamodel::data)::|^meta::pure::data::|^meta::pure::lineage::'),
     ('H corpus entry points and checks', r'^meta::legend::|^meta::json::|^meta::pure::functions::asserts::|^meta::alloy::objectReference::|^meta::pure::functions::collection::objectReferenceIn$'),
     ('I unexplained', r'.')]
grp = collections.defaultdict(list)
for r in rows:
    for name, rx in G:
        if re.search(rx, r['fqn']): grp[name].append(r); break
for name, _ in G:
    rs = grp[name]; why = collections.Counter(r['why'] for r in rs)
    corpus = sum(1 for r in rs if int(r['corpus_full']) > 0 or int(r['corpus_short']) > 0)
    files = collections.Counter(r['file'] for r in rs)
    print(f"\n{name}: {len(rs)} names ({', '.join(f'{k} {v}' for k, v in why.most_common())}); corpus files mention {corpus}")
    print("   names:", ', '.join(r['fqn'].split('::')[-1] for r in sorted(rs, key=lambda r: r['fqn'])))
    print("   files:", '; '.join(f"{f} ({n})" for f, n in files.most_common(8)), '...' if len(files) > 8 else '')
