import duckdb, json
db = duckdb.connect(':memory:')
db.execute("CREATE SCHEMA sales; CREATE SCHEMA base; CREATE TABLE base.trades(id INT, region VARCHAR); CREATE VIEW sales.trades AS SELECT * FROM base.trades")
db.execute("CREATE MACRO sales.m(x) AS x + 1; CREATE MACRO sales.tm() AS TABLE SELECT * FROM base.trades")

GRANTED = {'sales.trades', 'trades'}          # what the role may name
ALLOWED_FUNCS = {'+', 'count', 'sum', 'upper', 'lower', 'coalesce', '=', '>', '<', 'and', 'or', 'getvariable'}

def analyse(sql):
    raw = db.execute("SELECT json_serialize_sql(?)", [sql]).fetchone()[0]
    j = json.loads(raw)
    if j.get('error'):
        return {'parsed': False, 'why': j.get('error_message', '')[:70]}
    stmts = j.get('statements', [])
    tables, tfuncs, funcs, ctes, kinds = set(), set(), set(), set(), set()
    def walk(n):
        if isinstance(n, dict):
            t = n.get('type'); cls = n.get('class')
            if cls: kinds.add(cls)
            if t == 'BASE_TABLE':
                tables.add('.'.join(x for x in [n.get('catalog_name'), n.get('schema_name'), n.get('table_name')] if x))
            elif t == 'TABLE_FUNCTION':
                f = n.get('function', {})
                tfuncs.add(f.get('function_name'))
            if cls == 'FUNCTION' or t in ('FUNCTION',):
                funcs.add(n.get('function_name'))
            if cls in ('COMPARISON', 'CONJUNCTION', 'OPERATOR') and t: funcs.add(t)
            if 'cte_map' in n:
                for e in n['cte_map'].get('map', []):
                    ctes.add(e.get('key'))
            for v in n.values(): walk(v)
        elif isinstance(n, list):
            for v in n: walk(v)
    walk(stmts)
    return {'parsed': True, 'n': len(stmts), 'tables': tables - ctes, 'ctes': ctes, 'tfuncs': tfuncs, 'funcs': funcs}

def verdict(a):
    if not a['parsed']: return 'DENY (not a parseable SELECT)'
    if a['n'] != 1: return f"DENY ({a['n']} statements)"
    bad_t = [t for t in a['tables'] if t not in GRANTED]
    if bad_t: return f'DENY (object not granted: {bad_t})'
    if a['tfuncs']: return f"DENY (table function: {sorted(a['tfuncs'])})"
    return 'ALLOW'

CASES = [
 ('ok: plain', "SELECT * FROM trades WHERE region = 'EMEA'"),
 ('ok: schema-qualified', "SELECT count(*) FROM sales.trades"),
 ('base table', "SELECT * FROM base.trades"),
 ('base in subquery', "SELECT * FROM trades WHERE id IN (SELECT id FROM base.trades)"),
 ('base in CTE', "WITH x AS (SELECT * FROM base.trades) SELECT * FROM x"),
 ('recursive CTE over base', "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < (SELECT count(*) FROM base.trades)) SELECT * FROM r"),
 ('CTE shadowing a base name', "WITH trades AS (SELECT * FROM base.trades) SELECT * FROM trades"),
 ('lateral', "SELECT * FROM trades t, LATERAL (SELECT * FROM base.trades b WHERE b.id = t.id)"),
 ('set operation', "SELECT id FROM trades UNION SELECT id FROM base.trades"),
 ('scalar subquery in select', "SELECT (SELECT max(id) FROM base.trades)"),
 ('friendly FROM-first', "FROM base.trades"),
 ('file path as table', "SELECT * FROM '/etc/passwd'"),
 ('s3 path as table', "SELECT * FROM 's3://bucket/secret.parquet'"),
 ('read_csv', "SELECT * FROM read_csv('/etc/passwd')"),
 ('query() table function', "SELECT * FROM query('SELECT * FROM base.trades')"),
 ('query_table()', "SELECT * FROM query_table('base.trades')"),
 ('table macro', "SELECT * FROM sales.tm()"),
 ('duckdb_secrets()', "SELECT * FROM duckdb_secrets()"),
 ('duckdb_tables()', "SELECT * FROM duckdb_tables()"),
 ('glob', "SELECT * FROM glob('/*')"),
 ('range (harmless, still a table fn)', "SELECT * FROM range(3)"),
 ('PIVOT over base', "PIVOT base.trades ON region USING count(*)"),
 ('UNPIVOT', "UNPIVOT trades ON id INTO NAME k VALUE v"),
 ('SUMMARIZE', "SUMMARIZE base.trades"),
 ('DESCRIBE', "DESCRIBE base.trades"),
 ('SHOW TABLES', "SHOW TABLES"),
 ('EXPLAIN', "EXPLAIN SELECT * FROM base.trades"),
 ('two statements', "SELECT 1; SELECT * FROM base.trades"),
 ('SET VARIABLE', "SET VARIABLE app_user = 'admin'"),
 ('SET in a comment then SELECT', "/* SET VARIABLE x = 1 */ SELECT 1"),
 ('PREPARE', "PREPARE p AS SELECT * FROM base.trades"),
 ('EXECUTE', "EXECUTE p"),
 ('CALL pragma', "CALL pragma_database_list()"),
 ('ATTACH', "ATTACH 'other.db'"),
 ('COPY TO', "COPY (SELECT * FROM trades) TO '/tmp/x.csv'"),
 ('INSERT', "INSERT INTO base.trades VALUES (9,'X')"),
 ('CREATE TEMP MACRO', "CREATE TEMP MACRO app_user() AS 'admin'"),
 ('scalar current_setting', "SELECT current_setting('s3_secret_access_key')"),
 ('scalar getenv', "SELECT getenv('HOME')"),
 ('scalar read via function in select', "SELECT (SELECT content FROM read_text('/etc/hosts'))"),
 ('VALUES only', "VALUES (1),(2)"),
 ('catalog-qualified other db', "SELECT * FROM other.main.t"),
 ('quoted odd name', 'SELECT * FROM "sales"."trades"'),
 ('function in join condition', "SELECT * FROM trades t JOIN trades u ON upper(t.region) = u.region"),
]
for name, sql in CASES:
    a = analyse(sql)
    extra = '' if not a['parsed'] else f" tables={sorted(a['tables'])} tfuncs={sorted(a['tfuncs'])} funcs={sorted(f for f in a['funcs'] if f)[:6]}"
    print(f"{verdict(a):52s} | {name}{extra}")
