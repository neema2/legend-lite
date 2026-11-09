"""Parse Relational ###Data elements (schema.table: 'csv' + 'csv' ...;) and estimate their size as
CSV text, as rows/cells, as an INSERT ... VALUES text (cells as 'string literals'), and as the JSON of a
SetupStep.Rows (rows: [[cell,...],...]). Usage: python3 -I relational_csv.py <file.pure>..."""
import re, sys, json

def pure_string_concat(expr):
    # join single-quoted Pure literals, unescape \n, \', \\
    out = []
    for m in re.finditer(r"'((?:[^'\\]|\\.)*)'", expr, re.S):
        s = m.group(1)
        s = re.sub(r"\\(.)", lambda x: {'n': '\n', 't': '\t'}.get(x.group(1), x.group(1)), s)
        out.append(s)
    return ''.join(out)

def tables(text):
    for sec in re.split(r'(?m)^###', text):
        if not sec.startswith('Data'):
            continue
        for m in re.finditer(r'(?m)^\s*([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)\s*:\s*\n?(.*?);\s*$', sec, re.S):
            yield m.group(1), m.group(2), pure_string_concat(m.group(3))

def cells(line):
    return line.split(',')

for f in sys.argv[1:]:
    t = open(f, encoding='utf-8').read()
    n_tables = n_rows = n_cells = csv_bytes = insert_bytes = rows_json = 0
    widest = 0
    for schema, table, csv in tables(t):
        lines = [l for l in csv.split('\n') if l.strip()]
        if not lines:
            continue
        n_tables += 1
        csv_bytes += len(csv.encode())
        header, body = lines[0], lines[1:]
        n_rows += len(body)
        hdr = cells(header)
        widest = max(widest, len(body))
        ins = len(f'INSERT INTO "{schema}"."{table}" ({", ".join(hdr)}) VALUES ;')
        rj = []
        for r in body:
            cs = cells(r)
            n_cells += len(cs)
            ins += 4 + sum(len(c) + 2 + 2 for c in cs)
            rj.append([c if c else None for c in cs])
        insert_bytes += ins
        rows_json += len(json.dumps(rj))
    print(json.dumps({'file': f, 'tables': n_tables, 'rows': n_rows, 'cells': n_cells, 'csv_bytes': csv_bytes,
                      'insert_text_bytes_est': insert_bytes, 'rows_json_bytes_est': rows_json,
                      'largest_table_rows': widest}))
