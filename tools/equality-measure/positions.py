#!/usr/bin/env python3
"""The position matrix's report (tools/equality-measure/README.md): positions.py <out dir>.

Reads <out dir>/positions.tsv and positions-store.tsv (bazel run //core:equality_measure -- --positions <out dir>)
and writes <out dir>/positions.md: for each comparison and database, whether every place the comparison was asked
gave one answer, and whether that answer is the one the equality rule gives."""
import collections
import csv
import os
import re
import sys

KINDS = ['one answer, the expected one', 'one answer, NOT the expected one', 'refused everywhere',
         'ANSWERED ONLY IN SOME PLACES', 'CONTRADICTS']


def read(path):
    return list(csv.DictReader(open(path, encoding='utf-8'), delimiter='\t', quoting=csv.QUOTE_NONE))


def no_answer(row):
    """A short name of why a run gave no answer."""
    name = row['answer'].split(':', 1)[0].strip() or row['outcome']
    return row['outcome'].lower() + ' ' + name


def value(row):
    """One outcome of the value matrix as 'T', 'F', or a short name of why there is no answer."""
    kind, detail = row['outcome'], row['answer']
    if kind == 'PASS':
        v = 'T'
    elif kind == 'FAIL':
        v = 'F'
    elif kind == 'ANSWER':
        text = detail.lower()
        if row['position'] == 'table-filter':
            # the filtered one-row table: a row kept, or none
            v = 'T' if re.search(r'\b1\b', re.sub(r'"[^"]*"\s*:', '', text)) else 'F'
        elif 'true' in text and 'false' not in text:
            v = 'T'
        elif 'false' in text and 'true' not in text:
            v = 'F'
        else:
            return 'other answer'
    else:
        return no_answer(row)
    if row['negated'] == 'true':
        v = 'F' if v == 'T' else 'T'
    return v


ALL_ROWS = frozenset(range(1, 6))


def kept(row):
    """One outcome of the store matrix as the ids of the rows the comparison is true for (text '1,5'; '' for none),
    or a short name of why there is no answer."""
    if row['outcome'] != 'ANSWER':
        return no_answer(row)
    text = row['answer']
    if row['position'].startswith('value'):
        ids = {int(i) for i, same in re.findall(r'"id"\s*:\s*(\d+)\s*,\s*"same"\s*:\s*(true|false|null)', text)
               if same == 'true'}
        if set(int(i) for i in re.findall(r'"id"\s*:\s*(\d+)', text)) != set(ALL_ROWS):
            return 'other answer'
    else:
        ids = {int(i) for i in re.findall(r'"value"\s*:\s*(\d+)', text)}
    if row['negated'] == 'true':
        ids = set(ALL_ROWS) - ids
    return ','.join(str(i) for i in sorted(ids))


def answered(v, store):
    return bool(re.fullmatch(r'[\d,]*', v)) if store else v in ('T', 'F')


def right(v, expected, store):
    """Whether an answer is the expected one. N: never true (a refusal is right; an answer must be false / no row)."""
    if expected == '?':
        return True
    if expected == 'N':
        return v == ('' if store else 'F')
    return v == expected


def classify(rows, value_of, store):
    """Per comparison and database: its kind and the places grouped by what they said."""
    by = collections.OrderedDict()
    for r in rows:
        place = r['position'] + ('' if r.get('judge', '-') == '-' else ' (' + r['judge'].lower() + ')')
        by.setdefault((r['comparison'], r['expected'], r['left'], r['right']), collections.OrderedDict()) \
            .setdefault(r['database'], collections.OrderedDict())[place] = value_of(r)
    dbs = sorted({r['database'] for r in rows})
    # a place the database cannot answer even for the first, simplest comparison is that database's own gap
    simplest = next(iter(by.values()))
    missing = {d: {p: v for p, v in simplest.get(d, {}).items() if not answered(v, store)} for d in dbs}
    counts = {d: collections.Counter() for d in dbs}
    lines, asked = [], []
    for (name, expected, left, right_side), per_db in by.items():
        entry = []
        for d in dbs:
            got = collections.OrderedDict((p, v) for p, v in per_db.get(d, {}).items() if p not in missing[d])
            answers = {v for v in got.values() if answered(v, store)}
            refusals = {v for v in got.values() if not answered(v, store)}
            wrong = [p for p, v in got.items() if answered(v, store) and not right(v, expected, store)]
            if len(answers) > 1:
                kind = 'CONTRADICTS'
            elif answers and refusals and expected != 'N':
                kind = 'ANSWERED ONLY IN SOME PLACES'
            elif not answers:
                kind = 'one answer, the expected one' if expected == 'N' else 'refused everywhere'
            elif wrong:
                kind = 'one answer, NOT the expected one'
            else:
                kind = 'one answer, the expected one'
            counts[d][kind] += 1
            if kind in ('CONTRADICTS', 'ANSWERED ONLY IN SOME PLACES', 'one answer, NOT the expected one'):
                groups = collections.OrderedDict()
                for p, v in got.items():
                    groups.setdefault(v, []).append(p)
                entry.append('  - **%s: %s**' % (d, kind))
                for v, places in groups.items():
                    shown = ', '.join(places) if len(places) <= 6 else '%d places (%s, ...)' % (
                        len(places), ', '.join(places[:2]))
                    entry.append('    - `%s`: %s' % (v if v != '' else 'no row', shown))
        if expected == '?':
            asked.append('- **%s**: `%s` == `%s`' % (name, left.replace('\\n', ' '), right_side.replace('\\n', ' ')))
        if entry:
            lines.append('- **%s** (expected `%s`): `%s` == `%s`'
                         % (name, expected, left.replace('\\n', ' '), right_side.replace('\\n', ' ')))
            lines.extend(entry)
    return dbs, counts, missing, lines, asked, len(by)


def section(w, title, rows, value_of, store):
    dbs, counts, missing, lines, asked, n = classify(rows, value_of, store)
    w.write('## %s\n\n%d comparisons, %d runs.\n\n' % (title, n, len(rows)))
    w.write('| database | ' + ' | '.join(KINDS) + ' |\n|---|' + '---|' * len(KINDS) + '\n')
    for d in dbs:
        w.write('| %s | %s |\n' % (d, ' | '.join(str(counts[d][k]) for k in KINDS)))
    w.write('\nPlaces a database cannot answer even for the simplest comparison (left out of the counts):\n')
    for d in dbs:
        w.write('- %s: %s\n' % (d, ', '.join('%s (%s)' % kv for kv in missing[d].items()) or 'none'))
    if asked:
        w.write('\nNot plain from the equality rule, for the user (counted as expected whatever they answer):\n'
                + '\n'.join(asked) + '\n')
    w.write('\n### Comparisons that are not one expected answer everywhere\n\n' + ('\n'.join(lines) or 'none') + '\n\n')
    return {d: {k: counts[d][k] for k in KINDS} for d in dbs}


def main(out):
    with open(out + '/positions.md', 'w', encoding='utf-8') as w:
        w.write('# The position matrix: one comparison asked in every place\n\n')
        totals = section(w, 'Values: literals and computed values', read(out + '/positions.tsv'), value, False)
        if os.path.exists(out + '/positions-store.tsv'):
            store = section(w, "A store's rows: a comparison that reads the row it stands in",
                            read(out + '/positions-store.tsv'), kept, True)
            for d, c in store.items():
                for k, v in c.items():
                    totals.setdefault(d, {}).setdefault(k, 0)
                    totals[d][k] += v
    # the counts a pin reads: per database, the comparisons of each kind
    with open(out + '/positions-counts.tsv', 'w', encoding='utf-8') as w:
        w.write('database\t' + '\t'.join(KINDS) + '\n')
        for d in sorted(totals):
            w.write(d + '\t' + '\t'.join(str(totals[d][k]) for k in KINDS) + '\n')
    print(open(out + '/positions-counts.tsv', encoding='utf-8').read())


if __name__ == '__main__':
    main(sys.argv[1])
