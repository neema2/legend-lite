#!/usr/bin/env python3
"""The position matrix's report (tools/equality-measure/README.md): positions.py <out dir>.

Reads <out dir>/positions.tsv and positions-store.tsv (bazel run //core:equality_measure -- --positions <out dir>)
and writes <out dir>/positions.md and positions-counts.tsv: for each comparison and database, whether every place
the comparison was asked gave the answer the equality rule gives it.

A comparison's expected answer: T or F (values); the ids of the rows it is true for (a store's rows); N, never true:
a compile error (ModelException) where the two types are written, and false where they are known only when the query
runs (a function whose parameters are Any); ?, not plain from the rule: any one answer, for the user to rule."""
import collections
import csv
import os
import re
import sys

KINDS = ['right everywhere', 'refused by name everywhere', 'ANSWERED ONLY IN SOME PLACES', 'FAILS SOMEWHERE',
         'A WRONG ANSWER SOMEWHERE', 'CONTRADICTS']
# a refusal that names what is not supported, or a compile error; anything else that is no answer is a failure
BY_NAME = ('refused ModelException', 'refused DialectCapability', 'refused NotImplementedException',
           'refused TypeInferenceException', 'unjudged')
RUN_TIME_PLACES = ('function-any', 'filter-function-any')
ALL_ROWS = frozenset(range(1, 6))


def read(path):
    return list(csv.DictReader(open(path, encoding='utf-8'), delimiter='\t', quoting=csv.QUOTE_NONE))


def no_answer(row):
    """A short name of why a run gave no answer."""
    if row['outcome'] == 'UNJUDGED':
        return 'unjudged'
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
        elif 'true' in text and 'false' not in text and 'null' not in text:
            v = 'T'
        elif 'false' in text and 'true' not in text and 'null' not in text:
            v = 'F'
        else:
            return 'other answer'
    else:
        return no_answer(row)
    if row['negated'] == 'true':
        v = 'F' if v == 'T' else 'T'
    return v


def kept(row):
    """One outcome of the store matrix as the ids of the rows the comparison is true for (text '1,5'; '' for none),
    or a short name of why there is no answer. A kept value that is neither true nor false (a NULL) is no answer:
    the rule's comparison is two-valued."""
    if row['outcome'] != 'ANSWER':
        return no_answer(row)
    text = row['answer']
    if row['position'].startswith('value'):
        cells = re.findall(r'"id"\s*:\s*(\d+)\s*,\s*"same"\s*:\s*(true|false|null)', text)
        if {int(i) for i, _ in cells} != set(ALL_ROWS) or any(same == 'null' for _, same in cells):
            return 'other answer'
        ids = {int(i) for i, same in cells if same == 'true'}
    else:
        ids = {int(i) for i in re.findall(r'"id"\s*:\s*(\d+)', text)}
    if row['negated'] == 'true':
        ids = set(ALL_ROWS) - ids
    return ','.join(str(i) for i in sorted(ids))


def is_answer(v, store):
    return bool(re.fullmatch(r'[\d,]*', v)) if store else v in ('T', 'F')


def judge(v, expected, place, store):
    """What one place's outcome is against the expected answer: 'right', 'wrong', 'refused' (by name) or 'failed'."""
    if '@' in expected:
        # a@b: a where the comparison is written, b inside a function whose parameters are Any
        written, at_run = expected.split('@', 1)
        expected = at_run if place.split(' ')[0] in RUN_TIME_PLACES else written
    if expected == 'N':
        if place.split(' ')[0] in RUN_TIME_PLACES:
            want = '' if store else 'F'
            return 'right' if v == want else 'wrong' if is_answer(v, store) else 'failed'
        return 'right' if v == 'refused ModelException' else 'wrong' if is_answer(v, store) else 'failed'
    if is_answer(v, store):
        return 'right' if expected == '?' or v == expected else 'wrong'
    return 'refused' if v in BY_NAME else 'failed'


def classify(rows, value_of, store):
    by = collections.OrderedDict()
    for r in rows:
        place = r['position'] + ('' if r.get('judge', '-') == '-' else ' (' + r['judge'].lower() + ')')
        by.setdefault((r['comparison'], r['expected'], r['left'], r['right']), collections.OrderedDict()) \
            .setdefault(r['database'], collections.OrderedDict())[place] = value_of(r)
    dbs = sorted({r['database'] for r in rows})
    places = list(collections.OrderedDict((p, 0) for per_db in by.values() for got in per_db.values() for p in got))
    # a place the database cannot answer even for the first, simplest comparison is that database's own gap; the
    # count of such places is pinned with the rest, so it cannot grow quietly
    simplest = next(iter(by.values()))
    missing = {d: {p: v for p, v in simplest.get(d, {}).items() if not is_answer(v, store)} for d in dbs}
    counts = {d: collections.Counter() for d in dbs}
    lines, asked = [], []
    for (name, expected, left, right_side), per_db in by.items():
        entry = []
        for d in dbs:
            got = collections.OrderedDict((p, v) for p, v in per_db.get(d, {}).items() if p not in missing[d])
            verdicts = {p: judge(v, expected, p, store) for p, v in got.items()}
            answers = {v for v in got.values() if is_answer(v, store)}
            absent = [p for p in places if p not in per_db.get(d, {})]
            if len(answers) > 1 and expected != 'N':
                kind = 'CONTRADICTS'
            elif 'wrong' in verdicts.values():
                kind = 'A WRONG ANSWER SOMEWHERE'
            elif 'failed' in verdicts.values() or absent:
                kind = 'FAILS SOMEWHERE'
            elif 'refused' in verdicts.values():
                kind = 'ANSWERED ONLY IN SOME PLACES' if 'right' in verdicts.values() else 'refused by name everywhere'
            else:
                kind = 'right everywhere'
            counts[d][kind] += 1
            if kind != 'right everywhere':
                groups = collections.OrderedDict()
                for p, v in got.items():
                    groups.setdefault((v if v != '' else 'no row') + ' [' + verdicts[p] + ']', []).append(p)
                entry.append('  - **%s: %s**' % (d, kind))
                for v, ps in groups.items():
                    shown = ', '.join(ps) if len(ps) <= 6 else '%d places (%s, ...)' % (len(ps), ', '.join(ps[:2]))
                    entry.append('    - `%s`: %s' % (v, shown))
                if absent:
                    entry.append('    - never run: %s' % ', '.join(absent))
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
    w.write('| database | ' + ' | '.join(KINDS) + ' | places left out |\n|---|' + '---|' * (len(KINDS) + 1) + '\n')
    for d in dbs:
        w.write('| %s | %s | %d |\n' % (d, ' | '.join(str(counts[d][k]) for k in KINDS), len(missing[d])))
    w.write('\nPlaces a database cannot answer even for the simplest comparison (left out of the counts):\n')
    for d in dbs:
        w.write('- %s: %s\n' % (d, ', '.join('%s (%s)' % kv for kv in missing[d].items()) or 'none'))
    if asked:
        w.write('\nNot plain from the equality rule, for the user (any one answer counts as right):\n'
                + '\n'.join(asked) + '\n')
    w.write('\n### Comparisons that are not right everywhere\n\n' + ('\n'.join(lines) or 'none') + '\n\n')
    out = {d: {k: counts[d][k] for k in KINDS} for d in dbs}
    for d in dbs:
        out[d]['places left out'] = len(missing[d])
    return out


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
    # the counts a pin reads. 'right everywhere' may only rise; every other column may only fall
    columns = KINDS + ['places left out']
    with open(out + '/positions-counts.tsv', 'w', encoding='utf-8') as w:
        w.write('database\t' + '\t'.join(columns) + '\n')
        for d in sorted(totals):
            w.write(d + '\t' + '\t'.join(str(totals[d][k]) for k in columns) + '\n')
    print(open(out + '/positions-counts.tsv', encoding='utf-8').read())


if __name__ == '__main__':
    main(sys.argv[1])
