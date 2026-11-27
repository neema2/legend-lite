#!/usr/bin/env python3
"""The position matrix's report (tools/equality-measure/README.md): positions.py <out dir>.

Reads <out dir>/positions.tsv (bazel run //core:equality_measure -- --positions <out dir>) and writes
<out dir>/positions.md: for each comparison and database, whether every place the comparison was asked gave one
answer. Nothing is expected of an answer; the measure is whether the answers agree."""
import collections
import csv
import re
import sys


def value(row):
    """One outcome as 'T', 'F', or a short name of why there is no answer."""
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
        name = detail.split(':', 1)[0].strip() or kind
        return kind.lower() + ' ' + name
    if row['negated'] == 'true' and v in 'TF':
        v = 'F' if v == 'T' else 'T'
    return v


def main(out):
    rows = list(csv.DictReader(open(out + '/positions.tsv', encoding='utf-8'), delimiter='\t', quoting=csv.QUOTE_NONE))
    by = collections.OrderedDict()
    for r in rows:
        place = r['position'] + ('' if r['judge'] == '-' else ' (' + r['judge'].lower() + ')')
        by.setdefault((r['comparison'], r['left'], r['right']), collections.OrderedDict()) \
            .setdefault(r['database'], collections.OrderedDict())[place] = (value(r), r)
    dbs = sorted({r['database'] for r in rows})
    counts = {d: collections.Counter() for d in dbs}
    # a place the database cannot answer even for `1 == 1` is that database's gap, whatever is compared: set apart,
    # so a comparison is not counted for it
    simplest = next(iter(by.values()))
    missing = {d: {place: v for place, (v, _) in simplest.get(d, {}).items() if v not in ('T', 'F')} for d in dbs}
    lines = []
    for (name, left, right), per_db in by.items():
        entry = []
        for d in dbs:
            got = collections.OrderedDict((place, x) for place, x in per_db.get(d, {}).items()
                                          if place not in missing[d])
            values = {v for v, _ in got.values()}
            answers = values & {'T', 'F'}
            if len(answers) == 2:
                kind = 'CONTRADICTS'
            elif answers and len(values) > 1:
                kind = 'ANSWERED ONLY IN SOME PLACES'
            elif not answers and len(values) > 1:
                kind = 'refused everywhere, for different reasons'
            elif not answers:
                kind = 'refused everywhere'
            else:
                kind = 'one answer'
            counts[d][kind] += 1
            if kind in ('CONTRADICTS', 'ANSWERED ONLY IN SOME PLACES'):
                groups = collections.OrderedDict()
                for place, (v, _) in got.items():
                    groups.setdefault(v, []).append(place)
                entry.append('  - **%s: %s**' % (d, kind))
                for v, places in groups.items():
                    entry.append('    - `%s`: %s' % (v, ', '.join(places)))
        if entry:
            lines.append('- **%s**: `%s` == `%s`' % (name, left.replace('\\n', ' '), right.replace('\\n', ' ')))
            lines.extend(entry)
    # the same place on two databases giving two answers
    across = []
    for (name, left, right), per_db in by.items():
        places = collections.OrderedDict()
        for d, got in per_db.items():
            for place, (v, _) in got.items():
                if v in ('T', 'F') and place not in missing[d]:
                    places.setdefault(place, {})[d] = v
        differing = ['%s (%s)' % (p, ', '.join('%s %s' % kv for kv in sorted(a.items())))
                     for p, a in places.items() if len(set(a.values())) > 1]
        if differing:
            across.append('- **%s**: %s' % (name, '; '.join(differing)))
    with open(out + '/positions.md', 'w', encoding='utf-8') as w:
        w.write('# The position matrix: one comparison asked in every place\n\n')
        w.write('%d comparisons, %d places (asserts under both judges), %d runs.\n\n'
                % (len(by), len({(r['position'], r['judge']) for r in rows}), len(rows)))
        kinds = ['one answer', 'refused everywhere', 'refused everywhere, for different reasons',
                 'ANSWERED ONLY IN SOME PLACES', 'CONTRADICTS']
        w.write('| database | ' + ' | '.join(kinds) + ' |\n|---|' + '---|' * len(kinds) + '\n')
        for d in dbs:
            w.write('| %s | %s |\n' % (d, ' | '.join(str(counts[d][k]) for k in kinds)))
        w.write('\n## Places a database cannot answer even for `1 == 1` (left out of the counts above)\n\n')
        for d in dbs:
            w.write('- %s: %s\n' % (d, ', '.join('%s (%s)' % kv for kv in missing[d].items()) or 'none'))
        w.write('\n## Comparisons whose answer depends on where they stand\n\n' + '\n'.join(lines) + '\n')
        w.write('\n## One place, two databases, two answers\n\n' + ('\n'.join(across) if across else 'none') + '\n')
    print(open(out + '/positions.md', encoding='utf-8').read()[:1500])


if __name__ == '__main__':
    main(sys.argv[1])
