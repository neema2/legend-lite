// What the stress run opens: every OFFERED sample, plus the shapes
// that are known to be broken.
//
// The offered samples come from `src/samples.ts` rather than being
// copied here, so the picker in the page and this suite can never
// disagree about what a sample is. An option that stopped working
// fails the stress run instead of disappointing whoever picked it,
// and `run-stress.mjs` enforces exactly that: zero breakages allowed
// among the offered set.
//
// The KNOWN_BROKEN entries are kept deliberately. Each one reproduces
// an open bug, so the suite keeps watching it; none of them is
// offered in the page, because half-working is worse than absent.

import { SAMPLES } from '../src/samples.ts';

export interface CorpusEntry {
  readonly name: string;
  readonly text: string;
  /** What this one is trying to break. */
  readonly targets: string;
  /** True for shapes with open bugs, excluded from the picker. */
  readonly knownBroken?: boolean;
}

const rows = (n: number, f: (i: number) => string) =>
  Array.from({ length: n }, (_, i) => f(i)).join('\n');

/** Small, because breadth of SHAPES matters here, not row counts. */
const STRESS_ROWS = 300;

const offered: CorpusEntry[] = SAMPLES.map((s) => ({
  name: `${s.id}.${s.format ?? 'csv'}`,
  targets: s.about,
  text: s.build(Math.min(s.defaultRows, STRESS_ROWS)),
}));

const knownBroken: CorpusEntry[] = [
  {
    name: 'header-quote.csv',
    knownBroken: true,
    targets: 'a double quote inside a header — inexpressible in the '
      + 'Database grammar, so ingest renames it',
    text: '"a""b",n\n' + rows(60, (i) => `v${i % 3},${i}`),
  },
  {
    name: 'unicode.csv',
    knownBroken: true,
    targets: 'OPEN BUG: a non-ASCII header selects fine but cannot be '
      + 'grouped or sorted — the model quotes it "régión" and the '
      + 'serialiser quotes it \'régión\', and only the ASCII case '
      + 'survives that',
    text: 'régión,製品,مبلغ,emoji\n'
      + rows(60, (i) => `${['EMEA', 'Ünïcodé', '北京'][i % 3]},`
        + `${['製品A', '製品B'][i % 2]},${i * 3},${['🎉', '🚀', '🧊'][i % 3]}`),
  },
  {
    name: 'booleans.csv',
    knownBroken: true,
    targets: 'OPEN BUG: a BOOLEAN column compared to a string literal '
      + 'type-checks and then fails in DuckDB; contains() on one '
      + 'lowers to list_contains(BOOLEAN, ...)',
    text: 'a,b,c,n\n'
      + rows(60, (i) => `${i % 2 === 0},${i % 2 ? 'TRUE' : 'FALSE'},`
        + `${i % 2},${i}`),
  },
  {
    name: 'empty-body.csv',
    knownBroken: true,
    targets: 'a header and no rows at all',
    text: 'a,b,c\n',
  },
  {
    name: 'one-column.csv',
    knownBroken: true,
    targets: 'nothing to aggregate and nothing to pivot on',
    text: 'only\n' + rows(60, (i) => `v${i % 5}`),
  },
];

export const CORPUS: CorpusEntry[] = [...offered, ...knownBroken];
