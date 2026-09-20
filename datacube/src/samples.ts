// Sample files to open, each one awkward in a different way.
//
// ONE definition, two consumers: the picker in the page offers these,
// and `npm run stress` runs every cube operation against every one of
// them. That coupling is the point -- an option that stopped working
// would fail the stress run rather than disappoint whoever picked it.
//
// The set here is deliberately the WORKING set. Shapes that expose
// open bugs (a quote inside a header, non-ASCII header names, a
// boolean compared to a string literal) live in the stress corpus
// only, so the suite keeps watching them without the page offering
// something that half-works.

/** Deterministic, so the same choice gives the same file. */
function rng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** RFC 4180: quote when the value holds a comma, quote or newline. */
export function csvCell(value: string): string {
  return /[",\n\r]/.test(value)
    ? `"${value.replace(/"/g, '""')}"`
    : value;
}

function lines(n: number, f: (i: number) => string): string {
  const out: string[] = new Array(n);
  for (let i = 0; i < n; i++) out[i] = f(i);
  return out.join('\n');
}

export interface Sample {
  readonly id: string;
  /** Shown in the picker. */
  readonly label: string;
  /** What makes this one worth opening. */
  readonly about: string;
  /** Rows to generate when the caller has no preference. */
  readonly defaultRows: number;
  build(rows: number, seed?: number): string;
}

const REGIONS = ['EMEA', 'APAC', 'AMER'];
const DESKS = ['Rates', 'Credit', 'FX', 'Equities', 'Commodities'];
const BOOKS = [
  'Alpha Fund', 'Smith, "Big" Fund', "O'Brien Holdings", 'Beta Fund',
];

export const SAMPLE_COLUMNS = [
  'trade_id', 'trade_date', 'booked_at', 'region', 'desk', 'book',
  'year', 'quarter', 'notional', 'pnl', 'quantity', 'settled',
] as const;

/** The default: a realistic trade blotter across every column type. */
export function sampleCsv(
  options: { rows?: number; seed?: number } = {},
): string {
  const rows = Math.max(1, options.rows ?? 5000);
  const rand = rng(options.seed ?? 20260920);
  const out: string[] = [SAMPLE_COLUMNS.join(',')];
  for (let i = 0; i < rows; i++) {
    const year = 2021 + (i % 5);
    const month = 1 + Math.floor(rand() * 12);
    const day = 1 + Math.floor(rand() * 28);
    const mm = String(month).padStart(2, '0');
    const dd = String(day).padStart(2, '0');
    out.push([
      String(100000 + i),
      `${year}-${mm}-${dd}`,
      `${year}-${mm}-${dd} ${String(Math.floor(rand() * 24)).padStart(2, '0')}`
        + `:${String(Math.floor(rand() * 60)).padStart(2, '0')}:00`,
      REGIONS[i % REGIONS.length]!,
      DESKS[Math.floor(rand() * DESKS.length)]!,
      BOOKS[Math.floor(rand() * BOOKS.length)]!,
      String(year),
      `Q${1 + Math.floor((month - 1) / 3)}`,
      (rand() * 1_000_000).toFixed(2),
      (rand() * 200_000 - 100_000).toFixed(2),
      String(1 + Math.floor(rand() * 500)),
      rand() < 0.8 ? 'true' : 'false',
    ].map(csvCell).join(','));
  }
  return `${out.join('\n')}\n`;
}

export const SAMPLES: readonly Sample[] = [
  {
    id: 'trades',
    label: 'Trades — a bit of everything',
    about: 'Dates, a timestamp, a boolean, key-like integers, measures, '
      + 'and a book name containing a comma and a quoted phrase.',
    defaultRows: 5000,
    build: (rows, seed) =>
      sampleCsv(seed === undefined ? { rows } : { rows, seed }),
  },
  {
    id: 'types',
    label: 'Every column type',
    about: 'One column per type DuckDB sniffs: integer, bigint, double, '
      + 'decimal, boolean, date and timestamp.',
    defaultRows: 2000,
    build: (rows) => 'name,n_int,n_big,n_dbl,n_dec,flag,d,ts\n'
      + lines(rows, (i) =>
        `row${i % 7},${i},${9007199254740000 + i},${(i / 7).toFixed(4)},`
        + `${(i / 100).toFixed(2)},${i % 2 === 0},`
        + `2021-${String(1 + (i % 12)).padStart(2, '0')}-`
        + `${String(1 + (i % 28)).padStart(2, '0')},`
        + `2021-${String(1 + (i % 12)).padStart(2, '0')}-`
        + `${String(1 + (i % 28)).padStart(2, '0')} `
        + `${String(i % 24).padStart(2, '0')}:30:00`) + '\n',
  },
  {
    id: 'nulls',
    label: 'Missing values everywhere',
    about: 'Empty fields in every column, plus one column that is '
      + 'entirely null. Aggregates have to skip them.',
    defaultRows: 3000,
    build: (rows) => 'k,v,always_null,sometimes\n'
      + lines(rows, (i) => `${i % 5 === 0 ? '' : `k${i % 5}`},`
        + `${i % 3 === 0 ? '' : i},,${i % 2 ? '' : 'x'}`) + '\n',
  },
  {
    id: 'dates',
    label: 'Dates and timestamps',
    about: 'ISO dates, ISO timestamps, and a slash-formatted column '
      + 'DuckDB reads as text rather than a date.',
    defaultRows: 3000,
    build: (rows) => 'iso,stamp,slashy,n\n'
      + lines(rows, (i) => {
        const mm = String(1 + (i % 12)).padStart(2, '0');
        return `2021-${mm}-15,2021-${mm}-15T${String(i % 24)
          .padStart(2, '0')}:00:00,${mm}/15/2021,${i}`;
      }) + '\n',
  },
  {
    id: 'wide',
    label: 'Wide — 60 columns',
    about: 'A wide select and a wide group-by, for the column panel '
      + 'and the horizontal scroll.',
    defaultRows: 2000,
    build: (rows) => Array.from({ length: 60 }, (_, c) => `c${c}`).join(',')
      + '\n' + lines(rows, (i) => Array.from({ length: 60 },
        (_, c) => (c % 3 === 0 ? `s${i % 4}` : String(i * c))).join(',')) + '\n',
  },
  {
    id: 'pivot',
    label: 'Wide pivot — 250 distinct values',
    about: 'A column with 250 distinct values, to pivot on and watch '
      + 'the header build.',
    defaultRows: 20000,
    build: (rows) => 'grp,piv,val\n'
      + lines(rows, (i) => `g${i % 3},p${i % 250},${i}`) + '\n',
  },
  {
    id: 'tall',
    label: 'Tall — lots of rows',
    about: 'Few columns, many rows: the data cost rather than the '
      + 'planning cost.',
    defaultRows: 200000,
    build: (rows) => 'region,desk,amount\n'
      + lines(rows, (i) => `r${i % 5},d${i % 11},${(i % 997) / 7}`) + '\n',
  },
  {
    id: 'awkward-headers',
    label: 'Awkward column names',
    about: 'Headers with spaces, commas, SQL keywords and a leading '
      + 'digit. All of these quote correctly.',
    defaultRows: 2000,
    build: (rows) => '"total pnl",select,from,2024,"x,y",  padded  \n'
      + lines(rows, (i) => `${i}.5,s${i % 3},f${i % 2},${2020 + (i % 4)},`
        + `"c,${i}",p${i % 2}`) + '\n',
  },
  {
    id: 'hostile-values',
    label: 'Hostile values',
    about: "Values that look like SQL and like Pure: a DROP TABLE, an "
      + "OR '1'='1', backslashes, quotes and LIKE wildcards.",
    defaultRows: 2000,
    build: (rows) => 'label,amount\n'
      + `"Robert'); DROP TABLE x;--",1\n`
      + `"a' OR '1'='1",2\n`
      + `"back\\slash",3\n`
      + `"quote""inside",4\n`
      + `"%wildcard_",5\n`
      + lines(Math.max(0, rows - 5), (i) => `ok${i},${i}`) + '\n',
  },
  {
    id: 'extremes',
    label: 'Numbers at the edges',
    about: 'Int64 limits, 1e300, 1e-300, negative zero. Some arithmetic '
      + 'on these legitimately overflows.',
    defaultRows: 2000,
    build: (rows) => 'k,big,small,neg,sci\n'
      + 'a,9223372036854775807,0.000000001,-0.0,1e300\n'
      + 'b,-9223372036854775808,1e-300,-99999999.99,-1e300\n'
      + 'c,0,0,0,0\n'
      + lines(Math.max(0, rows - 3),
        (i) => `d${i % 4},${i},${i / 1000},-${i},${i}e2`) + '\n',
  },
  {
    id: 'quoting',
    label: 'Quoting and whitespace',
    about: 'Embedded newlines inside quoted fields, padded values, and '
      + 'mixed case for the case-insensitive filters.',
    defaultRows: 2000,
    build: (rows) => 'note,label,n\n'
      + '"line one\nline two",MiXeD,1\n'
      + '"another\n\nwith a blank",  padded  ,2\n'
      + lines(Math.max(0, rows - 2), (i) =>
        `plain${i},${['  padded  ', 'MiXeD', 'lower', 'UPPER'][i % 4]},`
        + `${i + 3}`) + '\n',
  },
  {
    id: 'semicolons',
    label: 'Semicolon-delimited',
    about: 'Not a comma in sight. DuckDB has to sniff the delimiter.',
    defaultRows: 2000,
    build: (rows) => 'k;v;w\n'
      + lines(rows, (i) => `k${i % 4};${i};${i * 2}`) + '\n',
  },
  {
    id: 'crlf-bom',
    label: 'Windows line endings and a BOM',
    about: 'CRLF throughout and a UTF-8 byte-order mark, which can end '
      + 'up inside the first column name.',
    defaultRows: 2000,
    build: (rows) => '﻿k,v\r\n'
      + lines(rows, (i) => `k${i % 4},${i}`).replace(/\n/g, '\r\n') + '\r\n',
  },
  {
    id: 'mixed-column',
    label: 'A column that changes its mind',
    about: 'Numeric for most rows and then not, so DuckDB widens the '
      + 'whole column to text.',
    defaultRows: 2000,
    build: (rows) => 'k,maybe_num\n'
      + lines(Math.max(0, rows - 1), (i) => `k${i % 3},${i}`)
      + '\nk0,not-a-number\n',
  },
  {
    id: 'one-row',
    label: 'A single row',
    about: 'Every aggregate over exactly one value, and a grid with '
      + 'nothing to scroll.',
    defaultRows: 1,
    build: () => 'a,b,c\nx,1,2.5\n',
  },
];

export function sampleById(id: string): Sample | undefined {
  return SAMPLES.find((s) => s.id === id);
}
