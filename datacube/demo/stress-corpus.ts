// CSVs chosen to break things.
//
// Each entry targets something specific: a type DuckDB sniffs
// differently, a header that is not an identifier, a value that is
// legal CSV and hostile SQL, a shape with no rows or no columns.
// Ordinary data is covered by the sample generator; this is the
// other half.

export interface CorpusEntry {
  readonly name: string;
  readonly text: string;
  /** What this one is trying to break. */
  readonly targets: string;
}

const rows = (n: number, f: (i: number) => string) =>
  Array.from({ length: n }, (_, i) => f(i)).join('\n');

export const CORPUS: CorpusEntry[] = [
  {
    name: 'types-all.csv',
    targets: 'every type DuckDB sniffs, in one table',
    text: 'name,n_int,n_big,n_dbl,n_dec,flag,d,ts\n'
      + rows(200, (i) =>
        `row${i % 7},${i},${9007199254740000 + i},${(i / 7).toFixed(4)},`
        + `${(i / 100).toFixed(2)},${i % 2 === 0},`
        + `2021-0${1 + (i % 9)}-0${1 + (i % 9)},`
        + `2021-0${1 + (i % 9)}-0${1 + (i % 9)} 1${i % 10}:30:00`),
  },
  {
    name: 'nulls.csv',
    targets: 'empty fields in every column, and an all-null column',
    text: 'k,v,always_null,sometimes\n'
      + rows(100, (i) => `${i % 5 === 0 ? '' : `k${i % 5}`},`
        + `${i % 3 === 0 ? '' : i},,${i % 2 ? '' : 'x'}`),
  },
  {
    name: 'unicode.csv',
    targets: 'emoji, CJK and RTL in values AND headers',
    text: 'régión,製品,مبلغ,emoji\n'
      + rows(60, (i) => `${['EMEA', 'Ünïcodé', '北京'][i % 3]},`
        + `${['製品A', '製品B'][i % 2]},${i * 3},${['🎉', '🚀', '🧊'][i % 3]}`),
  },
  {
    name: 'weird-headers.csv',
    targets: 'headers that are not identifiers: spaces, quotes, keywords',
    text: '"total pnl",select,from,"a""b",2024,'
      + '"x,y",  padded  ,VeryLongHeader'
      + 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n'
      + rows(40, (i) => `${i}.5,s${i % 3},f${i % 2},q${i % 4},`
        + `${2020 + (i % 4)},"c,${i}",p${i % 2},${i}`),
  },
  {
    name: 'header-spaces.csv',
    targets: 'headers needing quotes, but no quote INSIDE them',
    text: '"total pnl",select,from,2024,"x,y",  padded  \n'
      + rows(30, (i) => `${i}.5,s${i % 3},f${i % 2},${2020 + (i % 4)},`
        + `"c,${i}",p${i % 2}`),
  },
  {
    name: 'header-quote.csv',
    targets: 'a double quote INSIDE a header, on its own',
    text: '"a""b",n\n' + rows(30, (i) => `v${i % 3},${i}`),
  },
  {
    name: 'sql-injection.csv',
    targets: 'values that are hostile SQL and hostile Pure',
    text: 'label,amount\n'
      + `"Robert'); DROP TABLE x;--",1\n`
      + `"a' OR '1'='1",2\n`
      + `"back\\\\slash",3\n`
      + `"quote\\"\\"inside",4\n`
      + `"%wildcard_",5\n`
      + rows(30, (i) => `ok${i},${i}`),
  },
  {
    name: 'extremes.csv',
    targets: 'numbers at the edges of what a double and a bigint hold',
    text: 'k,big,small,neg,sci\n'
      + 'a,9223372036854775807,0.000000001,-0.0,1e300\n'
      + 'b,-9223372036854775808,1e-300,-99999999.99,-1e300\n'
      + 'c,0,0,0,0\n'
      + rows(40, (i) => `d${i % 4},${i},${i / 1000},-${i},${i}e2`),
  },
  {
    name: 'one-row.csv',
    targets: 'a single row: every aggregate over one value',
    text: 'a,b,c\nx,1,2.5\n',
  },
  {
    name: 'one-column.csv',
    targets: 'no measure to aggregate and nothing to pivot on',
    text: 'only\n' + rows(50, (i) => `v${i % 5}`),
  },
  {
    name: 'empty-body.csv',
    targets: 'a header and no rows at all',
    text: 'a,b,c\n',
  },
  {
    name: 'crlf.csv',
    targets: 'Windows line endings',
    text: 'k,v\r\n' + rows(50, (i) => `k${i % 4},${i}`).replace(/\n/g, '\r\n')
      + '\r\n',
  },
  {
    name: 'semicolons.csv',
    targets: 'a delimiter the sniffer has to find',
    text: 'k;v;w\n' + rows(50, (i) => `k${i % 4};${i};${i * 2}`),
  },
  {
    name: 'embedded-newlines.csv',
    targets: 'a quoted field containing a newline',
    text: 'note,n\n'
      + '"line one\nline two",1\n'
      + '"another\n\nwith a blank",2\n'
      + rows(20, (i) => `plain${i},${i + 3}`),
  },
  {
    name: 'mixed-column.csv',
    targets: 'a column that is numeric until it is not',
    text: 'k,maybe_num\n'
      + rows(40, (i) => `k${i % 3},${i}`) + '\nk0,not-a-number\n',
  },
  {
    name: 'wide.csv',
    targets: '60 columns: a wide select and a wide group',
    text: Array.from({ length: 60 }, (_, c) => `c${c}`).join(',') + '\n'
      + rows(30, (i) => Array.from({ length: 60 },
        (_, c) => (c % 3 === 0 ? `s${i % 4}` : String(i * c))).join(',')),
  },
  {
    name: 'high-cardinality.csv',
    targets: 'a pivot over many distinct values',
    text: 'grp,piv,val\n'
      + rows(2000, (i) => `g${i % 3},p${i % 250},${i}`),
  },
  {
    name: 'tall.csv',
    targets: '40k rows, to see the plan cost separate from the data cost',
    text: 'region,desk,amount\n'
      + rows(40000, (i) => `r${i % 5},d${i % 11},${(i % 997) / 7}`),
  },
  {
    name: 'spaces-and-case.csv',
    targets: 'leading/trailing spaces and mixed case, for the ci operators',
    text: 'label,n\n'
      + rows(40, (i) => `${['  padded  ', 'MiXeD', 'lower', 'UPPER'][i % 4]},${i}`),
  },
  {
    name: 'booleans.csv',
    targets: 'the spellings DuckDB does and does not read as boolean',
    text: 'a,b,c,n\n'
      + rows(40, (i) => `${i % 2 === 0},${i % 2 ? 'TRUE' : 'FALSE'},`
        + `${i % 2},${i}`),
  },
  {
    name: 'dates-various.csv',
    targets: 'date and timestamp spellings, including one that is text',
    text: 'iso,stamp,slashy,n\n'
      + rows(40, (i) => `2021-0${1 + (i % 9)}-15,`
        + `2021-0${1 + (i % 9)}-15T0${i % 9}:00:00,`
        + `0${1 + (i % 9)}/15/2021,${i}`),
  },
  {
    name: 'bom.csv',
    targets: 'a UTF-8 BOM, which can end up inside the first column name',
    text: '\uFEFFk,v\n' + rows(30, (i) => `k${i % 3},${i}`),
  },
];
