// Write a sample CSV, for when you want one on disk.
//
// The page has a button for the same thing; this is for the command
// line, so `bazel run //datacube:serve -- --data` has something to point at without a
// round trip through a browser download.
//
//   bazel run //datacube:make_sample                       -> sample-trades.csv, 5000 rows
//   bazel run //datacube:make_sample -- --rows 100000 --out /tmp/big.csv

import { writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';

import { sampleCsv, SAMPLE_COLUMNS } from '../src/samples.ts';

const args = process.argv.slice(2);
const flag = (name, fallback) => {
  const i = args.indexOf(`--${name}`);
  return i < 0 || args[i + 1] === undefined ? fallback : args[i + 1];
};

const rows = Number(flag('rows', '5000'));
if (!Number.isFinite(rows) || rows < 1) {
  console.error(`--rows must be a positive number, got ${flag('rows', '')}`);
  process.exit(2);
}
// `bazel run` starts this in its runfiles; anything written for a
// person goes to the directory they ran it from.
const CALLER = process.env.BUILD_WORKING_DIRECTORY ?? process.cwd();
const out = resolve(CALLER, flag('out', 'sample-trades.csv'));
const seed = Number(flag('seed', '20260920'));

const text = sampleCsv({ rows, seed });
await writeFile(out, text, 'utf8');

console.log(`${out}`);
console.log(`  ${rows.toLocaleString()} rows, `
  + `${SAMPLE_COLUMNS.length} columns, `
  + `${(Buffer.byteLength(text) / 1024).toFixed(0)} KB`);
console.log(`  ${SAMPLE_COLUMNS.join(', ')}`);
console.log('\nOpen it with the "Open a file" control, or:');
console.log(`  bazel run //datacube:serve -- --data ${out}`);
