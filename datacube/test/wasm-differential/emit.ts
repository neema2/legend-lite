// A BUILD ACTION (datacube/BUILD.bazel, cube_queries): writes the model
// and DataCube's serialised queries in the form //wasm:jvm_main reads —
// the model file, and one `name<TAB>query` line per case.
//
// Usage: node --experimental-strip-types emit.ts <model-out> <queries-out>

import { writeFileSync } from 'node:fs';

import { grammars, MODEL } from './cases.ts';

const [modelOut, queriesOut] = process.argv.slice(2);
if (modelOut === undefined || queriesOut === undefined) {
  throw new Error('usage: emit.ts <model-out> <queries-out>');
}
const lines = grammars().map(({ name, grammar }) => {
  // The TSV escapes nothing: a tab or newline in a query would split it.
  if (/[\t\n]/.test(grammar)) {
    throw new Error(`${name}: the serialised grammar holds a tab or newline`);
  }
  return `${name}\t${grammar}\n`;
});
writeFileSync(modelOut, MODEL);
writeFileSync(queriesOut, lines.join(''));
