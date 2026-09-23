// End-to-end: DataCube's own serialiser -> the WASM planner -> SQL,
// checked against the JVM over the same grammar.
//
// The unit tests exercise the seam with a fake module, and
// wasm/differential.mjs exercises the planner over a hand-written
// corpus. Neither proves the thing that actually matters for shipping:
// that the Pure DataCube EMITS -- from real snapshots, through
// serialize(), with pivots and tree levels and filters -- plans
// identically in the browser and on the server.
//
// The JVM's answers are a build output (//datacube:cube_jvm_answers,
// from emit.ts's queries); this plans the same queries through
// WasmPlanner -- the product's class, not a harness around the module --
// and compares.
//
//   bazel test //datacube:wasm_differential_test

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { it } from 'node:test';
import { fileURLToPath } from 'node:url';

import { WasmPlanner } from '../../src/wasm-planner.ts';
import { CASES, grammars, MODEL, RUNTIME } from './cases.ts';

// Beside this package in the runfiles: the module and its runtime
// (//wasm:planner), and the JVM's answers.
const MODULE_DIR = new URL('../../../wasm/planner/', import.meta.url).href;
const ANSWERS = fileURLToPath(new URL('../../cube_jvm_answers.txt', import.meta.url));

function blocks(text: string): Map<string, string> {
  const out = new Map<string, string>();
  const re = /<<<([^>]+)>>>\n([\s\S]*?)\n<<<END>>>\n/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(text)) !== null) out.set(m[1]!, m[2]!);
  return out;
}

// The JVM side keeps the exception class; the TS planner drops it by
// design. Compare on the parts both sides carry.
function norm(s: string): string {
  const [tag, ...rest] = s.split('\n');
  return tag === 'ERR' ? `ERR\n${rest.slice(1).join('\n')}` : s;
}

it('DataCube\'s own grammar plans the same in WASM and on the JVM', async () => {
  const jvm = blocks(readFileSync(ANSWERS, 'utf8'));
  const planner = new WasmPlanner({
    model: MODEL,
    runtime: RUNTIME,
    assetBaseUrl: MODULE_DIR,
    cache: false,
  });
  const queries = grammars();
  assert.equal(jvm.size, queries.length, 'the JVM answered a different query list');

  const differ: string[] = [];
  let planned = 0;
  for (const [i, { name, grammar }] of queries.entries()) {
    const c = CASES[i]!;
    const expected = jvm.get(name) ?? '<<missing>>';
    let actual: string;
    try {
      actual = `OK\n${await planner.plan(grammar, c.snapshot, c.scope)}`;
      planned++;
    } catch (e) {
      // planOrError's ERR text, reassembled the way the JVM half writes
      // it, so a refusal compares as a refusal rather than as a crash.
      actual = `ERR\n?\n${(e as Error).message}`;
    }
    if (norm(expected) !== norm(actual)) {
      differ.push(`${name}\n  grammar: ${grammar}\n  jvm : ${JSON.stringify(expected)}`
        + `\n  wasm: ${JSON.stringify(actual)}`);
    }
  }
  assert.deepEqual(differ, [], `${differ.length} of ${queries.length} differ:\n${differ.join('\n')}`);
  // Every shape here is one a user reaches, so every one must PLAN: two
  // builds that refuse alike agree perfectly while the cube is broken.
  assert.equal(planned, queries.length, 'a cube shape a user reaches was refused');
});
