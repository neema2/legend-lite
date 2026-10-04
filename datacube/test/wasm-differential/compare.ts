// End-to-end: DataCube's own query builder -> the WASM planner -> SQL,
// checked against the JVM over the same query.
//
// The unit tests exercise the seam with a fake module, and
// wasm/differential.mjs exercises the planner over a hand-written
// corpus. Neither proves the thing that actually matters for shipping:
// that the queries DataCube SENDS -- from real snapshots, through
// query.ts, with pivots and tree levels and filters -- plan
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

import { relationColumns } from '../../../engine-client/src/relation-type.ts';
import { WasmPlanner } from '../../src/wasm-planner.ts';
import { toJson } from '../../../pure-protocol/src/index.ts';
import { MODEL, queries, RUNTIME } from './cases.ts';

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
// design. Compare on the parts both sides carry. An OK answer is the plan
// -- its SQL and the compiler's result type -- read through the product's
// own reader, so the types are held to the JVM too.
function norm(s: string): string {
  const [tag, ...rest] = s.split('\n');
  if (tag === 'ERR') return `ERR\n${rest.slice(1).join('\n')}`;
  const body = JSON.parse(rest.join('\n')) as { sql: string; type: unknown };
  return `OK\n${JSON.stringify({ sql: body.sql, columns: relationColumns(body.type) })}`;
}

it('DataCube\'s own queries plan the same in WASM and on the JVM', async () => {
  const jvm = blocks(readFileSync(ANSWERS, 'utf8'));
  const planner = new WasmPlanner({
    model: MODEL,
    runtime: RUNTIME,
    assetBaseUrl: MODULE_DIR,
    cache: false,
  });
  const cases = queries();
  assert.equal(jvm.size, cases.length, 'the JVM answered a different query list');

  const differ: string[] = [];
  let planned = 0;
  for (const { name, query } of cases) {
    const expected = jvm.get(name) ?? '<<missing>>';
    let actual: string;
    try {
      const plan = await planner.plan(query);
      actual = `OK\n${JSON.stringify({ sql: plan.sql, columns: plan.columns })}`;
      planned++;
    } catch (e) {
      // planJsonOrError's ERR text, reassembled the way the JVM half writes
      // it, so a refusal compares as a refusal rather than as a crash.
      actual = `ERR\n?\n${(e as Error).message}`;
    }
    if (norm(expected) !== (actual.startsWith('OK\n') ? actual : norm(actual))) {
      differ.push(`${name}\n  query: ${toJson(query)}\n  jvm : ${JSON.stringify(expected)}`
        + `\n  wasm: ${JSON.stringify(actual)}`);
    }
  }
  assert.deepEqual(differ, [], `${differ.length} of ${cases.length} differ:\n${differ.join('\n')}`);
  // Every shape here is one a user reaches, so every one must PLAN: two
  // builds that refuse alike agree perfectly while the cube is broken.
  assert.equal(planned, cases.length, 'a cube shape a user reaches was refused');
});

// Byte for byte: the tree DataCube builds, printed by the compiler and parsed back, is the same JSON
// string -- numbers included (pure-protocol writes them as the wire does, spelling.ts).
it('the compiler prints every query so that it parses back to the same tree, byte for byte, in both styles', async () => {
  const planner = new WasmPlanner({
    model: MODEL,
    runtime: RUNTIME,
    assetBaseUrl: MODULE_DIR,
    cache: false,
  });
  const differ: string[] = [];
  for (const { name, query } of queries()) {
    for (const style of ['STANDARD', 'PRETTY'] as const) {
      const text = await planner.compose(query, style);
      const again = await planner.parse(text);
      if (toJson(again) !== toJson(query)) {
        differ.push(`${name} (${style}): the print does not parse back to the same tree\n  print: ${text}`
          + `\n  ours:  ${toJson(query)}\n  back:  ${toJson(again)}`);
      }
    }
  }
  assert.deepEqual(differ, [], differ.join('\n'));
});
