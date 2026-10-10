/**
 * THE CONFORMANCE DIFFERENTIAL (docs/WEB_IMAGE_SPIKE_2026_10_10.md, step 1): TeaVM's class library against the JDK's,
 * over what lite's browser code calls. The JVM half (//wasm:conformance_jvm, a build action) wrote every family's
 * answers; the module (//wasm:conformance_module, TeaVM's compile of the same families) answers the same questions
 * here, and each line is compared. A difference is one of three kinds: the answer differs; both threw the same class
 * and only the message differs; or the INPUT differs (the case generator itself ran differently, which no case should).
 *
 *   bazel test //wasm:conformance_test
 *
 * Every difference goes to the test's undeclared outputs (conformance-diffs.tsv: family, line, JVM, module).
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { runfileDirUrl, runfileFromEnv } from '../tools/js/runfiles.mts';

function blocks(text) {
  const map = new Map();
  const re = /<<<([^>]+)>>>\n([\s\S]*?)<<<END>>>\n/g;
  let m;
  while ((m = re.exec(text)) !== null) map.set(m[1], m[2]);
  return map;
}

const lines = (text) => text.split('\n').filter((l, i, all) => i < all.length - 1 || l !== '');
const input = (line) => line.slice(0, line.indexOf('\t'));
const answer = (line) => line.slice(line.indexOf('\t') + 1);
const thrown = (a) => (a.startsWith('ERR ') ? a.slice(0, a.indexOf(':')) : undefined);

test("TeaVM's class library answers as the JDK does", async () => {
  const jvm = blocks(readFileSync(runfileFromEnv('CONFORMANCE_JVM'), 'utf8'));
  const dir = runfileDirUrl('CONFORMANCE_MODULE');
  const { load } = await import(new URL('wasm-gc-module-runtime.js', dir).href);
  // TeaVM's loader wants a filesystem path under Node (a file: URL fails with an ENOENT)
  const teavm = await load(fileURLToPath(new URL('classes.wasm', dir)), {
    stackDeobfuscator: { enabled: false },
    installImports(o) {
      o.teavmConsole = o.teavmConsole || {};
      o.teavmConsole.putcharStdout = () => {};
      o.teavmConsole.putcharStderr = () => {};
    },
  });
  const families = teavm.exports.families().split('\n');
  assert.deepEqual(families, [...jvm.keys()], 'the module and the JVM name the same families');

  const rows = [];
  const diffs = [];
  for (const family of families) {
    const want = lines(jvm.get(family));
    const started = performance.now();
    let got;
    try {
      got = lines(teavm.exports.answers(family));
    } catch (e) {
      got = [];
      diffs.push([family, 0, 'the family', `THREW-THROUGH-JS ${e && e.message}`]);
    }
    const ms = Math.round(performance.now() - started);
    const c = { family, cases: want.length, equal: 0, answer: 0, message: 0, input: 0, ms };
    // matched by input, in order: a case one side has and the other lacks moves no other case
    const module = new Map();
    for (const g of got) {
      if (!module.has(input(g))) module.set(input(g), []);
      module.get(input(g)).push(answer(g));
    }
    want.forEach((w, i) => {
      const g = module.get(input(w))?.shift();
      if (g === undefined) {
        c.input++;
        diffs.push([family, i + 1, w, '(no such case)']);
      } else if (g === answer(w)) {
        c.equal++;
      } else {
        if (thrown(answer(w)) !== undefined && thrown(answer(w)) === thrown(g)) c.message++;
        else c.answer++;
        diffs.push([family, i + 1, w, `${input(w)}\t${g}`]);
      }
    });
    for (const [k, rest] of module) {
      for (const g of rest) {
        c.input++;
        diffs.push([family, 0, '(no such case)', `${k}\t${g}`]);
      }
    }
    rows.push(c);
  }

  console.log('family'.padEnd(26), 'cases'.padStart(7), 'equal'.padStart(7), 'answer'.padStart(7),
    'message'.padStart(8), 'input'.padStart(6), 'ms'.padStart(7));
  for (const c of rows) {
    console.log(c.family.padEnd(26), String(c.cases).padStart(7), String(c.equal).padStart(7),
      String(c.answer).padStart(7), String(c.message).padStart(8), String(c.input).padStart(6), String(c.ms).padStart(7));
  }
  const shown = new Map();
  for (const [family, line, w, g] of diffs) {
    const n = shown.get(family) ?? 0;
    if (n >= 3) continue;
    shown.set(family, n + 1);
    console.log(`\n${family}:${line}\n  JDK  : ${w.slice(0, 300)}\n  TeaVM: ${g.slice(0, 300)}`);
  }
  const outputs = process.env['TEST_UNDECLARED_OUTPUTS_DIR'];
  if (outputs && diffs.length > 0) {
    writeFileSync(join(outputs, 'conformance-diffs.tsv'),
      diffs.map(([f, l, w, g]) => `${f}\t${l}\t${w}\t${g}`).join('\n') + '\n');
  }

  // THE KNOWN DIFFERENCES (conformance-known.tsv): each family that still differs, with its counts, a digest of WHICH
  // cases differ, and why. The test holds the module to the ledger exactly: a new difference fails it, and so does a
  // fixed one, until its row is lowered -- the ledger only shrinks (a row moves only with a dated reason).
  const known = new Map(readFileSync(runfileFromEnv('CONFORMANCE_KNOWN'), 'utf8').split('\n')
    .filter((l) => l !== '' && !l.startsWith('#'))
    .map((l) => l.split('\t'))
    .map(([family, answer, message, missing, digest]) => [family, { answer: +answer, message: +message,
      input: +missing, digest }]));
  // the digest covers each differing case's input AND both answers, so an answer that changes to another wrong one
  // fails too
  const digestOf = (family) => createHash('sha256')
    .update(diffs.filter(([f]) => f === family).map(([, , w, g]) => `${w}\u0000${g}`).sort().join('\n'), 'utf8')
    .digest('hex').slice(0, 16);
  const wrong = [];
  for (const c of rows) {
    const has = c.answer + c.message + c.input > 0;
    const row = known.get(c.family);
    const now = { answer: c.answer, message: c.message, input: c.input, digest: has ? digestOf(c.family) : '-' };
    const then = row ?? { answer: 0, message: 0, input: 0, digest: '-' };
    if (now.answer !== then.answer || now.message !== then.message || now.input !== then.input
        || now.digest !== then.digest) {
      wrong.push(`${c.family}\t${now.answer}\t${now.message}\t${now.input}\t${now.digest}\t(the ledger: ${then.answer} `
        + `${then.message} ${then.input} ${then.digest})`);
    }
  }
  for (const family of known.keys()) {
    if (!families.includes(family)) wrong.push(`${family}: in the ledger, but no such family`);
  }
  assert.deepEqual(wrong, [], 'the module differs from conformance-known.tsv: a fewer count is a fix (lower the row, '
    + 'with a dated reason), a greater or a different digest is a new difference (conformance-diffs.tsv)');
});
