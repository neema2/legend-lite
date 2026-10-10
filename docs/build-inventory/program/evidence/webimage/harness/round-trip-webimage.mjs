// SPIKE (2026-10-10): wasm/round_trip.mjs, unchanged in what it compares, with the module swapped for the GraalVM
// Web Image build. Arguments: <web image dir> <tab-round-trip.jsonl> [<diffs out>]
const { load } = await import(process.env.LOADER ?? './webimage-loader.mjs');
import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';

const [dir, inputs, diffsOut] = process.argv.slice(2);
const GRAMMAR_TO_JSON = '/api/pure/v1/grammar/grammarToJson/model';
const JSON_TO_GRAMMAR = '/api/pure/v1/grammar/jsonToGrammar/model';
const STYLES = ['STANDARD', 'PRETTY'];

const sha256 = (s) => createHash('sha256').update(s, 'utf8').digest('hex');

function okBody(folded) {
  const prefix = 'OK\n200\n';
  if (!folded.startsWith(prefix)) return undefined;
  const type = folded.indexOf('\n', prefix.length);
  return type < 0 ? undefined : folded.slice(type + 1);
}

const lines = readFileSync(inputs, 'utf8').split('\n').filter((l) => l !== '');
const t0 = performance.now();
const module = await load(dir);
const t1 = performance.now();
const ask = (path, query, body) => {
  try {
    return module.exports.pureV1OrError(path, query, body);
  } catch (e) {
    return `THREW-THROUGH-JS\n${e && e.message}`;
  }
};

const counts = new Map();
const diffs = [];
const differing = [];
for (const line of lines) {
  const { from, id, text, answers } = JSON.parse(line);
  const c = counts.get(from) ?? { inputs: 0, converted: 0, matched: 0 };
  counts.set(from, c);
  c.inputs++;
  const toJson = ask(GRAMMAR_TO_JSON, 'returnSourceInformation=false', text);
  const asked = [sha256(toJson)];
  const json = okBody(toJson);
  if (json !== undefined) {
    c.converted++;
    for (const style of STYLES) asked.push(sha256(ask(JSON_TO_GRAMMAR, `renderStyle=${style}`, json)));
  }
  if (asked.length === answers.length && asked.every((a, i) => a === answers[i])) {
    c.matched++;
  } else {
    const at = asked.findIndex((a, i) => a !== answers[i]);
    diffs.push(`${from} ${id}: answer ${at < 0 ? asked.length : at} differs`
      + (at === 0 ? `\n  module: ${JSON.stringify(toJson.slice(0, 300))}` : ''));
    differing.push(JSON.stringify({ from, id, at, toJson, prints: json === undefined ? []
      : STYLES.map((style) => ask(JSON_TO_GRAMMAR, `renderStyle=${style}`, json)) }));
  }
}
const t2 = performance.now();
if (diffsOut && differing.length > 0) writeFileSync(diffsOut, differing.join('\n') + '\n');
console.log(`load ${(t1 - t0).toFixed(0)} ms, ${lines.length} inputs ${(t2 - t1).toFixed(0)} ms`);
for (const [from, c] of counts) {
  console.log(`[tab-round-trip] ${from}: inputs=${c.inputs} converted=${c.converted} matched=${c.matched}`);
}
console.log(`${diffs.length} inputs answered differently from the JVM`);
for (const d of diffs.slice(0, 40)) console.log(d);
if (diffs.length) process.exitCode = 1;
