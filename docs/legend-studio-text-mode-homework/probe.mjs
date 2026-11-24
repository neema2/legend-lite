// Studio text mode's three routes: legend-lite's engine module (in-process) against a real legend-engine (HTTP).
// usage: node probe.mjs <module dir> <cases dir> <engine base url>
import { readFileSync, readdirSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
import { resolve, join } from 'node:path';

const [dir, casesDir, engineBase] = process.argv.slice(2);
const { load } = await import(pathToFileURL(resolve(dir, 'wasm-gc-module-runtime.js')).href);
const m = await load(resolve(dir, 'classes.wasm'), {
  stackDeobfuscator: { enabled: false },
  installImports(o) {
    o.teavmConsole = { putcharStdout() {}, putcharStderr() {} };
  },
});

const lite = (path, query, body) => {
  const t = performance.now();
  const raw = m.exports.pureV1OrError(`/api/pure/v1/${path}`, query, body);
  const ms = performance.now() - t;
  if (!raw.startsWith('OK\n')) return { status: 'MODULE-ERROR', body: raw, ms };
  const rest = raw.slice(3);
  const i = rest.indexOf('\n');
  const j = rest.indexOf('\n', i + 1);
  return { status: Number(rest.slice(0, i)), body: rest.slice(j + 1), ms };
};
const engine = async (path, query, body, type = 'text/plain') => {
  const t = performance.now();
  const r = await fetch(`${engineBase}/api/pure/v1/${path}${query ? `?${query}` : ''}`, {
    method: 'POST',
    headers: { 'content-type': type },
    body,
  });
  const text = await r.text();
  return { status: r.status, body: text, ms: performance.now() - t };
};
const json = (s) => {
  try {
    return JSON.parse(s);
  } catch {
    return undefined;
  }
};

// every path at which two JSON values differ (object keys unordered, arrays ordered); a is the engine's, b lite's
const diffs = (a, b, path = '$', out = []) => {
  if (out.length > 50) return out;
  if (typeof a !== typeof b || Array.isArray(a) !== Array.isArray(b) || (a === null) !== (b === null)) {
    out.push(path);
  } else if (Array.isArray(a)) {
    if (a.length !== b.length) out.push(`${path}[len ${a.length} vs ${b.length}]`);
    for (let i = 0; i < Math.min(a.length, b.length); i++) diffs(a[i], b[i], `${path}[${i}]`, out);
  } else if (a && typeof a === 'object') {
    for (const k of new Set([...Object.keys(a), ...Object.keys(b)])) {
      if (!(k in a)) out.push(`${path}.${k} (lite only)`);
      else if (!(k in b)) out.push(`${path}.${k} (engine only)`);
      else diffs(a[k], b[k], `${path}.${k}`, out);
    }
  } else if (a !== b) out.push(path);
  return out;
};
const section = (t) => console.log(`\n========== ${t}`);

// ---------------------------------------------------------------- A. parse with source information
section('A. grammarToJson/model?returnSourceInformation=true, every Studio roundtrip case');
const cases = readdirSync(casesDir).filter((f) => f.endsWith('.pure')).sort();
const tally = { equal: 0, sourceInfoOnly: 0, other: 0, liteRefused: 0, engineRefused: 0 };
const elementSi = { equal: 0, differs: 0, liteMissing: 0 };
const examples = [];
for (const f of cases) {
  const text = readFileSync(join(casesDir, f), 'utf8');
  const [l, e] = [lite('grammar/grammarToJson/model', 'returnSourceInformation=true', text), await engine('grammar/grammarToJson/model', 'returnSourceInformation=true', text)];
  if (e.status !== 200) {
    tally.engineRefused++;
    examples.push(`${f}: engine ${e.status} ${e.body.slice(0, 160)}`);
    continue;
  }
  if (l.status !== 200) {
    tally.liteRefused++;
    examples.push(`${f}: lite ${l.status} ${l.body.slice(0, 160)}`);
    continue;
  }
  const [lj, ej] = [json(l.body), json(e.body)];
  const d = diffs(ej, lj);
  if (!d.length) tally.equal++;
  else if (d.every((p) => p.includes('sourceInformation'))) {
    tally.sourceInfoOnly++;
    if (examples.length < 25) examples.push(`${f}: sourceInformation differs at ${d.length} path(s), e.g. ${d.slice(0, 3).join(' | ')}`);
  } else {
    tally.other++;
    if (examples.length < 25) examples.push(`${f}: differs at ${d.length} path(s), e.g. ${d.filter((p) => !p.includes('sourceInformation')).slice(0, 4).join(' | ')}`);
  }
  // what Studio's text mode indexes: each element's own sourceInformation
  const byPath = (doc) => new Map((doc.elements ?? []).filter((x) => x._type !== 'sectionIndex').map((x) => [`${x.package}::${x.name}`, x.sourceInformation]));
  const [le, ee] = [byPath(lj), byPath(ej)];
  for (const [k, si] of ee) {
    const lsi = le.get(k);
    if (!lsi) elementSi.liteMissing++;
    else if (JSON.stringify(lsi) === JSON.stringify(si)) elementSi.equal++;
    else {
      elementSi.differs++;
      if (examples.length < 30) examples.push(`${f} ${k}: element sourceInformation engine ${JSON.stringify(si)} lite ${JSON.stringify(lsi)}`);
    }
  }
}
console.log(`cases: ${cases.length}`, JSON.stringify(tally));
console.log('element-level sourceInformation (what Studio indexes):', JSON.stringify(elementSi));
examples.slice(0, 30).forEach((x) => console.log('  -', x));

// ---------------------------------------------------------------- B. errors
section('B. errors: status and payload, engine then lite');
const errorCases = [
  ['parse: missing semicolon', 'grammar/grammarToJson/model', 'returnSourceInformation=true', 'Class model::A\n{\n  name: String[1]\n}\n'],
  ['parse: unknown section', 'grammar/grammarToJson/model', 'returnSourceInformation=true', '###Bogus\nwhatever\n'],
  ['parse: a GraphQL section (engine knows it, lite does not)', 'grammar/grammarToJson/model', 'returnSourceInformation=true',
    readdirSync(casesDir).includes('STO_GraphQL-store-connection-mapping.pure') ? readFileSync(join(casesDir, 'STO_GraphQL-store-connection-mapping.pure'), 'utf8') : '###GraphQL\nGraphQL store::G\n{\n}\n'],
];
for (const [name, path, q, text] of errorCases) {
  const [e, l] = [await engine(path, q, text), lite(path, q, text)];
  console.log(`\n-- ${name}\n   engine ${e.status}: ${e.body.slice(0, 420)}\n   lite   ${l.status}: ${l.body.slice(0, 420)}`);
}
const compileText = async (name, text) => {
  // as Studio's compileText does: parse with source information, then compile the JSON
  const [ep, lp] = [await engine('grammar/grammarToJson/model', 'returnSourceInformation=true', text), lite('grammar/grammarToJson/model', 'returnSourceInformation=true', text)];
  const [e, l] = [
    ep.status === 200 ? await engine('compilation/compile', '', ep.body, 'application/json') : ep,
    lp.status === 200 ? lite('compilation/compile', '', lp.body) : lp,
  ];
  console.log(`\n-- ${name}\n   engine ${e.status}: ${e.body.slice(0, 520)}\n   lite   ${l.status}: ${l.body.slice(0, 520)}`);
};
await compileText('compile: unknown property type', 'Class model::A\n{\n  name: Strin[1];\n}\n');
await compileText('compile: unknown property in a derived property', 'Class model::A\n{\n  name: String[1];\n  upper() {$this.nme->toUpper()}: String[1];\n}\n');
await compileText('compile: function returns the wrong type', 'function model::f(): Integer[1]\n{\n  \'a\'\n}\n');
await compileText('compile: a Pure class mapping with no ~src (lite refuses, register S21)',
  'Class model::A\n{\n  name: String[1];\n}\n\n\n###Mapping\nMapping model::M\n(\n  model::A: Pure\n  {\n    name: \'x\'\n  }\n)\n');

// ---------------------------------------------------------------- C. a dependency's entities in the payload
section('C. compile with a dependency: its elements as JSON, appended to the parsed project (as Studio sends it)');
{
  const dep = 'Class dep::Person\n{\n  name: String[1];\n}\n\nEnum dep::Kind\n{\n  A,\n  B\n}\n';
  const main = 'Class model::Firm\n{\n  employees: dep::Person[*];\n  kind: dep::Kind[1];\n  names() {$this.employees.name->joinStrings(\',\')}: String[1];\n}\n';
  const depJson = json((await engine('grammar/grammarToJson/model', 'returnSourceInformation=false', dep)).body);
  const lp = json(lite('grammar/grammarToJson/model', 'returnSourceInformation=true', main).body);
  const merged = { ...lp, elements: [...depJson.elements.filter((x) => x._type !== 'sectionIndex'), ...lp.elements] };
  const l = lite('compilation/compile', '', JSON.stringify(merged));
  const e = await engine('compilation/compile', '', JSON.stringify(merged), 'application/json');
  console.log(`   engine ${e.status}: ${e.body.slice(0, 200)}\n   lite   ${l.status}: ${l.body.slice(0, 200)}`);
  const broken = { ...merged, elements: merged.elements.filter((x) => x.name !== 'Kind') };
  const [e2, l2] = [await engine('compilation/compile', '', JSON.stringify(broken), 'application/json'), lite('compilation/compile', '', JSON.stringify(broken))];
  console.log(`   without dep::Kind:\n   engine ${e2.status}: ${e2.body.slice(0, 400)}\n   lite   ${l2.status}: ${l2.body.slice(0, 400)}`);
}

// ---------------------------------------------------------------- D. timings
section('D. timings (ms, first run / second run; both processes already warm from A-C; Node 25, not a browser; no dependencies in the compile)');
const time = async (name, text) => {
  const lp = lite('grammar/grammarToJson/model', 'returnSourceInformation=true', text);
  const lp2 = lite('grammar/grammarToJson/model', 'returnSourceInformation=true', text);
  const ep = await engine('grammar/grammarToJson/model', 'returnSourceInformation=true', text);
  const ep2 = await engine('grammar/grammarToJson/model', 'returnSourceInformation=true', text);
  const noSi = lite('grammar/grammarToJson/model', 'returnSourceInformation=false', text).body;
  const lc = lite('grammar/jsonToGrammar/model', 'renderStyle=PRETTY', noSi);
  const lc2 = lite('grammar/jsonToGrammar/model', 'renderStyle=PRETTY', noSi);
  const ec = await engine('grammar/jsonToGrammar/model', 'renderStyle=PRETTY', noSi, 'application/json');
  const ec2 = await engine('grammar/jsonToGrammar/model', 'renderStyle=PRETTY', noSi, 'application/json');
  const lk = lite('compilation/compile', '', lp.body);
  const lk2 = lite('compilation/compile', '', lp.body);
  const ek = await engine('compilation/compile', '', ep.body, 'application/json');
  const ek2 = await engine('compilation/compile', '', ep.body, 'application/json');
  const f = (a, b) => `${a.ms.toFixed(0)} / ${b.ms.toFixed(0)}`;
  console.log(`${name}: ${text.length} chars, ${(json(noSi)?.elements ?? []).length} elements`);
  console.log(`   parse    lite ${f(lp, lp2)} [${lp.status}]   engine ${f(ep, ep2)} [${ep.status}]`);
  console.log(`   compose  lite ${f(lc, lc2)} [${lc.status}]   engine ${f(ec, ec2)} [${ec.status}]   same text: ${lc.body === ec.body}`);
  console.log(`   compile  lite ${f(lk, lk2)} [${lk.status}]   engine ${f(ek, ek2)} [${ek.status}]`);
};
const biggest = cases.map((f) => [f, readFileSync(join(casesDir, f), 'utf8')]).sort((a, b) => b[1].length - a[1].length)[0];
await time(`largest case (${biggest[0]})`, biggest[1]);
const synth = (n) => {
  let s = '';
  for (let i = 0; i < n; i++) {
    s += `Class gen::p${i % 20}::C${i}\n{\n`;
    for (let k = 0; k < 8; k++) s += `  s${k}: String[0..1];\n`;
    s += `  n: Integer[1];\n  next: gen::p${(i + 1) % 20}::C${(i + 1) % n}[0..1];\n`;
    s += `  label() {$this.s0->toOne() + '-' + $this.n->toString()}: String[1];\n}\n\n`;
  }
  for (let i = 0; i < n / 5; i++) {
    s += `function gen::fn${i}(xs: gen::p${i % 20}::C${i}[*]): String[*]\n{\n  $xs->filter(x|$x.n > ${i})->map(x|$x.label())\n}\n\n`;
  }
  return s;
};
await time('synthetic: 500 classes, 100 functions', synth(500));
await time('synthetic: 2,000 classes, 400 functions', synth(2000));
