// Local mode with parse and compose local and compile on the server: does the engine compile legend-lite's parse JSON?
// And does legend-lite compose PRETTY, as text mode asks, exactly as the engine does?
// usage: node mixed.mjs <module dir> <cases dir> <engine base url>
import { readFileSync, readdirSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
import { resolve, join } from 'node:path';
const [dir, casesDir, engineBase] = process.argv.slice(2);
const { load } = await import(pathToFileURL(resolve(dir, 'wasm-gc-module-runtime.js')).href);
const m = await load(resolve(dir, 'classes.wasm'), { stackDeobfuscator: { enabled: false }, installImports(o) { o.teavmConsole = { putcharStdout() {}, putcharStderr() {} }; } });
const lite = (path, q, body) => { const r = m.exports.pureV1OrError(`/api/pure/v1/${path}`, q, body); const p = r.split('\n'); return { status: Number(p[1]), body: p.slice(3).join('\n') }; };
const eng = async (path, q, body, type) => { const r = await fetch(`${engineBase}/api/pure/v1/${path}${q ? '?' + q : ''}`, { method: 'POST', headers: { 'content-type': type }, body }); return { status: r.status, body: await r.text() }; };
const cases = readdirSync(casesDir).filter((f) => f.endsWith('.pure')).sort();
const t = { compiled: 0, engineRefusedLiteJson: 0, baselineFails: 0, skipped: 0, prettySame: 0, prettyDiff: 0, standardSame: 0, standardDiff: 0 };
const notes = [];
for (const f of cases) {
  const text = readFileSync(join(casesDir, f), 'utf8');
  const lp = lite('grammar/grammarToJson/model', 'returnSourceInformation=true', text);
  const ep = await eng('grammar/grammarToJson/model', 'returnSourceInformation=true', text, 'text/plain');
  if (lp.status !== 200 || ep.status !== 200) { t.skipped++; continue; }
  // 1. the mixed path: legend-lite's parse JSON, compiled by the engine (text mode's compileText, no dependencies)
  const mixed = await eng('compilation/compile', '', lp.body, 'application/json');
  const base = await eng('compilation/compile', '', ep.body, 'application/json');
  if (base.status !== 200) t.baselineFails++;
  if (mixed.status === 200) t.compiled++;
  else { t.engineRefusedLiteJson++; notes.push(`${f}: engine ${mixed.status} on legend-lite's JSON (on its own JSON: ${base.status}): ${mixed.body.slice(0, 220)}`); }
  // 2. compose: the engine's parse JSON, without positions, composed by both, PRETTY (text mode) and STANDARD
  const ej = (await eng('grammar/grammarToJson/model', 'returnSourceInformation=false', text, 'text/plain')).body;
  for (const style of ['PRETTY', 'STANDARD']) {
    const [lc, ec] = [lite('grammar/jsonToGrammar/model', `renderStyle=${style}`, ej), await eng('grammar/jsonToGrammar/model', `renderStyle=${style}`, ej, 'application/json')];
    const same = lc.status === 200 && ec.status === 200 && lc.body === ec.body;
    if (style === 'PRETTY') same ? t.prettySame++ : t.prettyDiff++; else same ? t.standardSame++ : t.standardDiff++;
    if (!same && notes.length < 40) {
      const a = lc.body.split('\n'), b = ec.body.split('\n'); let i = 0; while (i < Math.min(a.length, b.length) && a[i] === b[i]) i++;
      notes.push(`${f} ${style}: lite ${lc.status} / engine ${ec.status}; first difference at line ${i + 1}: lite ${JSON.stringify((a[i] ?? '<end>').slice(0, 90))} engine ${JSON.stringify((b[i] ?? '<end>').slice(0, 90))}`);
    }
  }
}
console.log(`cases ${cases.length}`, JSON.stringify(t));
notes.forEach((n) => console.log(' -', n));
