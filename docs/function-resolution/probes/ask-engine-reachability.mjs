// Ask legend-engine 4.145.0 itself whether each name lite flagged "L" is callable from user code:
// compile `name()` in a user function and read the error. "Function does not exist" = not reachable from user
// code; "Can't find a match" (or success) = registered (reachable), only the arguments are wrong.
import { readFileSync, writeFileSync } from 'node:fs';
const ENGINE = 'http://127.0.0.1:6300/api/pure/v1/compilation/compile';
const names = readFileSync('docs/function-resolution/data/l-names.txt', 'utf8').split('\n').filter(Boolean);
const verdict = async (name) => {
  const code = `###Pure\nfunction my::probe::q(): Any[*] { ${name}() }\n`;
  const r = await fetch(ENGINE, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ _type: 'text', code }) });
  const j = await r.json().catch(() => ({}));
  const m = String(j.message ?? '');
  if (r.status === 200) return 'reachable(compiles)';
  if (/Function does not exist/.test(m)) return 'unreachable';
  if (/Can't find a match for function/.test(m)) return 'reachable';
  if (/multiple matches/.test(m)) return 'ambiguous';
  return `other: ${m.replace(/\s+/g, ' ').slice(0, 80)}`;
};
const out = [];
for (let i = 0; i < names.length; i += 16) {
  const batch = names.slice(i, i + 16);
  const vs = await Promise.all(batch.map(verdict));
  batch.forEach((n, k) => out.push(`${n}\t${vs[k]}`));
}
writeFileSync('docs/function-resolution/data/l-verdicts.tsv', out.join('\n') + '\n');
const tally = {};
for (const l of out) { const v = l.split('\t')[1].replace(/:.*/, ''); tally[v] = (tally[v] ?? 0) + 1; }
console.log(tally);
