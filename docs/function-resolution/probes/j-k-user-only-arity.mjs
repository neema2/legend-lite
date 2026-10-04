// Divergence probe: a user function sharing a platform short name but with an ARITY no platform overload has.
// Union-by-signature (legend-pure style) picks the user function; tiered lookup (legend-engine) binds the platform name and fails.
const S = { engine: 'http://127.0.0.1:6300/api/pure/v1', lite: 'http://127.0.0.1:18777/api/pure/v1' };
const post = async (b, p, body) => { const r = await fetch(b + p, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }); return { s: r.status, t: await r.text() }; };
const msg = (r) => { try { const j = JSON.parse(r.t); return `${r.s} ${(j.message ?? 'OK').replace(/\s+/g, ' ').slice(0, 170)}`; } catch { return `${r.s} ${r.t.slice(0, 170)}`; } };
const util = `###Pure\nfunction my::util::toUpper(s: String[1], n: Integer[1]): String[1] { $s + $n->toString() }\n`;
const cases = {
  'J import + short toUpper(2 args): only the user fn fits': `###Pure\nimport my::util::*;\nfunction my::app::j(): String[1] { 'x'->toUpper(2) }\n`,
  'K full user path': `###Pure\nfunction my::app::k(): String[1] { 'x'->my::util::toUpper(2) }\n`,
};
for (const [label, code] of Object.entries(cases)) {
  const line = [];
  for (const [name, base] of Object.entries(S)) line.push(`${name}: ${msg(await post(base, '/compilation/compile', { _type: 'text', code: util + code }))}`);
  console.log(`${label}\n  ${line.join('\n  ')}`);
}
