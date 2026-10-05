// Distinguishing case: a user function sharing a platform function's short name, with a DIFFERENT return type.
// The caller only type-checks if the USER function is chosen.
const S = { engine: 'http://127.0.0.1:6300/api/pure/v1', lite: 'http://127.0.0.1:18777/api/pure/v1' };
const post = async (b, p, body) => { const r = await fetch(b + p, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }); return { s: r.status, t: await r.text() }; };
const msg = (r) => { try { const j = JSON.parse(r.t); return `${r.s} ${(j.message ?? 'OK').replace(/\s+/g, ' ').slice(0, 160)}`; } catch { return `${r.s} ${r.t.slice(0, 160)}`; } };
const util = `###Pure\nfunction my::util::toUpper(s: String[1]): Integer[1] { 42 }\n`;
const cases = {
  'G import + short toUpper, caller expects Integer (user wins?)': `###Pure\nimport my::util::*;\nfunction my::app::g(): Integer[1] { 'x'->toUpper() }\n`,
  'H import + short toUpper, caller expects String (platform wins?)': `###Pure\nimport my::util::*;\nfunction my::app::h(): String[1] { 'x'->toUpper() }\n`,
  'I full user path': `###Pure\nfunction my::app::i(): Integer[1] { 'x'->my::util::toUpper() }\n`,
};
for (const [label, code] of Object.entries(cases)) {
  const line = [];
  for (const [name, base] of Object.entries(S)) line.push(`${name}: ${msg(await post(base, '/compilation/compile', { _type: 'text', code: util + code }))}`);
  console.log(`${label}\n  ${line.join('\n  ')}`);
}
