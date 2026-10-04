// Platform Pure functions WITHOUT an engine handler: are they callable by short name, by full path,
// and does an imported user function of the same name win or lose against them?
const S = { engine: 'http://127.0.0.1:6300/api/pure/v1' };
const post = async (b, p, body) => { const r = await fetch(b + p, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }); return { s: r.status, t: await r.text() }; };
const msg = (r) => { try { const j = JSON.parse(r.t); return `${r.s} ${(j.message ?? 'OK').replace(/\s+/g, ' ').slice(0, 170)}`; } catch { return `${r.s} ${r.t.slice(0, 170)}`; } };
const base = `###Pure\nClass my::app::C {}\n`;
const cases = {
  'L platform-pure, short (elementToPath)': `###Pure\nfunction my::app::l(): String[1] { my::app::C->elementToPath() }\n`,
  'M platform-pure, full path': `###Pure\nfunction my::app::m(): String[1] { my::app::C->meta::pure::functions::meta::elementToPath() }\n`,
  'N user elementToPath(Integer) imported vs platform-pure short, user-only arity': `###Pure\nfunction my::util::elementToPath(i: Integer[1], j: Integer[1]): String[1] { 'u' }\n###Pure\nimport my::util::*;\nfunction my::app::n(): String[1] { 1->elementToPath(2) }\n`,
  'O user fn same signature as platform-pure, imported, short': `###Pure\nfunction my::util::elementToPath(e: PackageableElement[1]): String[1] { 'user' }\n###Pure\nimport my::util::*;\nfunction my::app::o(): String[1] { my::app::C->elementToPath() }\n`,
};
for (const [label, code] of Object.entries(cases)) console.log(`${label}\n  engine: ${msg(await post(S.engine, '/compilation/compile', { _type: 'text', code: base + code }))}`);
