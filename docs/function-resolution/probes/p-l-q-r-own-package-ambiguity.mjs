const S = { engine: 'http://127.0.0.1:6300/api/pure/v1', lite: 'http://127.0.0.1:18777/api/pure/v1' };
const post = async (b, p, body) => { const r = await fetch(b + p, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }); return { s: r.status, t: await r.text() }; };
const msg = (r) => { try { const j = JSON.parse(r.t); return `${r.s} ${(j.message ?? 'OK').replace(/\s+/g, ' ').slice(0, 150)}`; } catch { return `${r.s} ${r.t.slice(0, 150)}`; } };
const cases = {
  'P same-package call, no import': `###Pure\nfunction my::app::helper(): String[1] { 'h' }\nfunction my::app::p(): String[1] { helper() }\n`,
  'L platform-pure without engine handler (elementToPath), short': `###Pure\nClass my::app::C {}\nfunction my::app::l(): String[1] { my::app::C->elementToPath() }\n`,
  'Q two imports both define fn (ambiguous)': `###Pure\nfunction a::x::f(): String[1] { 'a' }\nfunction b::y::f(): String[1] { 'b' }\n###Pure\nimport a::x::*;\nimport b::y::*;\nfunction my::app::q(): String[1] { f() }\n`,
  'R two imports, different arities (overload across packages)': `###Pure\nfunction a::x::g(): String[1] { 'a' }\nfunction b::y::g(s: String[1]): String[1] { 'b' }\n###Pure\nimport a::x::*;\nimport b::y::*;\nfunction my::app::r(): String[1] { g('z') }\n`,
};
for (const [label, code] of Object.entries(cases)) {
  const line = [];
  for (const [name, base] of Object.entries(S)) line.push(`${name}: ${msg(await post(base, '/compilation/compile', { _type: 'text', code }))}`);
  console.log(`${label}\n  ${line.join('\n  ')}`);
}
