// How a function NAME in an applied function resolves: platform short name vs user function, with and without imports,
// as text (imports visible) and as Studio-style JSON (no SectionIndex: imports gone).
const S = { engine: 'http://127.0.0.1:6300/api/pure/v1', lite: 'http://127.0.0.1:18777/api/pure/v1' };
const post = async (b, p, body, t) => { const r = await fetch(b + p, { method: 'POST', headers: { 'Content-Type': t ? 'text/plain' : 'application/json' }, body: t ? body : JSON.stringify(body) }); return { s: r.status, t: await r.text() }; };
const msg = (r) => { try { const j = JSON.parse(r.t); return `${r.s} ${(j.message ?? 'OK').replace(/\s+/g, ' ').slice(0, 140)}`; } catch { return `${r.s} ${r.t.slice(0, 140)}`; } };
const util = `###Pure
function my::util::myFunc(s: String[1]): String[1] { $s + '!' }
function my::util::filter(s: String[1]): String[1] { $s + '?' }
`;
const cases = {
  'A short user fn via import':        `###Pure\nimport my::util::*;\nfunction my::app::a(): String[1] { 'x'->myFunc() }\n`,
  'B short user fn, NO import':        `###Pure\nfunction my::app::b(): String[1] { 'x'->myFunc() }\n`,
  'C short "filter" via import (user filter exists)': `###Pure\nimport my::util::*;\nfunction my::app::c(): String[1] { 'x'->filter() }\n`,
  'D full user filter':                 `###Pure\nfunction my::app::d(): String[1] { 'x'->my::util::filter() }\n`,
  'E platform filter, short':           `###Pure\nfunction my::app::e(): String[*] { ['x','y']->filter(s|$s == 'x') }\n`,
  'F platform filter, full meta path':  `###Pure\nfunction my::app::f(): String[*] { ['x','y']->meta::pure::functions::collection::filter(s|$s == 'x') }\n`,
};
for (const [label, code] of Object.entries(cases)) {
  const line = [];
  for (const [name, base] of Object.entries(S)) {
    const text = await post(base, '/compilation/compile', { _type: 'text', code: util + code });
    let json = '-';
    if (name === 'engine') {
      // Studio-style: elements as JSON, SectionIndex dropped (imports lost)
      const pm = JSON.parse((await post(base, '/grammar/grammarToJson/model?returnSourceInformation=false', util + code, true)).t);
      pm.elements = pm.elements.filter((e) => e._type !== 'sectionIndex');
      json = msg(await post(base, '/compilation/compile', pm));
    }
    line.push(`${name} text: ${msg(text)}${name === 'engine' ? ` | engine JSON w/o imports: ${json}` : ''}`);
  }
  console.log(`\n${label}\n  ${line.join('\n  ')}`);
}
