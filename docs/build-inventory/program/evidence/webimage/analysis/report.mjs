// SPIKE (2026-10-10): evaluate a native-image build report's `sections` literal with stand-in component classes and
// print the Image Heap and Resources sections' tables (and breakdowns) as text. Arguments: <report.html> [section]
import { readFileSync, writeFileSync } from 'node:fs';
import vm from 'node:vm';

const [file, only] = process.argv.slice(2);
const html = readFileSync(file, 'utf8');
const start = html.indexOf('const sections=');
const scriptEnd = html.indexOf('</script>', start);
const body = html.slice(start, scriptEnd);
const keep = (kind) => function (x) { this.kind = kind; this.data = x; };
const context = {
  Table: keep('Table'), Breakdown: keep('Breakdown'), HorizontalStackedBarChart: keep('Bar'), SunburstChart: keep('Sunburst'),
  Blob: function () {}, MutationObserver: function () { this.observe = () => {}; }, document: new Proxy({}, { get: () => () => ({}) }),
  window: {}, console,
};
vm.createContext(context);
// only the sections literal: up to its own terminating semicolon at depth 0
let depth = 0, inStr = false, esc = false, end = 0;
for (let k = 'const sections='.length; k < body.length; k++) {
  const c = body[k];
  if (inStr) { if (esc) esc = false; else if (c === '\\') esc = true; else if (c === '"') inStr = false; continue; }
  if (c === '"') inStr = true;
  else if (c === '[' || c === '{' || c === '(') depth++;
  else if (c === ']' || c === '}' || c === ')') { depth--; if (depth === 0) { end = k + 1; break; } }
}
vm.runInContext(`globalThis.sections = ${body.slice('const sections='.length, end)};`, context);
const cellText = (c) => c?.text ?? (Array.isArray(c?.cells) ? c.cells.map(cellText).join(' | ') : JSON.stringify(c)?.slice(0, 60));
for (const section of context.sections) {
  if (only && section.name !== only) continue;
  console.log(`\n######## ${section.name}`);
  for (const comp of section.components) {
    if (comp.kind === 'Table') {
      const t = comp.data;
      for (const row of t.header ?? []) console.log(`== ${row.cells.map(cellText).join(' | ')}`);
      for (const row of (t.body ?? []).slice(0, 25)) console.log(`   ${row.cells.map(cellText).join(' | ')}`);
      if ((t.body ?? []).length > 25) console.log(`   ... ${(t.body ?? []).length - 25} more rows`);
    } else {
      if (process.env.DUMP) writeFileSync(`${process.env.DUMP}.${comp.kind}.json`, JSON.stringify(comp.data));
      console.log(`[${comp.kind}] ${JSON.stringify(comp.data).slice(0, 1500)}`);
    }
  }
}
