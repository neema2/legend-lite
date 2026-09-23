// Drive the stress page and report what broke.
import { createServer } from 'node:http';
import { readFile, writeFile } from 'node:fs/promises';
import { extname, join, normalize } from 'node:path';
import { chromium } from 'playwright';

const ROOT = new URL('..', import.meta.url).pathname;
const TYPES = {
  '.html': 'text/html', '.js': 'text/javascript', '.wasm': 'application/wasm',
  '.pure': 'text/plain', '.css': 'text/css',
};
const server = createServer(async (req, res) => {
  const rel = normalize((req.url ?? '/').split('?')[0])
    .replace(/^(\.\.[/])+/, '');
  try {
    const body = await readFile(join(ROOT, rel));
    res.writeHead(200, {
      'Content-Type': TYPES[extname(rel)] ?? 'application/octet-stream',
    });
    res.end(body);
  } catch { res.writeHead(404).end('not found'); }
});
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const { port } = server.address();

const browser = await chromium.launch();
const page = await browser.newPage();
const pageErrs = [];
page.on('pageerror', (e) => pageErrs.push(e.message));

await page.goto(`http://127.0.0.1:${port}/demo/stress.html`,
  { waitUntil: 'load', timeout: 180_000 });
await page.waitForFunction(() => window.__stressDone === true,
  undefined, { timeout: 600_000 });
const results = await page.evaluate(() => window.__stress ?? []);
const offeredNames = await page.evaluate(() => window.__stressOffered ?? []);
await browser.close();
server.close();

const byVerdict = { ok: 0, refused: 0, broke: 0 };
for (const r of results) byVerdict[r.verdict]++;

const broke = results.filter((r) => r.verdict === 'broke');
const refused = results.filter((r) => r.verdict === 'refused');

console.log(`ran ${results.length} operations`);
console.log(`  ok      ${byVerdict.ok}`);
console.log(`  refused ${byVerdict.refused}`);
console.log(`  broke   ${byVerdict.broke}`);

if (broke.length) {
  console.log(`\n=== BROKE (${broke.length}) ===`);
  // Group by the message shape so 40 instances of one bug read as
  // one bug.
  const groups = new Map();
  for (const b of broke) {
    const key = (b.detail ?? '').replace(/"[^"]*"/g, '"…"')
      .replace(/\b\d+\b/g, 'N').slice(0, 160);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(b);
  }
  for (const [key, items] of [...groups].sort((a, b) => b[1].length - a[1].length)) {
    console.log(`\n--- ${items.length}× ${key}`);
    const s = items[0];
    console.log(`    e.g. ${s.csv} / ${s.op}`);
    if (s.pure) console.log(`    pure: ${s.pure.slice(0, 200)}`);
    if (s.sql) console.log(`    sql : ${s.sql.replace(/\n/g, ' ').slice(0, 200)}`);
    const csvs = [...new Set(items.map((i) => i.csv))];
    const ops = [...new Set(items.map((i) => i.op))];
    console.log(`    csvs: ${csvs.slice(0, 6).join(', ')}`
      + (csvs.length > 6 ? ` +${csvs.length - 6}` : ''));
    console.log(`    ops : ${ops.slice(0, 8).join(', ')}`
      + (ops.length > 8 ? ` +${ops.length - 8}` : ''));
  }
}

// Refusals grouped too: a refusal with a bad MESSAGE is its own bug.
const rGroups = new Map();
for (const r of refused) {
  const key = (r.detail ?? '').replace(/'[^']*'/g, "'…'")
    .replace(/\b\d+\b/g, 'N').slice(0, 120);
  rGroups.set(key, (rGroups.get(key) ?? 0) + 1);
}
console.log(`\n=== refusal shapes (${rGroups.size}) ===`);
for (const [key, n] of [...rGroups].sort((a, b) => b[1] - a[1]).slice(0, 25)) {
  console.log(`  ${String(n).padStart(4)}× ${key}`);
}

if (pageErrs.length) {
  console.log(`\n=== uncaught page errors (${pageErrs.length}) ===`);
  for (const e of [...new Set(pageErrs)].slice(0, 10)) console.log(`  ${e}`);
}

// `bazel run` starts this in its runfiles; the results are for a person,
// so they go to the directory the run was started from.
const RESULTS = join(process.env.BUILD_WORKING_DIRECTORY ?? process.cwd(),
  'stress-results.json');
await writeFile(RESULTS, JSON.stringify(results, null, 2));
console.log(`\nfull results: ${RESULTS}`);

// THE INVARIANT: no NEW kind of breakage.
//
// Keyed by break CLASS, not by sample. The harness deliberately
// builds type-invalid comparisons (equalColumn between whichever two
// columns come first), so nearly every file trips the same open core
// gaps -- failing per-sample would just restate those and drown a
// genuinely new failure. Each entry below is an open bug with a
// reproduction; delete one when it is fixed, and a breakage matching
// nothing here fails this run.
const KNOWN = [
  {
    id: 'cross-type-comparison',
    match: (d) => /Conversion Error/.test(d),
    why: 'equalColumn/notEqualColumn between columns of different types '
      + 'type-checks and then fails in DuckDB. The LITERAL path refuses '
      + 'this correctly; the column-to-column path does not check at '
      + 'all. Also covers a String literal against a BOOLEAN column, '
      + 'which reaches DuckDB the same way.',
  },
  {
    id: 'contains-on-non-string',
    match: (d) => /Binder Error.*list_contains/.test(d),
    why: 'contains() on a BOOLEAN lowers to list_contains(BOOLEAN, ...) '
      + 'instead of refusing.',
  },
  {
    id: 'int64-overflow',
    match: (d) => /Out of Range|Overflow/.test(d),
    why: 'x * 2 on 9223372036854775807. Arguably correct of DuckDB to '
      + 'refuse; the cube should surface it as a query error.',
  },
];

const unexplained = [];
const seen = new Map();
for (const b of broke) {
  const d = b.detail ?? '';
  const k = KNOWN.find((x) => x.match(d));
  if (k) seen.set(k.id, (seen.get(k.id) ?? 0) + 1);
  else unexplained.push(b);
}

console.log('\n=== known-open break classes ===');
for (const k of KNOWN) {
  const n = seen.get(k.id) ?? 0;
  console.log(`  ${String(n).padStart(3)}× ${k.id}`
    + (n === 0 ? '   (none — fixed? remove it)' : ''));
}

if (unexplained.length) {
  console.log(`\n!!! ${unexplained.length} UNEXPLAINED breakage(s) — `
    + 'not any known-open class:');
  for (const b of unexplained.slice(0, 10)) {
    console.log(`  ${b.csv} / ${b.op}`);
    console.log(`    ${(b.detail ?? '').split('\n')[0].slice(0, 150)}`);
  }
}
// An offered sample failing to INGEST is always this suite's problem,
// whatever the break classes say.
const badIngest = results.filter((r) => r.op === 'ingest'
  && r.verdict !== 'ok' && offered.has(r.csv));
for (const b of badIngest) {
  console.log(`\n!!! offered sample ${b.csv} does not even ingest: `
    + `${(b.detail ?? '').slice(0, 120)}`);
}

process.exit(unexplained.length > 0 || badIngest.length > 0 ? 1 : 0);
