// DataCube's line and branch coverage over its Node tests, as a REPORT, never a gate
// (docs/DATACUBE_COMMAND_API_HOMEWORK_2026_10_17.md §11, part 4): run from time to time to find the code no unit or DOM
// test reaches, then a test written where it matters. The browser harnesses (demo/verify-*.mjs) and Python's suite
// reach code these do not -- the app shell, the headless bridge -- and are not counted here.
//
//   bazel build //wasm:planner_dir //engine-client:duckdb_extensions   # the planner and DuckDB the tests load
//   node datacube/tools/coverage.mjs [out-dir]                          # default: datacube-coverage/ in the system's temp folder
//
// Writes <out>/lcov.info (any lcov viewer reads it), <out>/tests.txt (each test's outcome: a test that needs what
// only its Bazel target gives -- a built bundle, a server -- fails here and is listed, not hidden) and
// <out>/summary.md (each source file's lines and branches, the least covered first).

import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, readdirSync, readFileSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const datacube = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const repo = resolve(datacube, '..');
const out = resolve(process.argv[2] ?? join(tmpdir(), 'datacube-coverage'));
mkdirSync(out, { recursive: true });

/** Every file under `dir`, recursively. */
function files(dir) {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    return statSync(path).isDirectory() ? files(path) : [path];
  });
}

// what the tests' Bazel targets give them, as runfiles under RUNFILES_DIR=/ (tools/js/runfiles.mts)
const bin = join(repo, 'bazel-bin');
const planner = join(bin, 'wasm', 'planner_dir');
const extensions = join(bin, 'engine-client', 'duckdb-extensions');
for (const [what, path] of [['the planner', planner], ["DuckDB's extensions", extensions]]) {
  if (!existsSync(path)) throw new Error(`${what} is not built (${path}): bazel build //wasm:planner_dir //engine-client:duckdb_extensions`);
}
const runfile = (path) => resolve(path).replace(/^\//, '');
const env = {
  ...process.env,
  RUNFILES_DIR: '/',
  WASM_PLANNER: runfile(planner),
  DUCKDB_EXTENSIONS: files(extensions).map(runfile).join(' '),
  CHART_RULES: runfile(join(datacube, 'test', 'fixtures', 'chart-rules.json')),
  PARITY: runfile(join(datacube, 'test', 'fixtures', 'parity', 'parity.json')),
  CORPUS: files(join(datacube, 'test', 'fixtures', 'pages')).filter((f) => f.endsWith('.page.json')).map(runfile).join(' '),
  UPSTREAM_PLANS: runfile(join(datacube, 'test', 'fixtures', 'upstream-plans.json')),
};

const tests = readdirSync(join(datacube, 'test')).filter((f) => f.endsWith('.test.ts')).sort().map((f) => join(datacube, 'test', f));
const ran = spawnSync(process.execPath, [
  '--experimental-wasm-exnref', '--experimental-strip-types', '--disable-warning=ExperimentalWarning',
  '--test', '--test-concurrency=2', '--experimental-test-coverage', `--test-coverage-include=${join(datacube, 'src')}/**`,
  '--test-reporter=lcov', `--test-reporter-destination=${join(out, 'lcov.info')}`,
  '--test-reporter=tap', `--test-reporter-destination=${join(out, 'tests.txt')}`,
  ...tests,
  // run from the report's folder: a test's database writes its files where it runs (upload.test.ts), never in the tree
], { cwd: out, env, stdio: 'inherit', timeout: 60 * 60 * 1000 });

// the lcov records, summed per source file
const lcov = readFileSync(join(out, 'lcov.info'), 'utf8');
const rows = [];
for (const record of lcov.split('end_of_record')) {
  const file = /^SF:(.*)$/m.exec(record)?.[1];
  if (!file) continue;
  const n = (key) => Number(new RegExp(`^${key}:(\\d+)$`, 'm').exec(record)?.[1] ?? 0);
  rows.push({ file: relative(datacube, resolve(datacube, file)), lines: [n('LH'), n('LF')], branches: [n('BRH'), n('BRF')] });
}
// a source file no test loaded has no record: listed as such, its lines its whole length, so it is not missed
const loaded = new Set(rows.map((r) => r.file));
const unloaded = files(join(datacube, 'src')).map((f) => relative(datacube, f))
  .filter((f) => f.endsWith('.ts') && !f.endsWith('.d.ts') && !loaded.has(f)).sort();
const pct = ([hit, all]) => (all === 0 ? 100 : (100 * hit) / all);
rows.sort((a, b) => pct(a.lines) - pct(b.lines) || b.lines[1] - a.lines[1]);
const sum = (k) => rows.reduce(([h, a], r) => [h + r[k][0], a + r[k][1]], [0, 0]);
const cell = (x) => `${pct(x).toFixed(1)}% (${x[0]}/${x[1]})`;
const failed = (readFileSync(join(out, 'tests.txt'), 'utf8').match(/^not ok \d+ - .*$/gm) ?? []);
writeFileSync(join(out, 'summary.md'), [
  `# DataCube coverage (Node tests), ${new Date().toISOString().slice(0, 10)}`,
  '',
  `${tests.length} test files; ${failed.length} top-level results not ok (needing what only their Bazel target gives, or failing):`,
  ...failed.map((f) => `- ${f}`),
  '',
  `Of the source files the tests load: lines ${cell(sum('lines'))}, branches ${cell(sum('branches'))}.`,
  '',
  `${unloaded.length} source files no Node test loads (the browser harnesses and Python's suite may):`,
  ...unloaded.map((f) => `- ${f}`),
  '',
  '| File | Lines | Branches |',
  '|---|---|---|',
  ...rows.map((r) => `| ${r.file} | ${cell(r.lines)} | ${cell(r.branches)} |`),
  '',
].join('\n'));
console.log(`coverage: ${join(out, 'summary.md')} (node exited ${ran.status})`);
