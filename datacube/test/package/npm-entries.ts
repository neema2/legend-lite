// THE NPM PACKAGE'S FOLDER, JUDGED (//datacube:npm_package; npm/README.md): what `npm publish` would send is the
// folder's files, so the folder is read here as an application's install would find it -- its package.json's every
// entry point a file that is there, its module with no import left for the application to resolve, its types the one
// file demo/library-api.ts is, its assets the ones the module fetches and no other, its licences with it. Read from the build's
// own output, with Node's own file calls: nothing is installed and nothing is fetched.
import assert from 'node:assert/strict';
import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs';
import { join, posix } from 'node:path';
import { test } from 'node:test';
import { runfileFromEnv } from '../../../tools/js/runfiles.mts';

const ROOT = runfileFromEnv('NPM_PACKAGE');
const at = (file: string): string => join(ROOT, ...file.split('/'));
const read = (file: string): string => readFileSync(at(file), 'utf8');
const isFile = (file: string): boolean => existsSync(at(file)) && statSync(at(file)).isFile();

/** Every file of the folder under `dir`, by its path from the folder ('/' between its parts), sorted. */
function files(dir = ''): string[] {
  return readdirSync(at(dir), { withFileTypes: true }).flatMap((entry) => {
    const path = dir === '' ? entry.name : `${dir}/${entry.name}`;
    return statSync(at(path)).isDirectory() ? files(path) : [path];
  }).sort();
}

interface PackageJson {
  readonly name?: string;
  readonly version?: string;
  readonly type?: string;
  readonly license?: string;
  readonly private?: boolean;
  readonly main?: string;
  readonly module?: string;
  readonly types?: string;
  readonly files?: readonly string[];
  readonly exports?: Readonly<Record<string, string | Readonly<Record<string, string>>>>;
  readonly dependencies?: unknown;
  readonly peerDependencies?: unknown;
  readonly optionalDependencies?: unknown;
  readonly scripts?: unknown;
}

const pkg = JSON.parse(read('package.json')) as PackageJson;

test('the folder is the package and nothing else', () => {
  assert.deepEqual(readdirSync(ROOT).sort(),
    ['LICENSE', 'NOTICE', 'README.md', 'THIRD_PARTY_NOTICES.md', 'assets', 'dist', 'package.json']);
  // what npm packs: `files`, and the three it always takes -- so every entry of the folder is sent
  const always = ['LICENSE', 'README.md', 'package.json'];
  assert.deepEqual([...(pkg.files ?? []), ...always].sort(), readdirSync(ROOT).sort(),
    'package.json `files` and the folder disagree: an entry npm would leave out, or one that is not there');
});

test('package.json names a module an application can install and import', () => {
  assert.match(pkg.name ?? '', /^@[a-z0-9-]+\/[a-z0-9-]+$/, 'a scoped name');
  assert.match(pkg.version ?? '', /^\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?$/, 'a version npm takes');
  assert.equal(pkg.type, 'module');
  assert.equal(pkg.license, 'Apache-2.0');
  assert.equal(pkg.private, undefined, 'a private package is refused by every registry');
  // everything DataCube runs on is inside the module: the application installs nothing beside it, and no script runs
  // as it is installed
  for (const key of ['dependencies', 'peerDependencies', 'optionalDependencies', 'scripts'] as const) {
    assert.equal(pkg[key], undefined, `package.json has ${key}`);
  }
  // each entry point is a file of the folder
  const targets = Object.entries(pkg.exports ?? {}).flatMap(([entry, target]): [string, string][] =>
    (typeof target === 'string' ? [[entry, target]] : Object.values(target).map((t): [string, string] => [entry, t])));
  assert.ok(targets.length >= 5, `too few entry points: ${JSON.stringify(pkg.exports)}`);
  for (const [entry, target] of targets) {
    assert.ok(target.startsWith('./'), `${entry}: ${target} is not a path of the package`);
    if (target.endsWith('/*')) {
      assert.ok(statSync(at(target.slice(2, -2))).isDirectory(), `${entry}: no folder ${target}`);
    } else {
      assert.ok(isFile(target.slice(2)), `${entry}: no file ${target}`);
    }
  }
  for (const field of ['main', 'module', 'types'] as const) {
    const target = pkg[field];
    assert.ok(target !== undefined && isFile(target.replace(/^\.\//, '')), `${field}: no file ${target}`);
  }
  const root = pkg.exports?.['.'];
  assert.ok(typeof root === 'object' && root.types === './dist/index.d.ts' && root.default === './dist/datacube.js',
    'the package itself is its module, with its types');
  // a stylesheet has types too (nothing to import): an application's typecheck holds an import made for its effect
  // alone to resolve to something
  for (const [entry, target] of Object.entries(pkg.exports ?? {})) {
    if (entry.endsWith('.css')) assert.ok(typeof target === 'object' && target.types?.endsWith('.d.ts'), `${entry} has no types`);
  }
  assert.ok(read('README.md').includes(`# ${pkg.name}`), "the README is the package's own");
});

test('its types are demo/library-api.ts, a file that reads no other', () => {
  const types = read('dist/index.d.ts');
  assert.equal(types, readFileSync(runfileFromEnv('NPM_API'), 'utf8'));
  const code = types.split('\n').filter((line) => !/^\s*(\/\/|\/?\*)/.test(line)).join('\n');
  assert.doesNotMatch(code, /\bimport\b|\bfrom\s*['"]|\brequire\s*\(/, 'the declarations import another file');
  assert.match(code, /export declare function mountDataCube\(/);
});

test('its module leaves no import for the application to resolve', () => {
  const meta = JSON.parse(readFileSync(runfileFromEnv('NPM_META'), 'utf8')) as {
    readonly outputs: Readonly<Record<string, { readonly imports?: readonly { readonly path: string; readonly external?: boolean }[] }>>;
  };
  const outputs = Object.keys(meta.outputs);
  assert.ok(outputs.length > 10, `esbuild's record names ${outputs.length} outputs: it is not the bundle's`);
  // esbuild names each output from where it ran (the package's directory): npm/dist/<file>
  const shipped = (output: string): string => posix.join('dist', posix.relative('npm/dist', output));
  for (const [output, { imports = [] }] of Object.entries(meta.outputs)) {
    assert.ok(isFile(shipped(output)), `${output} was built and is not in the package`);
    for (const i of imports) {
      assert.ok(!i.external, `${output} imports ${i.path}, which the package does not hold`);
      assert.ok(isFile(shipped(i.path)), `${output} imports ${i.path}, which is not in the package`);
    }
  }
  // and nothing in dist/ the bundle did not make, beside the types (the module's, the stylesheets') and the fonts'
  // stylesheet
  const made = new Set(outputs.map(shipped));
  assert.deepEqual(files('dist').filter((f) => !made.has(f)), ['dist/css.d.ts', 'dist/fonts.css', 'dist/index.d.ts']);
  assert.ok(read('dist/datacube.js').includes('mountDataCube'), 'the module exports mountDataCube by name');
});

test('its assets are what the module fetches, and no other', () => {
  const assets = files('assets');
  const extensions = assets.filter((f) => f.startsWith('assets/duckdb-extensions/'));
  // DuckDB's extensions as it asks a repository for them: <its version>/<its build>/<name>.duckdb_extension.wasm
  assert.deepEqual(extensions.map((f) => f.replace(/^assets\/duckdb-extensions\/v[\d.]+\/wasm_eh\//, '')),
    ['httpfs', 'icu', 'json', 'parquet'].map((name) => `${name}.duckdb_extension.wasm`));
  assert.deepEqual(assets.filter((f) => !extensions.includes(f)), [
    // legend-lite's compiler: its module, TeaVM's runtime, and the worker the module runs in
    'assets/classes.wasm',
    // DuckDB, the one build the apps ship (WebAssembly exception handling), and its worker
    'assets/duckdb-browser-eh.worker.js',
    'assets/duckdb-eh.wasm',
    'assets/fonts/LICENSE-roboto.txt',
    'assets/fonts/roboto-latin-400-normal.woff2',
    'assets/fonts/roboto-latin-500-normal.woff2',
    'assets/fonts/roboto-latin-700-normal.woff2',
    'assets/fonts/roboto-latin-ext-400-normal.woff2',
    'assets/fonts/roboto-latin-ext-500-normal.woff2',
    'assets/fonts/roboto-latin-ext-700-normal.woff2',
    'assets/planner-worker.js',
    // the saved pages' rules
    'assets/queries/classes.wasm',
    'assets/queries/wasm-gc-module-runtime.js',
    'assets/wasm-gc-module-runtime.js',
  ]);
  for (const f of assets) assert.ok(statSync(at(f)).size > 0, `${f} is empty`);
});

test("the fonts' stylesheet names files of the package", () => {
  const urls = [...read('dist/fonts.css').matchAll(/url\(([^)]+)\)/g)].map((m) => m[1]!);
  assert.ok(urls.length >= 6, `fonts.css names ${urls.length} files`);
  for (const url of urls) assert.ok(isFile(posix.join('dist', url)), `fonts.css names ${url}, which is not in the package`);
});

test('its licences come with it', () => {
  for (const licence of ['LICENSE', 'NOTICE', 'THIRD_PARTY_NOTICES.md']) {
    assert.ok(statSync(at(licence)).size > 1000, `${licence} is not the project's`);
  }
});
