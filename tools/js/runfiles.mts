// A JavaScript test's inputs, found through Bazel's runfiles (Bazel workplan P1-24): never by `../..` arithmetic on
// import.meta.url, never relative to the working directory, never by a hard-coded `_main`.
//
// The BUILD file names each input in `env` with $(rlocationpath <label>) (or $(rlocationpaths ...) for a list), and
// the test resolves it here. rules_js always runs a program inside a runfiles tree and exports its root as
// JS_BINARY__RUNFILES, so the tree is the whole lookup (no manifest-only mode to support, which is the one thing
// @bazel/runfiles would add, and it cannot be imported from the packages here that have no npm dependencies).
import { readFileSync } from 'node:fs';
import { basename, join, posix } from 'node:path';
import { pathToFileURL } from 'node:url';

function root(): string {
  const dir = process.env['JS_BINARY__RUNFILES'] ?? process.env['RUNFILES_DIR'];
  if (!dir) throw new Error('not run by Bazel: neither JS_BINARY__RUNFILES nor RUNFILES_DIR is set');
  return dir;
}

/** The runfiles tree's root, for a child process that needs RUNFILES_DIR (a Java launcher). */
export function runfilesRoot(): string {
  return root();
}

/** The absolute path of the runfile `rlocationpath` (what $(rlocationpath) gives, e.g. `_main/wasm/planner_dir`). */
export function runfile(rlocationpath: string): string {
  return join(root(), rlocationpath);
}

/** The runfile the environment variable `name` names with $(rlocationpath); fails when the BUILD file set none. */
export function runfileFromEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is not set: the test's BUILD target names it in env with $(rlocationpath ...)`);
  return runfile(value);
}

/** Every runfile the environment variable `name` names with $(rlocationpaths) (space-separated). */
export function runfilesFromEnv(name: string): string[] {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is not set: the test's BUILD target names it in env with $(rlocationpaths ...)`);
  return value.split(' ').filter((p) => p.length > 0).map(runfile);
}

/** The one runfile called `file` among those `name` names with $(rlocationpaths). */
export function runfileNamed(name: string, file: string): string {
  const found = runfilesFromEnv(name).filter((p) => basename(p) === file);
  if (found.length !== 1) throw new Error(`${name} names ${found.length} files called ${file}, not one`);
  return found[0]!;
}

/** The directory runfile `name` names, as a file: URL ending in `/` (a module loader's base). */
export function runfileDirUrl(name: string): string {
  const url = pathToFileURL(runfileFromEnv(name)).href;
  return url.endsWith('/') ? url : `${url}/`;
}

/**
 * A source scanner's files (Bazel workplan P3-29): the runfiles the environment variable `name` names with
 * $(rlocationpaths), each by its path from the package `pkg`, with `/` separators on every platform (`src/app.ts`,
 * `../engine-client/src/types.ts`): what the scanner matches on and reports. A scanner walks no directory; what it
 * reads is what its BUILD target declares.
 */
export class Sources {
  readonly #files = new Map<string, string>();

  constructor(name: string, pkg: string) {
    const value = process.env[name];
    if (!value) throw new Error(`${name} is not set: the test's BUILD target names its sources in env with $(rlocationpaths ...)`);
    for (const rlocationpath of value.split(' ').filter((p) => p.length > 0)) {
      // `<repository>/<path in it>`; a scanner reads the main repository's files
      const inRepo = rlocationpath.slice(rlocationpath.indexOf('/') + 1);
      this.#files.set(posix.relative(pkg, inRepo), runfile(rlocationpath));
    }
  }

  /** Every file under `dir` (a path from the package) whose name ends with one of `suffixes`, sorted. */
  under(dir: string, ...suffixes: string[]): string[] {
    const prefix = dir.endsWith('/') ? dir : `${dir}/`;
    return [...this.#files.keys()]
      .filter((f) => f.startsWith(prefix) && (suffixes.length === 0 || suffixes.some((s) => f.endsWith(s))))
      .sort();
  }

  /** Whether `file` is among the declared sources. */
  has(file: string): boolean {
    return this.#files.has(file);
  }

  /** `file`'s text; fails when the BUILD target did not declare it. */
  read(file: string): string {
    const path = this.#files.get(file);
    if (path === undefined) throw new Error(`${file} is not among the declared sources: add it to the test's data and env`);
    return readFileSync(path, 'utf8');
  }
}
