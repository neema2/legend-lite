// A JavaScript test's inputs, found through Bazel's runfiles (Bazel workplan P1-24): never by `../..` arithmetic on
// import.meta.url, never relative to the working directory, never by a hard-coded `_main`.
//
// The BUILD file names each input in `env` with $(rlocationpath <label>) (or $(rlocationpaths ...) for a list), and
// the test resolves it here. rules_js always runs a program inside a runfiles tree and exports its root as
// JS_BINARY__RUNFILES, so the tree is the whole lookup (no manifest-only mode to support, which is the one thing
// @bazel/runfiles would add, and it cannot be imported from the packages here that have no npm dependencies).
import { basename, join } from 'node:path';
import { pathToFileURL } from 'node:url';

function root(): string {
  const dir = process.env['JS_BINARY__RUNFILES'] ?? process.env['RUNFILES_DIR'];
  if (!dir) throw new Error('not run by Bazel: neither JS_BINARY__RUNFILES nor RUNFILES_DIR is set');
  return dir;
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
