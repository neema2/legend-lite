// EVERY package.json EDIT IS RELOCKED (Bazel workplan P1-26): each package's pnpm-lock.yaml importer lists exactly
// the dependencies its package.json declares, each with the same specifier. rules_js installs from the lock, so a
// package.json edit without `bazel run -- @pnpm//:pnpm --dir $PWD/<package> install --lockfile-only` would
// otherwise change nothing, silently. Offline: it reads the two files and nothing else.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { basename, dirname } from 'node:path';

import { runfilesFromEnv } from './runfiles.mts';

type Specifiers = Record<string, string>;

/** The `.` importer's dependencies and devDependencies, name -> specifier, from a pnpm v9 lock. */
function lockedSpecifiers(lock: string): Specifiers {
  const start = lock.indexOf('\nimporters:\n');
  const end = lock.indexOf('\npackages:\n', start);
  assert.ok(start >= 0 && end > start, 'the lock has no importers section before packages');
  const out: Specifiers = {};
  let name: string | undefined;
  for (const line of lock.slice(start, end).split('\n')) {
    const dep = /^ {6}'?([^':]+)'?:$/.exec(line);
    if (dep) name = dep[1];
    const spec = /^ {8}specifier: (.+)$/.exec(line);
    if (spec && name !== undefined) out[name] = spec[1]!.replace(/^'(.*)'$/, '$1');
  }
  return out;
}

const packages = runfilesFromEnv('PACKAGE_JSONS');
const locks = runfilesFromEnv('PNPM_LOCKS');

test('a lock for every package.json', () => {
  assert.deepEqual(locks.map((l) => basename(dirname(l))).sort(), packages.map((p) => basename(dirname(p))).sort());
});

for (const pkgPath of packages) {
  const dir = basename(dirname(pkgPath));
  test(`${dir}: pnpm-lock.yaml matches package.json`, () => {
    const pkg = JSON.parse(readFileSync(pkgPath, 'utf8')) as { dependencies?: Specifiers; devDependencies?: Specifiers };
    const declared: Specifiers = { ...pkg.dependencies, ...pkg.devDependencies };
    const lockPath = locks.find((l) => basename(dirname(l)) === dir)!;
    assert.deepEqual(lockedSpecifiers(readFileSync(lockPath, 'utf8')), declared,
      `${dir}/package.json and its pnpm-lock.yaml disagree: relock with \`bazel run -- @pnpm//:pnpm --dir $PWD/${dir} install --lockfile-only\``);
  });
}
