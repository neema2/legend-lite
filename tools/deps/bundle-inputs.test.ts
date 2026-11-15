// Every module a browser bundle reads is one it declares (2026-11-15, the C5-T final audit's finding from the Java
// worker's gate). esbuild runs in a sandbox that holds a bundle's declared inputs, but it resolves each one's symlink to
// the real output tree and looks for that file's relative imports THERE. So a module the bundle never declared is found
// whenever some other action has copied it into bazel-bin, and the bundle builds or not depending on what was built
// first: //engine-client:planner_worker named three files by hand and read a fourth, which //datacube:verify_join_test
// found once bazel-bin/engine-client was cleared.
//
// Each bundle writes, in its `bundle_inputs` output group (tools/js/esbuild.bzl), esbuild's metafile (every module it
// read) and the files it declares; guards_package collects them in every package, so a new bundle is read without a
// list to update. A read is mapped to its path in the workspace whichever way esbuild spelled it: from the bundle's
// directory in the output tree, or through the sandbox's symlink to the real output tree. An npm package's file is
// declared by its package's directory (a tree artifact, listed with a trailing `/`): esbuild finds an undeclared
// package as it finds an undeclared module (2026-11-15, the C5-T re-check's item 4). A read from anywhere else is a
// failure, named.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { posix } from 'node:path';
import { describe, it } from 'node:test';

import { runfilesFromEnv } from '../js/runfiles.mts';

/** A read's path in the workspace (an external repository's under `external/`), or as esbuild spelled one outside it. */
export function workspacePath(pkg: string, read: string): string {
  const path = read.replaceAll('\\', '/');
  // through the sandbox's symlink: the real output tree, at any depth
  const bin = /(?:^|\/)bazel-out\/[^/]+\/bin\/(.+)$/.exec(path);
  return bin !== null
    ? bin[1]!
    : path.startsWith('/') || /^[A-Za-z]:\//.test(path)
      ? path
      : posix.normalize(posix.join(pkg, path));
}

/** The modules a bundle read that it does not declare, by path in the workspace (or as esbuild spelled one outside it):
 *  declared as a file, or under a declared directory (an npm package's). */
export function undeclared(pkg: string, declared: ReadonlySet<string>, reads: readonly string[]): string[] {
  const directories = [...declared].filter((d) => d.endsWith('/'));
  const missing = new Set<string>();
  for (const read of reads) {
    // a module esbuild made itself, in a namespace of its own (`(disabled):fs`, `dataurl:...`), is no file
    if (/^(?:\([^)]*\)|[a-z][\w-]+):/i.test(read)) continue;
    const path = workspacePath(pkg, read);
    if (!declared.has(path) && !directories.some((d) => path.startsWith(d))) missing.add(path);
  }
  return [...missing].sort();
}

interface Bundle {
  readonly label: string;
  readonly pkg: string;
  readonly declared: ReadonlySet<string>;
  readonly reads: readonly string[];
}

function bundles(): Bundle[] {
  const files = runfilesFromEnv('BUNDLE_INPUTS');
  const lists = files.filter((f) => f.endsWith('.esbuild-inputs.txt'));
  assert.equal(files.length, 2 * lists.length, 'each bundle gives its declared files and its metafile, and nothing else');
  return lists.map((list) => {
    // its label, its package, its metafile's path in the workspace, then the files it declares
    const [label, pkg, metafile, ...declared] = readFileSync(list, 'utf8')
      .split('\n')
      .filter((l, i) => i < 3 || l.length > 0);
    const meta = files.filter((f) => f.replaceAll('\\', '/').endsWith(`/${metafile}`));
    assert.equal(meta.length, 1, `${label}'s metafile, ${metafile}, is in its output group`);
    const { inputs } = JSON.parse(readFileSync(meta[0]!, 'utf8')) as { inputs: Record<string, unknown> };
    return { label: label!, pkg: pkg!, declared: new Set(declared), reads: Object.keys(inputs) };
  });
}

describe('every module a bundle reads is one it declares', () => {
  it('holds for every bundle in the repository', () => {
    const all = bundles();
    // the incident's bundle is among them: the collection reaches every package
    assert.ok(all.some((b) => b.label === '@@//engine-client:planner_worker' || b.label === '//engine-client:planner_worker'),
      `the bundles read: ${all.map((b) => b.label).join(', ')}`);
    const failures = all.flatMap((b) => {
      const missing = undeclared(b.pkg, b.declared, b.reads);
      return missing.length === 0 ? [] : [`${b.label} reads ${missing.join(', ')}, which it does not declare`];
    });
    assert.deepEqual(failures, []);
  });

  it('reads every spelling esbuild gives a module (planted)', () => {
    const declared = new Set([
      'engine-client/src/legend/planner-worker.ts',
      'engine-client/src/legend/planner-answer.ts',
      // an npm package, declared by its directory
      'engine-client/node_modules/.aspect_rules_js/apache-arrow@17.0.0/node_modules/apache-arrow/',
    ]);
    const reads = [
      // in the bundle's directory, as an unsandboxed build spells it
      'src/legend/planner-worker.ts',
      // through the sandbox's symlink to the real output tree: the incident, a file another action had copied there
      '../../../../../../../../../execroot/_main/bazel-out/k8-fastbuild/bin/engine-client/src/legend/planner-answer.ts',
      '../../../../../../../../../execroot/_main/bazel-out/k8-fastbuild/bin/engine-client/src/plan-guard.ts',
      // Windows' separators
      '..\\..\\execroot\\_main\\bazel-out\\x64_windows-fastbuild\\bin\\engine-client\\src\\run-frame.ts',
      // a declared npm package's file, and one of a package the bundle does not declare (the re-check's item 4)
      '../../../../../../../../../execroot/_main/bazel-out/k8-fastbuild/bin/engine-client/node_modules/.aspect_rules_js/apache-arrow@17.0.0/node_modules/apache-arrow/Arrow.mjs',
      '../../../../../../../../../execroot/_main/bazel-out/k8-fastbuild/bin/engine-client/node_modules/.aspect_rules_js/flatbuffers@24.12.23/node_modules/flatbuffers/mjs/builder.js',
      // a package's directory name is no prefix of another's: apache-arrow/ declares nothing of apache-arrow-extra
      'node_modules/.aspect_rules_js/apache-arrow@17.0.0/node_modules/apache-arrow-extra/index.mjs',
      // esbuild's own namespace: no file
      '(disabled):fs',
      // outside the output tree altogether
      '../../../../../../../../../../home/someone/legend-lite/engine-client/src/records.ts',
    ];
    assert.deepEqual(undeclared('engine-client', declared, reads), [
      // normalized from the bundle's directory, it climbs out of the workspace: named as it resolves
      posix.normalize(posix.join('engine-client', reads[8]!)),
      'engine-client/node_modules/.aspect_rules_js/apache-arrow@17.0.0/node_modules/apache-arrow-extra/index.mjs',
      'engine-client/node_modules/.aspect_rules_js/flatbuffers@24.12.23/node_modules/flatbuffers/mjs/builder.js',
      'engine-client/src/plan-guard.ts',
      'engine-client/src/run-frame.ts',
    ]);
  });
});
