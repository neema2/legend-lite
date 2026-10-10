// THE PACKAGE'S ENTRIES (the build rebuild's L1d): //datacube:app_package holds the app folder's five things -- the
// server, DuckDB's library, its postgres extension, site/ and licenses/ -- under datacube/app/, and nothing else: not the runfiles
// tree Bazel keeps beside the executable (the audit of L1c found the package carrying a second copy of the app that
// way), no file of Bazel's. The archive is read here, by this test, with Node's own gunzip and the tar format's
// 512-byte headers: no host tar.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import { gunzipSync } from 'node:zlib';
import { runfileFromEnv } from '../../../tools/js/runfiles.mts';

type Entry = { name: string; mode: number; type: string; size: number };

// ustar: name at 0 (100 bytes), mode at 100 (8, octal), size at 124 (12, octal), type at 156, prefix at 345 (155)
function entries(tar: Buffer): Entry[] {
  const field = (at: number, len: number) => tar.toString('utf8', at, at + len).replace(/\0.*$/s, '');
  const out: Entry[] = [];
  for (let at = 0; at + 512 <= tar.length; ) {
    if (tar.subarray(at, at + 512).every((b) => b === 0)) break; // the end: two zero blocks
    const prefix = field(at + 345, 155);
    const name = (prefix ? `${prefix}/` : '') + field(at, 100);
    const size = parseInt(field(at + 124, 12) || '0', 8);
    const type = field(at + 156, 1) || '0';
    out.push({ name, mode: parseInt(field(at + 100, 8) || '0', 8), type, size });
    at += 512 + Math.ceil(size / 512) * 512;
  }
  return out;
}

test('the package holds the app folder and nothing else', () => {
  const all = entries(gunzipSync(readFileSync(runfileFromEnv('APP_PACKAGE'))));
  // pax extended headers (type x, g) describe the entry after them; they are not entries of the app
  const files = all.filter((e) => e.type !== 'x' && e.type !== 'g');
  assert.ok(files.length > 4, `too few entries: ${files.map((e) => e.name).join(', ')}`);
  for (const e of files) {
    assert.ok(e.name === 'datacube/' || e.name === 'datacube/app/' || e.name.startsWith('datacube/app/'),
      `an entry outside the app folder: ${e.name}`);
    assert.ok(!/runfiles|repo_mapping/.test(e.name), `a file of Bazel's in the package: ${e.name}`);
  }
  const exe = process.platform === 'win32' ? 'datacube/app/warehouse.exe' : 'datacube/app/warehouse';
  const byName = new Map(files.map((e) => [e.name, e]));
  assert.ok(byName.has(exe), `no server at ${exe}`);
  assert.equal(byName.get(exe)!.mode & 0o100, 0o100, `${exe} is not executable (mode ${byName.get(exe)!.mode.toString(8)})`);
  assert.ok(byName.has('datacube/app/postgres_scanner.duckdb_extension'), 'no postgres extension');
  assert.ok(files.some((e) => e.name.startsWith('datacube/app/libduckdb_java.so')), "no DuckDB library");
  assert.ok(byName.has('datacube/app/site/index.html'), 'no site/index.html');
  // what it redistributes, and on what terms: ours, the notices it carries, and GraalVM's (compiled into the server)
  for (const licence of ['LICENSE', 'NOTICE', 'THIRD_PARTY_NOTICES.md', 'GRAALVM-LICENSE.txt',
    'GRAALVM-LICENSE-NATIVEIMAGE.txt', 'GRAALVM-THIRD-PARTY-LICENSE.txt']) {
    assert.ok(byName.get(`datacube/app/licenses/${licence}`)?.size, `no licenses/${licence}`);
  }
  // the folder's top level: the three files, site/ and licenses/, no sixth thing
  const top = files.filter((e) => /^datacube\/app\/[^/]+\/?$/.test(e.name)).map((e) => e.name.replace('datacube/app/', ''));
  assert.deepEqual(top.sort(), [exe.replace('datacube/app/', ''), ...top.filter((n) => n.startsWith('libduckdb_java.so')), 'postgres_scanner.duckdb_extension', 'site/', 'licenses/'].sort());
});
