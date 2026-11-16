// Bazel supplies built artifacts by path. This packaging step copies only the release files, dereferencing Bazel's
// links so npm users never depend on a checkout, runfiles tree, or build tools.
import { chmod, cp, mkdir, readFile, writeFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';

const [root, output] = process.argv.slice(2);
if (!root || !output || process.argv.length !== 4) throw new Error('usage: make-package.mjs <datacube-root> <output>');
const source = resolve(root);
const target = resolve(output);
const copy = (from, to) => cp(from, to, { recursive: true, dereference: true });
await mkdir(join(target, 'dist'), { recursive: true });
await mkdir(join(target, 'bin'), { recursive: true });
for (const name of ['browser.js', 'styles.css', 'chunks']) {
  await copy(join(source, 'npm-build', name), join(target, 'dist', name));
}
// tsc emits the implementation's transitive types too. Only the deliberately self-contained browser facade is
// public; the isolated consumer's compiler (without skipLibCheck) proves it needs none of those internal files.
await copy(join(source, 'npm-types', 'datacube', 'src', 'browser.d.ts'), join(target, 'dist', 'browser.d.ts'));
await copy(join(source, 'npm', 'styles.d.ts'), join(target, 'dist', 'styles.d.ts'));
await copy(join(source, 'npm-assets'), join(target, 'assets'));
await copy(join(source, 'npm', 'copy-assets.mjs'), join(target, 'bin', 'copy-assets.mjs'));
await chmod(join(target, 'bin', 'copy-assets.mjs'), 0o755);
for (const name of ['README.md', 'package.json']) {
  await copy(join(source, 'npm', name), join(target, name));
}
for (const name of ['LICENSE', 'NOTICE', 'THIRD_PARTY_NOTICES.md']) {
  await copy(join(source, 'npm-licenses', name), join(target, name));
}
// One release version describes the wrapper and every binary copied from this build, useful when reporting an
// asset path accidentally left on an older deployment. The runtime itself still validates its own module loading.
const metadata = JSON.parse(await readFile(join(target, 'package.json'), 'utf8'));
await writeFile(join(target, 'assets', 'version.json'), JSON.stringify({ name: metadata.name, version: metadata.version }) + '\n');
