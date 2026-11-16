// npm packs the release using the same pinned Node/npm toolchain as the consumer test. A private cache and empty
// user/global configs make this an offline action; package lifecycle scripts are never run.
import { chmod, cp, mkdir, mkdtemp, readFile, readdir, rename, rm, writeFile } from 'node:fs/promises';
import { dirname, join, relative, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';

const [npmCli, packageDir, archive] = process.argv.slice(2);
if (!npmCli || !packageDir || !archive || process.argv.length !== 5) {
  throw new Error('usage: archive.mjs <npm-cli.js> <package-directory> <output.tgz>');
}
const output = resolve(archive);
await mkdir(dirname(output), { recursive: true });
const scratch = await mkdtemp(join(dirname(output), '.npm-pack-'));
try {
  const userConfig = join(scratch, 'user.npmrc');
  const globalConfig = join(scratch, 'global.npmrc');
  await writeFile(userConfig, '');
  await writeFile(globalConfig, '');
  // npm intentionally excludes symlinks. Bazel presents action inputs as symlinks even when the package action
  // wrote real files, so pack a materialized copy, as a consumer receives it.
  const materialized = join(scratch, 'package');
  await cp(resolve(packageDir), materialized, { recursive: true, dereference: true });
  // Bazel marks every output executable/read-only. npm should retain an executable bit only on the CLI.
  async function permissions(directory) {
    await chmod(directory, 0o755);
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) await permissions(path);
      else await chmod(path, relative(materialized, path).replaceAll('\\', '/') === 'bin/copy-assets.mjs' ? 0o755 : 0o644);
    }
  }
  await permissions(materialized);
  const packed = spawnSync(process.execPath, [
    resolve(npmCli), 'pack', materialized, '--json', '--offline', '--ignore-scripts',
    '--pack-destination', scratch, '--cache', join(scratch, 'cache'),
    '--userconfig', userConfig, '--globalconfig', globalConfig,
  ], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
  if (packed.error) throw packed.error;
  if (packed.status !== 0) throw new Error(`npm pack failed (${packed.status}):\n${packed.stderr}\n${packed.stdout}`);
  const results = JSON.parse(packed.stdout);
  if (results.length !== 1 || !results[0].filename) throw new Error('npm pack must create exactly one archive');
  const metadata = JSON.parse(await readFile(join(packageDir, 'package.json'), 'utf8'));
  if (results[0].name !== metadata.name || results[0].version !== metadata.version) {
    throw new Error('npm packed a different package');
  }
  await rename(join(scratch, results[0].filename), output);
} finally {
  await rm(scratch, { recursive: true, force: true });
}
