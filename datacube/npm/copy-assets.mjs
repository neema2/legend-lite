#!/usr/bin/env node
// A consumer's build copies the version-matched runtimes to its own static host. No network, package manager,
// Java, or Bazel is involved. A dedicated destination can be safely regenerated after each package upgrade.
import { cp, mkdir, readdir, realpath } from 'node:fs/promises';
import { dirname, isAbsolute, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const [destination, ...extra] = process.argv.slice(2);
if (!destination || extra.length || destination.startsWith('-')) {
  console.error('usage: legend-datacube-assets <destination-directory>');
  process.exitCode = 2;
} else {
  const source = await realpath(fileURLToPath(new URL('../assets/', import.meta.url)));
  const target = resolve(destination);
  // Resolve existing parents as well: a symlink into the source would otherwise recursively copy it into itself.
  async function canonical(path) {
    try {
      return await realpath(path);
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
      const parent = dirname(path);
      if (parent === path) throw error;
      return resolve(await canonical(parent), relative(parent, path));
    }
  }
  const resolvedTarget = await canonical(target);
  const inside = relative(source, resolvedTarget);
  if (inside === '' || (inside !== '..' && !inside.startsWith(`..${sep}`) && !isAbsolute(inside))) {
    throw new Error('the asset destination must be outside the installed package assets');
  }
  await mkdir(target, { recursive: true });
  for (const entry of await readdir(source)) {
    await cp(resolve(source, entry), resolve(target, entry), { recursive: true, dereference: true });
  }
  console.log(`DataCube browser assets copied to ${target}`);
}
