// The pinned Chromium (MODULE.bazel's chromium.pin) is the one EVERY locked Playwright expects: each lock's
// playwright-core browsers.json names chromium-headless-shell's revision and Chrome for Testing version, and
// both must equal the pin's (PIN: @chromium_pin//:pin.json). A Playwright bump that moves either fails here,
// at once, without launching or downloading anything.
//
// LOCKS names the pnpm locks checked, by runfiles path; each lock's package has playwright in its node_modules.
// The list is held to the repository (G19, Bazel workplan P6-19): every pnpm-lock.yaml in the inventory
// (INVENTORY: @repo_inventory//:files.txt) must be one of them, so a new lock cannot go unchecked.
import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';

import { runfile, runfileFromEnv } from '../js/runfiles.mts';

const pin = JSON.parse(readFileSync(runfileFromEnv('PIN'), 'utf8'));
const locks = process.env.LOCKS.split(' ').filter((l) => l.length > 0);
let bad = 0;

// every lock in the repository is checked
const inventoried = readFileSync(runfileFromEnv('INVENTORY'), 'utf8').split('\n')
  .filter((f) => f === 'pnpm-lock.yaml' || f.endsWith('/pnpm-lock.yaml')).sort();
const checked = process.env.LOCK_PATHS.split(' ').filter((l) => l.length > 0).sort();
if (JSON.stringify(inventoried) !== JSON.stringify(checked)) {
  bad += 1;
  console.log(`BAD the repository's pnpm locks ${JSON.stringify(inventoried)} are not the checked ${JSON.stringify(checked)}`);
}

for (const lock of locks) {
  const fromPkg = createRequire(join(dirname(runfile(lock)), 'node_modules', 'x.js'));
  // the package exports neither file: read them beside its resolved entry point
  const coreDir = dirname(createRequire(fromPkg.resolve('playwright')).resolve('playwright-core'));
  const read = (f) => JSON.parse(readFileSync(join(coreDir, f), 'utf8'));
  const shell = read('browsers.json').browsers.find((b) => b.name === 'chromium-headless-shell');
  const ok = shell.revision === pin.revision && shell.browserVersion === pin.version;
  bad += ok ? 0 : 1;
  console.log(`${ok ? 'ok ' : 'BAD'} ${dirname(lock)}: playwright-core ${read('package.json').version} wants `
    + `chromium-headless-shell ${shell.revision} (${shell.browserVersion}); pinned ${pin.revision} (${pin.version})`);
}
process.exit(bad ? 1 : 0);
