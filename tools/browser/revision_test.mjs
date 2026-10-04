// The pinned Chromium (MODULE.bazel's chromium.pin) is the one EVERY locked Playwright expects: each lock's
// playwright-core browsers.json names chromium-headless-shell's revision and Chrome for Testing version, and
// both must equal the pin's (PIN: @chromium_pin//:pin.json). A Playwright bump that moves either fails here,
// at once, without launching or downloading anything. LOCKS: package directories whose node_modules have
// playwright.
import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';

const RUNFILES = process.env.JS_BINARY__RUNFILES;
const pin = JSON.parse(readFileSync(join(RUNFILES, process.env.PIN), 'utf8'));
let bad = 0;
for (const pkg of process.env.LOCKS.split(' ')) {
  const fromPkg = createRequire(join(RUNFILES, '_main', pkg, 'node_modules', 'x.js'));
  // the package exports neither file: read them beside its resolved entry point
  const coreDir = dirname(createRequire(fromPkg.resolve('playwright')).resolve('playwright-core'));
  const read = (f) => JSON.parse(readFileSync(join(coreDir, f), 'utf8'));
  const shell = read('browsers.json').browsers.find((b) => b.name === 'chromium-headless-shell');
  const ok = shell.revision === pin.revision && shell.browserVersion === pin.version;
  bad += ok ? 0 : 1;
  console.log(`${ok ? 'ok ' : 'BAD'} ${pkg}: playwright-core ${read('package.json').version} wants `
    + `chromium-headless-shell ${shell.revision} (${shell.browserVersion}); pinned ${pin.revision} (${pin.version})`);
}
process.exit(bad ? 1 : 0);
