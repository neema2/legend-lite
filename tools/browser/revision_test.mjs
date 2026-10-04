// The pinned Chromium (MODULE.bazel) is the one EVERY locked Playwright expects: each lock's
// playwright-core browsers.json names the chromium-headless-shell revision, and the pinned
// archive's directory carries it. A Playwright bump that moves the revision fails here, at once,
// without launching anything. LOCKS: a package directory whose node_modules has playwright.
import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';

const pinned = process.env.PINNED_CHROMIUM;
const have = /chromium_headless_shell-(\d+)\//.exec(pinned ?? '')?.[1];
if (!have) throw new Error(`no revision in PINNED_CHROMIUM=${pinned}`);
let bad = 0;
for (const pkg of process.env.LOCKS.split(' ')) {
  const fromPkg = createRequire(join(process.env.JS_BINARY__RUNFILES, '_main', pkg, 'node_modules', 'x.js'));
  // the package exports neither file: read them beside its resolved entry point
  const coreDir = dirname(createRequire(fromPkg.resolve('playwright')).resolve('playwright-core'));
  const read = (f) => JSON.parse(readFileSync(join(coreDir, f), 'utf8'));
  const shell = read('browsers.json').browsers.find((b) => b.name === 'chromium-headless-shell');
  const ok = shell.revision === have;
  bad += ok ? 0 : 1;
  console.log(`${ok ? 'ok ' : 'BAD'} ${pkg}: playwright-core ${read('package.json').version} wants `
    + `chromium-headless-shell ${shell.revision} (${shell.browserVersion}); pinned ${have}`);
}
process.exit(bad ? 1 : 0);
