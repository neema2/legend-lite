// Install the browser the harnesses drive: Playwright's own Chromium, at the version this
// package pins (pnpm-lock.yaml), so a laptop and CI run the same one. A browser binary is not
// something Bazel fetches; this is the one command that puts it where Playwright looks.
//
//   bazel run //datacube:install_browser                  the browser
//   bazel run //datacube:install_browser -- --with-deps   and its system libraries (a Linux desk)
import { spawnSync } from 'node:child_process';
import { createRequire } from 'node:module';
import path from 'node:path';

const require = createRequire(import.meta.url);
// the package's own command line, beside its entry point (the package exports no `./cli`)
const cli = path.join(path.dirname(require.resolve('playwright')), 'cli.js');
const run = spawnSync(process.execPath, [cli, 'install', ...process.argv.slice(2), 'chromium'], { stdio: 'inherit' });
process.exit(run.status ?? 1);
