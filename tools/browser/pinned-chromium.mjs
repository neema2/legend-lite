// Point Playwright at the Chromium Bazel fetched (//tools/browser:chromium_headless_shell), never
// at a cache in $HOME. Import this BEFORE 'playwright': Playwright reads PLAYWRIGHT_BROWSERS_PATH
// when its registry module loads, and ES modules evaluate in import order.
//
// PINNED_CHROMIUM is the executable's rlocationpath (browser_test sets it), e.g.
//   chromium_headless_shell_mac_arm64/chromium_headless_shell-1243/chrome-headless-shell-mac-arm64/chrome-headless-shell
// Its grandparent's parent is the browsers path; the revision directory between them is the name
// Playwright's registry derives from the locked playwright-core's browsers.json, so a pin that
// drifts from the lock fails at launch ("Executable doesn't exist") instead of running another
// browser.
import { existsSync, realpathSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';

const pinned = process.env.PINNED_CHROMIUM;
// Bazel gives a test HOME = TEST_TMPDIR but passes the host's TMPDIR through on macOS
// (/var/folders/...), where Playwright would put the browser profile and artifacts: keep them in
// the test's own directory. os.tmpdir() reads TMPDIR on every call, and on Windows TEMP and TMP instead
// (CI, 2026-10-04: the profile went to the runner's AppData\Local\Temp).
if (process.env.TEST_TMPDIR) {
  process.env.TMPDIR = process.env.TEST_TMPDIR;
  process.env.TEMP = process.env.TEST_TMPDIR;
  process.env.TMP = process.env.TEST_TMPDIR;
}
if (pinned) {
  const runfiles = process.env.JS_BINARY__RUNFILES ?? process.env.RUNFILES_DIR
    ?? resolve(process.cwd(), '..');
  const exe = join(runfiles, pinned);
  if (!existsSync(exe)) throw new Error(`the pinned Chromium is not in runfiles: ${exe}`);
  let browsers = dirname(dirname(dirname(exe)));
  if (process.platform === 'win32') {
    // Windows starts a process only by a path under MAX_PATH (260): CreateProcess reported a 277-character
    // runfiles path as ENOENT (CI, 2026-10-04) though the file was there. In a runfiles tree the FILES are
    // symlinks (the directories are real): the executable's link points into the archive Bazel fetched,
    // under the output base's external/, a far shorter path. Its browsers path is three levels up.
    const real = realpathSync.native(exe);
    browsers = dirname(dirname(dirname(real)));
    if (real.length >= 260) {
      throw new Error(`the pinned Chromium's path is ${real.length} characters, and Windows starts nothing`
        + ` past 259: ${real} (a shorter --output_user_root shortens it)`);
    }
    console.error(`pinned chromium on Windows: ${real} (${real.length} characters)`);
  }
  process.env.PLAYWRIGHT_BROWSERS_PATH = browsers;
  // Playwright's own guard against a browser without its system libraries is a host `ldd`
  // walk keyed to Ubuntu/Debian package names; the test's own launch is the real check.
  process.env.PLAYWRIGHT_SKIP_VALIDATE_HOST_REQUIREMENTS ??= '1';
  console.error(`pinned chromium: ${exe} (browsers path ${browsers})`);
} else if (process.env.TEST_SRCDIR) {
  throw new Error('a browser test without PINNED_CHROMIUM: use browser_test (//tools/browser:defs.bzl)');
}
