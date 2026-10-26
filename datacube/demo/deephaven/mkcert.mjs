// THE HAND TOOLS' CERTIFICATE (the user, 2026-10-26: "build it with mkcert"): the Deephaven a hand tool starts
// (serve.mjs, market/serve.mjs, market/desk.mjs) serves a certificate mkcert makes for localhost and 127.0.0.1, signed
// by mkcert's local CA. A browser on a machine where `mkcert -install` ran trusts it and asks nothing -- and the page's
// own requests to Deephaven, which no browser lets a person accept, just work. It is made each run (the trust is in the
// CA, so nothing need be kept), under the system's temporary directory, removed when the tool exits. Without mkcert the
// fixture makes its own, as the tests' does, and the browser asks to accept it: said plainly, with how to get mkcert.
// The tests never come here: theirs is made each run and accepted by the test's browser.

import { spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

/** The identity a hand tool's Deephaven serves: `args` for startDeephaven (none without mkcert), and `said`, what to
 *  print after the server's URL. */
export function mkcertIdentity() {
  const dir = mkdtempSync(join(tmpdir(), 'deephaven-identity-'));
  process.on('exit', () => rmSync(dir, { recursive: true, force: true }));
  const made = spawnSync('mkcert', ['-cert-file', join(dir, 'cert.pem'), '-key-file', join(dir, 'key.pem'),
    'localhost', '127.0.0.1', '::1'], { encoding: 'utf8' });
  if (made.error?.code === 'ENOENT') {
    return {
      args: [],
      said: '(its certificate is the test server\'s own: open this URL first and accept it, or the page cannot reach'
        + ' the server. To be asked nothing, install mkcert -- `brew install mkcert` on a Mac -- run `mkcert -install`'
        + ' once, and start again)',
    };
  }
  if (made.error || made.status !== 0) {
    throw new Error(`mkcert made no certificate: ${made.error?.message ?? made.stderr}`);
  }
  // mkcert says which of this machine's trust stores lack its CA; the Java one is no browser's
  const notes = made.stderr.split('\n').filter((l) => l.startsWith('Note:') && !l.includes('Java'));
  return {
    args: [`--jvm_flag=-Dembedded.deephaven.identity=${dir}`],
    said: notes.length === 0
      ? '(its certificate is mkcert\'s, and this machine trusts mkcert\'s CA: the browser asks nothing)'
      : `(its certificate is mkcert's, but mkcert says:\n  ${notes.join('\n  ')}\n`
        + '  Run `mkcert -install` once to be asked nothing; until then a browser reading that store asks to accept it:'
        + ' open this URL first)',
  };
}
