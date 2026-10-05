// THE BROWSER HARNESSES' ONE MODULE (Bazel workplan P4-01): what every harness here, Query's and the site's used to
// copy -- a static server, where the site is, where a test's artifacts and temp files go, and the check counter.
//
//   serve(root, { route })  the site on 127.0.0.1, port 0, honouring Range (DuckDB reads Parquet in parts); `route`
//                           answers what is not a file (a mock engine, a data route) and returns true when it did
//   siteRoot(name)          the served root, from the build: env <name> (default SITE) names a file of the site by
//                           its runfiles path ($(rlocationpath demo/index.html)), never this file's own location
//   outPath(name)           a test's artifact: under TEST_UNDECLARED_OUTPUTS_DIR (a scratch directory under bazel run)
//   tmpDir(prefix)          a fresh directory under TEST_TMPDIR (the host's temp under bazel run)
//   checks()                check(name, ok, detail) and done(): exit 1 on a failure, or when nothing was checked

import { createServer } from 'node:http';
import { mkdirSync, mkdtempSync } from 'node:fs';
import { mkdtemp, open, readFile, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, extname, join } from 'node:path';

import { runfileFromEnv } from '../../tools/js/runfiles.mts';
import { servedPath } from './static-files.ts';

/** One MIME map for every harness. */
export const TYPES = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.mjs': 'text/javascript',
  '.css': 'text/css', '.wasm': 'application/wasm', '.pure': 'text/plain; charset=utf-8',
  '.json': 'application/json', '.map': 'application/json', '.png': 'image/png', '.svg': 'image/svg+xml',
  '.csv': 'text/csv', '.woff2': 'font/woff2', '.parquet': 'application/octet-stream',
};

/** Send `path`, honouring a `bytes=a-b` Range (206) so DuckDB can read parts of a file. */
export async function sendFile(res, path, range, headers = {}) {
  const st = await stat(path);
  const size = st.size;
  const type = TYPES[extname(path)] ?? 'application/octet-stream';
  const m = /^bytes=(\d*)-(\d*)$/.exec(range ?? '');
  if (!m) {
    res.writeHead(200, { 'Content-Type': type, 'Content-Length': String(size), 'Accept-Ranges': 'bytes', ...headers });
    res.end(await readFile(path));
    return;
  }
  const start = m[1] ? Number(m[1]) : 0;
  const end = m[2] ? Math.min(Number(m[2]), size - 1) : size - 1;
  const len = Math.max(0, end - start + 1);
  const fh = await open(path, 'r');
  try {
    const buf = Buffer.alloc(len);
    await fh.read(buf, 0, len, start);
    res.writeHead(206, {
      'Content-Type': type, 'Content-Length': String(len), 'Content-Range': `bytes ${start}-${end}/${size}`,
      'Accept-Ranges': 'bytes', ...headers,
    });
    res.end(buf);
  } finally {
    await fh.close();
  }
}

/**
 * Serve `root` on 127.0.0.1, port 0 (or `port`, for a harness whose page origin a server must allow). `route(req,
 * res, url)` is asked first and returns true when it answered; `headers` ride every file response. Resolves to
 * `{ port, origin, close }`.
 */
export async function serve(root, { route, headers = {}, port: listenOn = 0 } = {}) {
  const server = createServer(async (req, res) => {
    try {
      const url = new URL(req.url ?? '/', 'http://127.0.0.1');
      if (route && await route(req, res, url)) return;
      const file = servedPath(root, req.url);
      if (!file) throw new Error('not under the root');
      await sendFile(res, file, req.headers.range, headers);
    } catch {
      if (!res.headersSent) res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('not found');
    }
  });
  await new Promise((resolve) => server.listen(listenOn, '127.0.0.1', resolve));
  const { port } = server.address();
  return {
    port,
    origin: `http://127.0.0.1:${port}`,
    close: () => new Promise((resolve) => server.close(() => resolve())),
  };
}

/** The served root: the directory two levels above the site file env `name` names (datacube/demo/index.html). */
export function siteRoot(name = 'SITE') {
  return dirname(dirname(runfileFromEnv(name)));
}

let scratch;

/** A test artifact's path: under Bazel's undeclared-outputs directory, or a scratch directory under bazel run. */
export function outPath(name) {
  let dir = process.env['TEST_UNDECLARED_OUTPUTS_DIR'];
  if (!dir) {
    scratch ??= mkdtempSync(join(tmpdir(), 'harness-out-'));
    dir = scratch;
  }
  const p = join(dir, name);
  mkdirSync(dirname(p), { recursive: true });
  return p;
}

/** A fresh directory under the test's temp (TEST_TMPDIR), or the host's under bazel run. */
export function tmpDir(prefix) {
  return mkdtemp(join(process.env['TEST_TMPDIR'] ?? tmpdir(), prefix));
}

/** The check counter: `check(name, ok, detail)` prints a line; `done()` exits 1 on a failure or on zero checks. */
export function checks() {
  let failed = 0;
  let total = 0;
  return {
    check(name, ok, detail = '') {
      total++;
      if (!ok) failed++;
      console.log(`${ok ? 'ok  ' : 'FAIL'}  ${name}${detail ? ` — ${detail}` : ''}`);
      return ok;
    },
    get failed() { return failed > 0; },
    done() {
      console.log(`\n${total - failed}/${total} checks passed`);
      if (total === 0) {
        console.log('FAIL  nothing was checked');
        process.exit(1);
      }
      process.exit(failed ? 1 : 0);
    },
  };
}

/** `n` animation frames in `page`: what a layout change (a resized viewport, a re-render) needs before it is
 *  measured. An awaited condition, never a fixed sleep (G-11). */
export async function frames(page, n = 2) {
  await page.evaluate((k) => new Promise((resolve) => {
    const step = (i) => (i === 0 ? resolve() : requestAnimationFrame(() => step(i - 1)));
    step(k);
  }), n);
}
