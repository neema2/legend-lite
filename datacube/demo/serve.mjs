// One command: build, serve, print a URL.
//
//   bazel run //datacube:serve
//   bazel run //datacube:serve -- --data ~/trades.parquet
//   bazel run //datacube:serve -- --open
//
// Everything DataCube needs runs in the browser -- the planner is
// WebAssembly and so is DuckDB -- so this is a plain static file
// server. Bazel builds what it serves (//datacube:site: the bundles,
// and both WebAssembly runtimes under demo/vendor), and it serves a
// local data file next to the page so you do not have to host one
// yourself.
//
// It also answers byte-range requests, which `python3 -m http.server`
// does not. DuckDB reads a Parquet footer and then only the row
// groups a query needs; against a server that ignores Range it
// re-downloads the whole file instead.

import { createServer } from 'node:http';
import { execFileSync } from 'node:child_process';
import { open, readFile, stat } from 'node:fs/promises';
import { basename, extname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { servedPath } from './static-files.ts';

const HERE = fileURLToPath(new URL('.', import.meta.url));
const ROOT = resolve(HERE, '..');

const args = process.argv.slice(2);
const flag = (name) => {
  const i = args.indexOf(`--${name}`);
  return i < 0 ? undefined : (args[i + 1]?.startsWith('--') ? '' : args[i + 1]);
};
// `bazel run` starts this in the runfiles tree; a path the user typed
// means the directory they typed it in.
const CALLER = process.env.BUILD_WORKING_DIRECTORY ?? process.cwd();
const dataPath = flag('data') ? resolve(CALLER, flag('data')) : undefined;
const wantOpen = args.includes('--open');
const port = Number(flag('port') ?? 8000);
// LOOPBACK BY DEFAULT.
//
// `listen(port)` with no host binds every interface, which put this
// on the Wi-Fi: anyone on the network could browse the served
// directory, and with --data could read the file it points at,
// through a process running as whoever started it. That is a lot to
// hand out for a convenience server.
//
// Loopback still serves every LOGIN ACCOUNT on this machine --
// 127.0.0.1 belongs to the kernel, not to a session, so another
// user's browser reaches it fine. Only other machines are excluded,
// and `--host 0.0.0.0` says so out loud when that is the intent.
const host = flag('host') || '127.0.0.1';

// ---- serve -----------------------------------------------------------
const TYPES = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript',
  '.mjs': 'text/javascript', '.css': 'text/css', '.wasm': 'application/wasm',
  '.pure': 'text/plain; charset=utf-8', '.json': 'application/json',
  '.map': 'application/json', '.png': 'image/png', '.svg': 'image/svg+xml',
};

/** Serve a file, honouring Range so DuckDB can read parts of it. */
async function sendFile(res, path, range) {
  const st = await stat(path);
  const size = st.size;
  const type = TYPES[extname(path)] ?? 'application/octet-stream';
  // REVALIDATE ALWAYS. Without this the browser heuristically caches
  // bundle.js, and a rebuilt page loads new HTML against old script:
  // the sample dropdown renders empty, the row count sits at its
  // min, and the button does nothing, because the code that fills
  // them in is simply not in the file the browser kept. `no-cache`
  // is revalidation, not "do not store" -- the 36 MB duckdb binary
  // still comes back 304 while its mtime is unchanged.
  const cacheHeaders = {
    'Cache-Control': 'no-cache',
    'Last-Modified': st.mtime.toUTCString(),
  };
  const m = /^bytes=(\d*)-(\d*)$/.exec(range ?? '');
  if (!m) {
    res.writeHead(200, {
      'Content-Type': type,
      'Content-Length': String(size),
      'Accept-Ranges': 'bytes',
      ...cacheHeaders,
    });
    res.end(await readFile(path));
    return;
  }
  const start = m[1] ? Number(m[1]) : 0;
  const end = m[2] ? Number(m[2]) : size - 1;
  const len = Math.max(0, end - start + 1);
  const fh = await open(path, 'r');
  try {
    const buf = Buffer.alloc(len);
    await fh.read(buf, 0, len, start);
    res.writeHead(206, {
      'Content-Type': type,
      'Content-Length': String(len),
      'Content-Range': `bytes ${start}-${end}/${size}`,
      'Accept-Ranges': 'bytes',
      ...cacheHeaders,
    });
    res.end(buf);
  } finally {
    await fh.close();
  }
}

const DATA_ROUTE = dataPath ? `/data${extname(dataPath) || '.parquet'}` : undefined;

const server = createServer(async (req, res) => {
  const [path, query = ''] = (req.url ?? '/').split('?');
  try {
    if (DATA_ROUTE && path === DATA_ROUTE) {
      await sendFile(res, dataPath, req.headers.range);
      return;
    }
    // REDIRECT, never serve the page under a different path. Serving
    // demo/index.html at `/` looks like it works -- the HTML arrives
    // and the shell renders -- but every relative URL in it then
    // resolves against `/`, so ./bundle.js 404s and NO script runs.
    // The result is a page that looks right and does nothing: an
    // empty dropdown, a status stuck on its initial "loading…", and
    // controls that ignore clicks. Reported exactly that way.
    if (path === '/' || path === '/demo' || path === '/demo/') {
      res.writeHead(302, {
        Location: `/demo/index.html${query ? `?${query}` : ''}`,
      });
      res.end();
      return;
    }
    const file = servedPath(ROOT, path);
    if (!file) throw new Error('not under the root');
    await sendFile(res, file, req.headers.range);
  } catch {
    res.writeHead(404, { 'Content-Type': 'text/plain' }).end('not found');
  }
});

server.listen(port, host, () => {
  const base = `http://localhost:${port}`;
  let url = `${base}/demo/index.html`;
  if (DATA_ROUTE) {
    const fmt = extname(dataPath) === '.csv' ? 'csv'
      : extname(dataPath) === '.json' ? 'json' : 'parquet';
    url += `?remote=${encodeURIComponent(base + DATA_ROUTE)}&format=${fmt}`;
    console.log(`\nserving ${basename(dataPath)} and the cube\n`);
  } else {
    console.log('\nserving the cube with generated demo data');
    console.log('(point it at your own with: bazel run //datacube:serve -- --data FILE)\n');
  }
  console.log(`  ${url}\n`);
  if (host === '127.0.0.1' || host === 'localhost') {
    console.log('  (loopback only: every account on this Mac can reach it,'
      + ' other machines cannot.\n   --host 0.0.0.0 to expose it on the'
      + ' network.)\n');
  } else {
    console.log(`  EXPOSED on ${host}: any machine that can route here can`
      + ' read what is served.\n');
  }
  if (wantOpen) {
    const opener = process.platform === 'darwin' ? 'open'
      : process.platform === 'win32' ? 'start' : 'xdg-open';
    try { execFileSync(opener, [url]); } catch { /* not fatal */ }
  }
});
