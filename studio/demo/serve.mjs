// `bazel run //studio:serve [-- --port 8200] [-- --host 0.0.0.0]`: Legend Studio as a static site.
//
// Everything the page runs is in the browser: the app, legend-lite's compiler and (with the default
// config) the SDLC, as WebAssembly. Projects live in this browser, or on the SDLC server
// demo/config-server.json names (`?config=./config-server.json`). Loopback unless told otherwise.

import { createServer } from 'node:http';
import { readFile, stat } from 'node:fs/promises';
import { extname, resolve, sep } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const ROOT = resolve(fileURLToPath(new URL('.', import.meta.url)), '..');
const args = process.argv.slice(2);
const flag = (name) => {
  const i = args.indexOf(`--${name}`);
  return i < 0 ? undefined : args[i + 1];
};
const port = Number(flag('port') ?? 8200);
const host = flag('host') ?? '127.0.0.1';

const TYPES = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.mjs': 'text/javascript',
  '.css': 'text/css', '.wasm': 'application/wasm', '.pure': 'text/plain; charset=utf-8',
  '.json': 'application/json', '.svg': 'image/svg+xml',
};

/** The file under ROOT a request path names, or undefined when it climbs out. */
function servedPath(pathname) {
  const base = pathToFileURL(ROOT.endsWith(sep) ? ROOT : `${ROOT}${sep}`);
  const file = new URL(`.${pathname}`, base);
  if (!file.href.startsWith(base.href) || /%2f|%5c/i.test(file.href)) return undefined;
  return fileURLToPath(file);
}

createServer(async (req, res) => {
  const { pathname, search } = new URL(req.url ?? '/', 'http://x');
  if (pathname === '/' || pathname === '/demo' || pathname === '/demo/') {
    res.writeHead(302, { Location: `/demo/index.html${search}` }).end();
    return;
  }
  const file = servedPath(pathname);
  try {
    if (!file) throw new Error('outside the root');
    const st = await stat(file);
    res.writeHead(200, {
      'Content-Type': TYPES[extname(file)] ?? 'application/octet-stream',
      'Content-Length': String(st.size),
      'Cache-Control': 'no-cache',
    });
    res.end(await readFile(file));
  } catch {
    res.writeHead(404, { 'Content-Type': 'text/plain' }).end('not found');
  }
}).listen(port, host, () => {
  console.log(`Legend Studio: http://${host === '0.0.0.0' ? 'localhost' : host}:${port}/demo/index.html`);
});
