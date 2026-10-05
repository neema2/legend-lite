// `bazel run //site:serve [-- --port 8200] [-- --host 0.0.0.0]`: Legend Query, DataCube and Studio from one
// origin (BUILD.bazel says why), as a static site -- everything the apps run is in the browser.

import { createServer } from 'node:http';
import { readFile, stat } from 'node:fs/promises';
import { extname, resolve, sep } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const ROOT = resolve(fileURLToPath(new URL('.', import.meta.url)), 'dist');
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
  '.sql': 'text/plain; charset=utf-8', '.json': 'application/json', '.svg': 'image/svg+xml',
  '.woff2': 'font/woff2',
};
/** Each app's page, where its own server would put it. */
const PAGES = { '/query': '/query/demo/index.html', '/datacube': '/datacube/demo/index.html', '/studio': '/studio/demo/index.html' };

/** The file under ROOT a request path names, or undefined when it climbs out. */
function servedPath(pathname) {
  const base = pathToFileURL(ROOT.endsWith(sep) ? ROOT : `${ROOT}${sep}`);
  const file = new URL(`.${pathname}`, base);
  if (!file.href.startsWith(base.href) || /%2f|%5c/i.test(file.href)) return undefined;
  return fileURLToPath(file);
}

createServer(async (req, res) => {
  const { pathname, search } = new URL(req.url ?? '/', 'http://x');
  const app = pathname.replace(/\/(demo\/?)?$/, '');
  if (PAGES[app]) {
    res.writeHead(302, { Location: `${PAGES[app]}${search}` }).end();
    return;
  }
  const file = servedPath(pathname === '/' ? '/index.html' : pathname);
  try {
    if (!file || !(await stat(file)).isFile()) throw new Error('not a file');
    res.writeHead(200, { 'Content-Type': TYPES[extname(file)] ?? 'application/octet-stream', 'Cache-Control': 'no-cache' });
    res.end(await readFile(file));
  } catch {
    res.writeHead(404, { 'Content-Type': 'text/plain' }).end('not found');
  }
}).listen(port, host, () => {
  console.log(`Legend Query, DataCube and Studio, one origin: http://${host === '0.0.0.0' ? 'localhost' : host}:${port}/`);
  console.log(`  Query     http://localhost:${port}/query/`);
  console.log(`  DataCube  http://localhost:${port}/datacube/`);
  console.log(`  Studio    http://localhost:${port}/studio/`);
});
