// A local HTTP shim: legend-engine's pure/v1 routes answered by legend-lite's engine module (WebAssembly),
// in-process, so legend-studio's own tests can call it as they call an engine server.
// usage: node shim.mjs <dir holding classes.wasm + wasm-gc-module-runtime.js> <port> [engineBaseUrl]
// Routes the module does not answer (the two protocol GET routes) are proxied to engineBaseUrl, and counted.
import http from 'node:http';
import { gunzipSync, inflateSync } from 'node:zlib';
import { pathToFileURL } from 'node:url';
import { resolve } from 'node:path';

const [dir, portArg, engineBase = 'http://localhost:6500'] = process.argv.slice(2);
const { load } = await import(pathToFileURL(resolve(dir, 'wasm-gc-module-runtime.js')).href);
const t0 = performance.now();
const m = await load(resolve(dir, 'classes.wasm'), {
  stackDeobfuscator: { enabled: false },
  installImports(o) {
    o.teavmConsole = { putcharStdout() {}, putcharStderr() {} };
  },
});
console.log(`module loaded in ${(performance.now() - t0).toFixed(0)} ms`);

const counts = new Map();
const bump = (k) => counts.set(k, (counts.get(k) ?? 0) + 1);
process.on('SIGTERM', () => {
  console.log('ROUTES', JSON.stringify(Object.fromEntries(counts)));
  process.exit(0);
});

const PROXIED = ['/api/pure/v1/protocol/pure/getClassifierPathMap', '/api/pure/v1/protocol/pure/getSubtypeInfo'];

http
  .createServer(async (req, res) => {
    // CORS as an engine server answers it, so a page (or jsdom) on another origin can call
    res.setHeader('access-control-allow-origin', req.headers.origin ?? '*');
    res.setHeader('access-control-allow-credentials', 'true');
    res.setHeader('access-control-allow-methods', 'GET,POST,PUT,DELETE,OPTIONS');
    res.setHeader('access-control-allow-headers', req.headers['access-control-request-headers'] ?? '*');
    if (req.method === 'OPTIONS') {
      res.writeHead(204);
      res.end();
      return;
    }
    const url = new URL(req.url, 'http://x');
    const chunks = [];
    for await (const c of req) chunks.push(c);
    let buf = Buffer.concat(chunks);
    const enc = req.headers['content-encoding'];
    if (enc === 'gzip') buf = gunzipSync(buf);
    else if (enc === 'deflate') buf = inflateSync(buf);
    const body = buf.toString('utf8');
    if (PROXIED.includes(url.pathname)) {
      bump(`PROXIED ${url.pathname}`);
      const r = await fetch(engineBase + url.pathname + url.search);
      res.writeHead(r.status, { 'content-type': r.headers.get('content-type') ?? 'application/json' });
      res.end(await r.text());
      return;
    }
    const raw = m.exports.pureV1OrError(url.pathname, url.search.slice(1), body);
    if (!raw.startsWith('OK\n')) {
      bump(`MODULE-ERROR ${url.pathname}`);
      res.writeHead(500, { 'content-type': 'text/plain' });
      res.end(raw);
      return;
    }
    const rest = raw.slice(3);
    const i = rest.indexOf('\n');
    const j = rest.indexOf('\n', i + 1);
    const status = Number(rest.slice(0, i));
    bump(`${status} ${req.method} ${url.pathname}`);
    res.writeHead(status, { 'content-type': rest.slice(i + 1, j) });
    res.end(rest.slice(j + 1));
  })
  .listen(Number(portArg), '127.0.0.1', () => console.log(`shim listening on ${portArg}`));
