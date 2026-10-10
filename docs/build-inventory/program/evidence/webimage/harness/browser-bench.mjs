// SPIKE (2026-10-10): serve page.html and run runBench(kind) in the pinned headless Chromium over the DevTools
// protocol, a fresh browser per run. Arguments: <chrome> <web image dir> <teavm dir> <data dir> <kind> [roundtrip]
import { createServer } from 'node:http';
import { spawn } from 'node:child_process';
import { readFile, mkdtemp, rm } from 'node:fs/promises';
import { existsSync, readFileSync } from 'node:fs';
import { join, extname } from 'node:path';

const [chrome, webImageDir, teavmDir, dataDir, kind, roundTrip] = process.argv.slice(2);
const here = new URL('.', import.meta.url).pathname;
const TYPES = { '.html': 'text/html', '.js': 'text/javascript', '.mjs': 'text/javascript', '.wasm': 'application/wasm' };
const roots = { 'build/': webImageDir, 'teavm/': teavmDir, 'data/': dataDir };

const server = createServer(async (req, res) => {
  const path = decodeURIComponent(new URL(req.url, 'http://x').pathname).slice(1);
  let file = path === 'page.html' ? join(here, 'page.html') : undefined;
  for (const [prefix, dir] of Object.entries(roots)) if (path.startsWith(prefix)) file = join(dir, path.slice(prefix.length));
  if (!file || !existsSync(file)) { res.writeHead(404).end(); return; }
  res.writeHead(200, { 'content-type': TYPES[extname(file)] ?? 'application/octet-stream' });
  res.end(await readFile(file));
});
await new Promise((r) => server.listen(0, '127.0.0.1', r));
const port = server.address().port;

const profile = await mkdtemp(join(here, 'chrome-profile-'));
const proc = spawn(chrome, ['--headless', '--remote-debugging-port=0', `--user-data-dir=${profile}`, '--no-first-run',
  'about:blank'], { stdio: 'ignore' });
let devtools;
for (let i = 0; i < 200 && !devtools; i++) {
  await new Promise((r) => setTimeout(r, 50));
  const f = join(profile, 'DevToolsActivePort');
  if (existsSync(f)) devtools = readFileSync(f, 'utf8').split('\n')[0];
}
const target = await (await fetch(`http://127.0.0.1:${devtools}/json/new?about:blank`, { method: 'PUT' })).json();
const ws = new WebSocket(target.webSocketDebuggerUrl);
await new Promise((r) => ws.addEventListener('open', r));
let id = 0;
const pending = new Map();
ws.addEventListener('message', (e) => {
  const msg = JSON.parse(e.data);
  if (msg.id && pending.has(msg.id)) { pending.get(msg.id)(msg); pending.delete(msg.id); }
});
const send = (method, params = {}) => new Promise((r) => { const n = ++id; pending.set(n, r); ws.send(JSON.stringify({ id: n, method, params })); });

await send('Page.enable');
await send('Page.navigate', { url: `http://127.0.0.1:${port}/page.html` });
for (let i = 0; i < 200; i++) {
  const r = await send('Runtime.evaluate', { expression: 'document.readyState + "/" + typeof runBench', returnByValue: true });
  if (r.result?.result?.value === 'complete/function') break;
  await new Promise((res) => setTimeout(res, 50));
}
const r = await send('Runtime.evaluate', {
  expression: `runBench(${JSON.stringify(kind)}, ${roundTrip === 'roundtrip'})`, awaitPromise: true, returnByValue: true,
});
console.log(JSON.stringify(r.result?.result?.value ?? r.result?.exceptionDetails ?? r));
ws.close();
proc.kill();
server.close();
await new Promise((res) => setTimeout(res, 300));
await rm(profile, { recursive: true, force: true });
