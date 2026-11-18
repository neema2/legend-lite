// PYTHON'S SAVED QUERIES HELD TO THE ONE QUERY-STORE SUITE (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B5): legend-lite's
// saved-query rules in the native library -- the very rules its server runs on the JVM and the page runs compiled to
// WebAssembly -- over a folder in this user's legend-lite folder, answering legend-engine's /api/pure/v1/query as the
// server and the page answer it (query-store/test/conformance.ts). Python's engine is the browser test's
// (engine_fixture.py), its folder the test's own; every call carries the engine's token, as the page's do.

import { spawn } from 'node:child_process';
import { mkdtempSync } from 'node:fs';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { after, before } from 'node:test';

import { conformance } from '../../../query-store/test/conformance.ts';
import { runfileFromEnv } from '../../../tools/js/runfiles.mts';

const home = mkdtempSync(join(process.env['TEST_TMPDIR'] ?? '.', 'legend-lite-home-'));
const engine = spawn(runfileFromEnv('ENGINE_FIXTURE'), ['--site', runfileFromEnv('DATACUBE_SITE')], {
  stdio: ['pipe', 'pipe', 'inherit'],
  env: { ...process.env, LEGEND_LITE_LIBRARY: runfileFromEnv('LEGEND_LITE_LIBRARY'),
    LEGEND_LITE_ENGINE_LIBRARY: runfileFromEnv('LEGEND_LITE_ENGINE_LIBRARY'),
    LEGEND_LITE_DATACUBE: runfileFromEnv('LEGEND_LITE_DATACUBE'), LEGEND_LITE_HOME: home, PYTHONUTF8: '1',
    PYTHONDONTWRITEBYTECODE: '1' },
});

let api = '';
let authorization = '';

before(async () => {
  // the engine's first JSON line says where it is and its token (show() says its link before it)
  const lines = createInterface({ input: engine.stdout! });
  const served = await new Promise<{ url: string; authorization: string }>((done, fail) => {
    engine.once('exit', (code) => fail(new Error(`the engine exited (${String(code)})`)));
    lines.on('line', (line) => { if (line.startsWith('{')) done(JSON.parse(line) as { url: string; authorization: string }); });
  });
  api = `${served.url}/api`;
  authorization = served.authorization;
});

after(() => { engine.stdin?.end(); });

conformance('Python\'s saved queries, the native library\'s', () => ({
  api,
  fetch: (input: string | URL | Request, init?: RequestInit) =>
    fetch(input, { ...init, headers: { ...(init?.headers as Record<string, string> | undefined), Authorization: authorization } }),
  user: 'this user',
}));
