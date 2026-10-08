// DATACUBE ON AN ENGINE: one cube over a Legend engine that RUNS its queries (docs/DATACUBE_PYTHON_SHOW_DESIGN_2026_10_08.md)
// -- Python's engine, which serves this page beside its API (`legend_lite.show`). The cube runs in remote-run mode, as
// Query's results do (query/src/app/cube.ts): each query goes to the engine's pure/v1 execute, the rows come back,
// nothing runs in this tab. So the page loads neither DuckDB-WASM nor the compiler's module: its script is the grid
// and the remote client (budgeted, test/bundle-budget.test.ts), and it needs no WebAssembly GC.
//
// What the page is told, and how:
//   - the engine: this page's own origin (one origin, no cross-origin call);
//   - the engine's token: the link's fragment (`#token=...`), which a browser never sends to a server, as the
//     warehouse's launch key is given; every call carries it;
//   - the cube: `cube.json?table=<name>` (asked with the token): the table's model, runtime and source, written by
//     legend-lite's one writer in Python (Frames), and its title.
// The engine answers execute in upstream's Arrow format, declared here (`serializationFormat`), never tried and fallen
// back from.

import { CubeApp, RemoteRun, sourceColumns, type CubeSnapshot } from '../src/embed.ts';
import { LegendEngineExecutor } from '../../engine-client/src/engine-remote.ts';
import type { ValueSpecification } from '../../pure-protocol/src/index.ts';

/** What the engine says the cube is (Python's `Engine`, `/cube.json`). */
interface CubeConfig {
  readonly title: string;
  readonly model: string;
  readonly runtime: string;
  readonly source: ValueSpecification;
}

function refuse(message: string): never {
  const host = document.getElementById('cube');
  if (host) host.textContent = message;
  throw new Error(message);
}

/** What the link says: the engine's token (its fragment) and the frame to show. It chooses nothing about who runs. */
function linked(): { readonly authorization: string; readonly table: string } {
  const token = new URLSearchParams(location.hash.slice(1)).get('token');
  if (!token) refuse('this page opens from the link the engine gave (it carries the engine\'s token)');
  return { authorization: `Bearer ${token}`, table: new URLSearchParams(location.search).get('table') ?? '' };
}

async function open({ authorization, table }: ReturnType<typeof linked>): Promise<CubeApp> {
  const asked = await fetch(`cube.json?table=${encodeURIComponent(table)}`, { headers: { Authorization: authorization } });
  if (!asked.ok) refuse(`the engine did not say what to show: ${asked.status} ${await asked.text()}`);
  const config = (await asked.json()) as CubeConfig;
  document.title = config.title;
  const runner = new RemoteRun(new LegendEngineExecutor({
    baseUrl: location.origin,
    model: config.model,
    runtime: config.runtime,
    serializationFormat: 'ARROW_IPC',
    authorization,
  }));
  const columns = await sourceColumns(runner, config.source);
  const snapshot: CubeSnapshot = {
    source: { query: config.source }, columns, derived: [], rows: [], pivotOn: [], measures: [], sorts: [], epoch: 1,
  };
  const cube = new CubeApp(document.getElementById('cube')!, snapshot, {
    runner,
    writeClipboard: (text) => navigator.clipboard?.writeText(text),
    // an export is text (CSV, HTML) or bytes (Excel, PDF)
    download: (name, mime, content) => {
      const a = document.createElement('a');
      const part: BlobPart = typeof content === 'string' ? content : new Uint8Array(content);
      a.href = URL.createObjectURL(new Blob([part], { type: mime }));
      a.download = name;
      a.click();
      setTimeout(() => URL.revokeObjectURL(a.href), 5000);
    },
  });
  // the first query: the cube runs (and re-runs) the rest itself
  await cube.open();
  return cube;
}

// a page that cannot open says why, on the page (the cube's own failures it shows in its status bar)
open(linked()).catch((e: unknown) => {
  const host = document.getElementById('cube');
  if (host && host.childElementCount === 0) host.textContent = e instanceof Error ? e.message : String(e);
  throw e;
});
