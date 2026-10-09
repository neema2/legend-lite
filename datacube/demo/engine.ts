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
//     legend-lite's one writer in Python (Frames), its title and its version;
//   - that the frame changed: `version.json?table=<name>`, asked about once a second (follow, below).
// The engine answers execute in upstream's Arrow format, declared here (`serializationFormat`), never tried and fallen
// back from.

import { CubeApp, RemoteRun, sourceColumns, type CubeSnapshot } from '../src/embed.ts';
import { LegendEngineExecutor } from '../../engine-client/src/engine-remote.ts';
import type { ValueSpecification } from '../../pure-protocol/src/index.ts';

/** What the engine says the cube is (Python's `Engine`, `/cube.json`), and its version: how often its frame changed. */
interface CubeConfig {
  readonly title: string;
  readonly model: string;
  readonly runtime: string;
  readonly source: ValueSpecification;
  readonly version: number;
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

type Link = ReturnType<typeof linked>;

/** The cube as the engine says it is now, or why it does not say (the frame closed, the engine gone). */
async function asked({ authorization, table }: Link): Promise<CubeConfig | string> {
  const answer = await fetch(`cube.json?table=${encodeURIComponent(table)}`, { headers: { Authorization: authorization } })
    .catch((e: unknown) => String(e));
  if (typeof answer === 'string') return `the engine did not answer: ${answer}`;
  if (!answer.ok) return `the engine did not say what to show: ${answer.status} ${await answer.text()}`;
  return (await answer.json()) as CubeConfig;
}

/** One cube over the engine's frame, its first query run. */
async function build(config: CubeConfig, authorization: string): Promise<CubeApp> {
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

/** How often the page asks whether its frame changed: a brief call on this machine, nothing held open (ms). */
const ASK_EVERY = 1000;

const pause = (ms: number): Promise<void> => new Promise((done) => { setTimeout(done, ms); });

/** Once the tab is shown: a hidden page has no grid to keep current, and asks again when it is looked at. */
const shown = (): Promise<void> => (document.hidden
  ? new Promise((done) => { document.addEventListener('visibilitychange', () => done(), { once: true }); })
  : Promise.resolve());

/**
 * FOLLOWING THE FRAME: about once a second the page asks the engine its frame's version (`version.json`), which
 * Python moves after a notebook cell, a `cube.update`, a `cube.refresh`, a Live frame's new columns, or a close. A
 * brief call, nothing held open: cubes in many tabs never use up the browser's six connections to one origin (the
 * audit of show(), 2026-10-08). A new version re-reads the cube: the same model re-runs the view as it stands (its
 * groups, filters and pivots kept); a new one (the frame's columns changed) opens the cube again over it. A frame
 * closed in Python, or an engine that stopped, leaves the page showing what it showed, saying it no longer follows.
 */
async function follow(link: Link, first: CubeConfig, firstCube: CubeApp): Promise<void> {
  let config = first;
  let cube = firstCube;
  const stopped = (why: string): void => { document.title = `${config.title} (not followed: ${why})`; };
  for (;;) {
    await pause(ASK_EVERY);
    await shown();
    const answer = await fetch(`version.json?table=${encodeURIComponent(link.table)}`,
      { headers: { Authorization: link.authorization } }).catch(() => undefined);
    if (answer === undefined) return stopped('the engine stopped');
    if (answer.status === 404) return stopped('the frame was closed');
    if (!answer.ok) return stopped(`the engine answered ${answer.status}`);
    const { version } = (await answer.json()) as { version: number };
    if (version === config.version) continue;
    const next = await asked(link);
    if (typeof next === 'string') return stopped(next);
    if (next.model === config.model && JSON.stringify(next.source) === JSON.stringify(config.source)) {
      config = next;
      await cube.state.refresh();
    } else {
      cube.dispose();
      document.getElementById('cube')!.replaceChildren();
      config = next;
      cube = await build(next, link.authorization);
    }
  }
}

async function open(link: Link): Promise<void> {
  const config = await asked(link);
  if (typeof config === 'string') refuse(config);
  await follow(link, config, await build(config, link.authorization));
}

// a page that cannot open says why, on the page (the cube's own failures it shows in its status bar)
open(linked()).catch((e: unknown) => {
  const host = document.getElementById('cube');
  if (host && host.childElementCount === 0) host.textContent = e instanceof Error ? e.message : String(e);
  throw e;
});
