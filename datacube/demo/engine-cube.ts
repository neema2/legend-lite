// ONE CUBE ON AN ENGINE THAT RUNS ITS QUERIES (docs/DATACUBE_PYTHON_SHOW_DESIGN_2026_10_08.md): what DataCube's page of
// one cube (engine.ts, a browser tab) and a notebook's cube (widget.ts) share. The cube runs in remote-run mode, as
// Query's results do (query/src/app/cube.ts): each query goes to the engine's pure/v1 execute, the rows come back,
// nothing runs in the page. It asks the engine `cube.json?table=<name>` for the table's model, runtime and source
// (written by legend-lite's one writer in Python, Frames), its title and its version. The engine answers execute in
// upstream's Arrow format, declared here (`serializationFormat`), never tried and fallen back from.
//
// How the cube reaches its engine is the caller's (`EngineLink`): a tab calls its own origin over HTTP with the
// engine's token; a notebook's cube sends its calls over the widget's channel, which the notebook authenticates.

import { CubeApp, RemoteRun, sourceColumns, type CubeSnapshot } from '../src/embed.ts';
import { LegendEngineExecutor } from '../../engine-client/src/engine-remote.ts';
import type { ValueSpecification } from '../../pure-protocol/src/index.ts';

/** What the engine says the cube is (Python's `Engine`, `/cube.json`), and its version: how often its frame changed. */
export interface CubeConfig {
  readonly title: string;
  readonly model: string;
  readonly runtime: string;
  readonly source: ValueSpecification;
  readonly version: number;
}

/** How the cube reaches its engine, and the frame it shows. */
export interface EngineLink {
  /** What every call's path goes under: a tab's own origin, or the widget channel's base (widget-loader.ts). */
  readonly baseUrl: string;
  /** What carries the calls: the page's fetch, or the widget channel's. */
  readonly fetch: typeof fetch;
  /** The `Authorization` header the engine asks for over HTTP (`Bearer <token>`); a notebook's channel needs none. */
  readonly authorization?: string;
  /** The frame's table, by its name. */
  readonly table: string;
}

const headers = (link: EngineLink): HeadersInit => (link.authorization === undefined ? {} : { Authorization: link.authorization });

/** The cube as the engine says it is now, or why it does not say (the frame closed, the engine gone). */
export async function asked(link: EngineLink): Promise<CubeConfig | string> {
  const answer = await link.fetch(`${link.baseUrl}/cube.json?table=${encodeURIComponent(link.table)}`, { headers: headers(link) })
    .catch((e: unknown) => String(e));
  if (typeof answer === 'string') return `the engine did not answer: ${answer}`;
  if (!answer.ok) return `the engine did not say what to show: ${answer.status} ${await answer.text()}`;
  return (await answer.json()) as CubeConfig;
}

/** One cube over the engine's frame in `host`, its first query run. */
async function build(host: HTMLElement, config: CubeConfig, link: EngineLink): Promise<CubeApp> {
  const runner = new RemoteRun(new LegendEngineExecutor({
    baseUrl: link.baseUrl,
    model: config.model,
    runtime: config.runtime,
    serializationFormat: 'ARROW_IPC',
    ...(link.authorization === undefined ? {} : { authorization: link.authorization }),
    fetch: link.fetch,
  }));
  const columns = await sourceColumns(runner, config.source);
  const snapshot: CubeSnapshot = {
    source: { query: config.source }, columns, derived: [], rows: [], pivotOn: [], measures: [], sorts: [], epoch: 1,
  };
  const cube = new CubeApp(host, snapshot, {
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

/** The engine's cube in `host`, kept as the engine says it is (`reread`). */
export class EngineCube {
  readonly #host: HTMLElement;
  readonly #link: EngineLink;
  #config: CubeConfig;
  #cube: CubeApp;

  private constructor(host: HTMLElement, link: EngineLink, config: CubeConfig, cube: CubeApp) {
    this.#host = host;
    this.#link = link;
    this.#config = config;
    this.#cube = cube;
  }

  /** The cube the engine says, opened in `host`; refused with the engine's reason when it says none. */
  static async open(host: HTMLElement, link: EngineLink): Promise<EngineCube> {
    const config = await asked(link);
    if (typeof config === 'string') throw new Error(config);
    return new EngineCube(host, link, config, await build(host, config, link));
  }

  get config(): CubeConfig {
    return this.#config;
  }

  /**
   * The cube read again, its frame having changed: the same model re-runs the view as it stands (its groups, filters
   * and pivots kept); a new one (the frame's columns changed) opens the cube again over it. Undefined, or why the
   * engine no longer says what to show (the frame was closed, the engine stopped): the cube keeps what it shows.
   */
  async reread(): Promise<string | undefined> {
    const next = await asked(this.#link);
    if (typeof next === 'string') return next;
    if (next.model === this.#config.model && JSON.stringify(next.source) === JSON.stringify(this.#config.source)) {
      this.#config = next;
      await this.#cube.state.refresh();
    } else {
      this.#cube.dispose();
      this.#host.replaceChildren();
      this.#config = next;
      this.#cube = await build(this.#host, next, this.#link);
    }
    return undefined;
  }

  dispose(): void {
    this.#cube.dispose();
  }
}
