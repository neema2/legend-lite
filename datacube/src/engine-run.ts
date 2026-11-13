// A GRID'S RUNNER OVER DATACUBE'S ENGINE CONTRACT (docs/DATACUBE_ENGINE_CONTRACT_2026_11_12.md, step 4a): the
// QueryRunner every grid reads its rows through, made of an EngineSession and the source's model as the engine handed it
// (R3a: no connection, a runtime the engine owns) -- whichever engine it is: the tab's, Python's, a server's. DataCube
// sends Pure and gets typed answers; the query panes show the answer's explain text, read-only (R4). A query calling
// DataCube's Pure library sends it beside the model (`library`, Q17), never in it.

import type { EngineSession, QueryAsk } from '../../engine-client/src/engine-contract.ts';
import type { PrintStyle } from '../../engine-client/src/pure-v1.ts';
import type { PlanColumn } from '../../engine-client/src/relation-type.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';
import type { Lambda } from '../../pure-protocol/src/index.ts';
import { callsLibrary, loadLibrary } from './pure-library.ts';
import { lazily } from './lazily.ts';
import { QueryFailure, type QueryRunner, type RunOutcome } from './runner.ts';
import type { CubeSnapshot } from './snapshot.ts';
import type { LevelScope } from './query.ts';

/** What a grid's queries are asked over: the model the engine handed DataCube, its runtime, and how it is read. */
export interface EngineSource {
  readonly model: string;
  readonly runtime: string;
  readonly how?: QueryAsk['how'];
}

// the Arrow answer's reader (apache-arrow), fetched with a grid's first rows: a page's start never downloads it
const arrow = () => lazily(() => import('../../engine-client/src/engine-arrow.ts'));

export class EngineRun implements QueryRunner {
  readonly name: string;
  readonly #session: EngineSession | Promise<EngineSession>;
  readonly #source: EngineSource | Promise<EngineSource>;

  /** Either may be on its way: the page's engine opened at its first grid's rows, a model it serves once it has read
   *  it (R3a (c)); a failure of either is the first query's. */
  constructor(session: EngineSession | Promise<EngineSession>, source: EngineSource | Promise<EngineSource>, name: string) {
    this.#session = session;
    this.#source = source;
    this.name = name;
  }

  /** The query as the engine is asked it: its model, runtime and how; DataCube's library beside it where it calls it. */
  async #ask(query: Lambda): Promise<QueryAsk> {
    const source = await this.#source;
    return {
      model: source.model, runtime: source.runtime, query,
      ...(source.how !== undefined ? { how: source.how } : {}),
      ...(callsLibrary(query) ? { library: await loadLibrary() } : {}),
    };
  }

  /** A failure as the cube says it (QueryFailure, its debug info the query): the engine's typed error its cause. */
  #failure(e: unknown, query: Lambda, signal?: AbortSignal): unknown {
    return signal?.aborted ? e : new QueryFailure(e, query);
  }

  async run(query: Lambda, snapshot: CubeSnapshot, _scope?: LevelScope, signal?: AbortSignal): Promise<RunOutcome> {
    try {
      const session = await this.#session;
      const answer = await session.query(await this.#ask(query), signal ? { signal } : {});
      return { rows: await (await arrow()).resultOf(answer, snapshot.epoch), sql: answer.explain ?? '' };
    } catch (e) {
      throw this.#failure(e, query, signal);
    }
  }

  async stream(query: Lambda, snapshot: CubeSnapshot, onChunk: (chunk: ResultTable) => void, signal?: AbortSignal): Promise<void> {
    try {
      const session = await this.#session;
      const answer = await session.query(await this.#ask(query), signal ? { signal } : {});
      await (await arrow()).resultOf(answer, snapshot.epoch, onChunk);
    } catch (e) {
      throw this.#failure(e, query, signal);
    }
  }

  /** Compile-only: the engine types it (typeOf), nothing runs. */
  async compile(query: Lambda, _snapshot: CubeSnapshot, signal?: AbortSignal): Promise<void> {
    await this.relationType(query, signal);
  }

  async relationType(query: Lambda, signal?: AbortSignal): Promise<PlanColumn[]> {
    try {
      const typed = await (await this.#session).typeOf(await this.#ask(query), signal ? { signal } : {});
      return typed.columns.map((c) => ({ name: c.name, type: c.type, ...(c.enumeration !== undefined ? { enumeration: c.enumeration } : {}) }));
    } catch (e) {
      throw this.#failure(e, query, signal);
    }
  }

  async parse(text: string, signal?: AbortSignal): Promise<Lambda> {
    return (await this.#session).parse(text, signal ? { signal } : {});
  }

  async print(query: Lambda, style?: PrintStyle, signal?: AbortSignal): Promise<string> {
    return (await this.#session).print(query, style === 'STANDARD' ? 'COMPACT' : 'PRETTY', signal ? { signal } : {});
  }
}
