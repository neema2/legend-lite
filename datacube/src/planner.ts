// The planner client: Pure grammar out, SQL back.
//
// legend-lite is the single planner. The browser plane executes SQL
// locally against a snap, but it does not PRODUCE that SQL -- writing a
// second planner in TypeScript would create a second thing that has to
// agree with the first about null ordering, type coercion and aggregate
// semantics, which is precisely the class of divergence that is
// expensive to find and embarrassing to ship.
//
// Verified against the real endpoint: the Pure pipeline
//   demo::Person.all()->project(~[name:x|$x.firstName])->sort(...)
// returns
//   SELECT t0.FIRST_NAME AS name FROM PERSON AS t0
//   ORDER BY t0.FIRST_NAME NULLS LAST
// with no database touched, because Compiler.plan needs no Connection.

import type { Planner } from './cube.ts';
import type { CubeSnapshot } from './snapshot.ts';

export interface LegendLitePlannerOptions {
  /** Base URL of the legend-lite server, e.g. 'http://localhost:8080'. */
  readonly baseUrl: string;
  /** Pure model source: classes, mapping, connection, runtime. */
  readonly model: string;
  /** Runtime to plan against, e.g. 'demo::RT'. */
  readonly runtime: string;
  /** Defaults to the global fetch; injectable for tests. */
  readonly fetch?: typeof fetch;
  /**
   * Cache plans by grammar text. Safe because planning is pure: the
   * same grammar and runtime always lower to the same SQL. Worth it
   * because scrolling re-issues structurally identical queries.
   */
  readonly cache?: boolean;
}

interface PlanResponse {
  readonly success?: boolean;
  readonly sql?: string;
  readonly shape?: string;
  readonly error?: string;
}

export class PlanError extends Error {
  readonly grammar: string;
  constructor(message: string, grammar: string) {
    super(message);
    this.name = 'PlanError';
    this.grammar = grammar;
  }
}

export class LegendLitePlanner implements Planner {
  readonly #options: LegendLitePlannerOptions;
  readonly #fetch: typeof fetch;
  readonly #cache = new Map<string, string>();

  constructor(options: LegendLitePlannerOptions) {
    this.#options = options;
    this.#fetch = options.fetch ?? globalThis.fetch.bind(globalThis);
  }

  async plan(pureGrammar: string, _snapshot: CubeSnapshot): Promise<string> {
    const useCache = this.#options.cache !== false;
    if (useCache) {
      const hit = this.#cache.get(pureGrammar);
      if (hit !== undefined) return hit;
    }

    const url = `${this.#options.baseUrl.replace(/\/$/, '')}/engine/plan`;
    let response: Response;
    try {
      response = await this.#fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          code: `${this.#options.model}\n${pureGrammar}`,
          runtime: this.#options.runtime,
        }),
      });
    } catch (cause) {
      throw new PlanError(
        `could not reach the planner at ${url}: ${String(cause)}`,
        pureGrammar,
      );
    }

    const body = (await response.json().catch(() => ({}))) as PlanResponse;
    if (!response.ok || body.error) {
      // The compiler's own message is the useful part; keep it verbatim
      // rather than wrapping it in something friendlier and vaguer.
      throw new PlanError(
        body.error ?? `planner returned ${response.status}`,
        pureGrammar,
      );
    }
    if (!body.sql) {
      throw new PlanError('planner returned no SQL', pureGrammar);
    }

    if (useCache) this.#cache.set(pureGrammar, body.sql);
    return body.sql;
  }

  /** Cached plan count, for tests and diagnostics. */
  get cacheSize(): number {
    return this.#cache.size;
  }
}
