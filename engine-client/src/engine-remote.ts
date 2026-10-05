// Server mode: the ENGINE runs the query.
//
// The other two planes plan and execute in two steps -- Pure in, SQL
// out, then DuckDB-WASM runs the SQL in the tab. That is what
// `Planner` is for, and it is what upstream does for a CACHED source:
// `generatePlan`, take the SQL out of the plan, run it locally.
//
// An engine-backed source is the other shape entirely. Upstream posts
// the query to `/api/pure/v1/execution/execute` and renders the rows
// the engine sends back (`_runQuery`); the SQL it shows comes back as
// an execution ACTIVITY and is never run by the browser. The data
// lives wherever the engine's store points, which is the whole point
// of the mode: a cube over a database the tab cannot reach.
//
// So this is not a planner. Pure in, ROWS out.

import type { ResultColumn, ResultTable, Scalar } from './result.ts';
import type { Lambda } from '../../pure-protocol/src/index.ts';
import { hostOf } from './receipt.ts';
import { PureV1Client, type PrintStyle, type PureV1Options } from './pure-v1.ts';
import { pureType, relationColumns, type PlanColumn } from './relation-type.ts';
import { hasTimeOfDay } from './types.ts';
import { timestampFromText } from './values.ts';

/** What a remote engine answers with. */
export interface RemoteResult {
  readonly rows: ResultTable;
  /**
   * The SQL the engine REPORTS having run, for display only.
   *
   * It arrives as an execution activity, after the fact. Running it
   * here would need the engine's own connection, which is the thing
   * this mode exists to avoid.
   */
  readonly sql: string;
}

export interface RemoteExecutor {
  execute(query: Lambda, epoch: number, signal?: AbortSignal): Promise<RemoteResult>;
  /** The compiler's type of a query's result: the engine's `lambdaRelationType`. */
  relationType(query: Lambda, signal?: AbortSignal): Promise<PlanColumn[]>;
  /** What a person typed, as its lambda: the engine's `grammarToJson`. */
  parse(text: string, signal?: AbortSignal): Promise<Lambda>;
  /** A query as Pure text for a person: the engine's `jsonToGrammar`. */
  print(query: Lambda, style?: PrintStyle, signal?: AbortSignal): Promise<string>;
}

export class RemoteExecutionError extends Error {
  /** What was asked: the query, or, for a parse, the text. */
  readonly subject: Lambda | string;
  constructor(message: string, subject: Lambda | string) {
    super(message);
    this.name = 'RemoteExecutionError';
    this.subject = subject;
  }
}

export type LegendEngineOptions = PureV1Options;

/** The protocol shapes this client touches, and only those. */
interface TdsResponse {
  readonly builder?: {
    readonly columns?: readonly {
      readonly name: string;
      readonly type?: string;
    }[];
  };
  readonly activities?: readonly { readonly sql?: string; readonly comment?: string }[];
  readonly result?: {
    readonly columns?: readonly string[];
    readonly rows?: readonly { readonly values?: readonly Scalar[] }[];
  };
}

/** A TDS as the engine sends it, as a ResultTable. */
export function toResultTable(
  body: TdsResponse,
  epoch: number,
  elapsedMs: number,
): ResultTable {
  // THE BUILDER NAMES THE COLUMNS AND THEIR TYPES (the engine's plan types them);
  // `result.columns` repeats the names alone. A TDS result always carries a builder:
  // one without it has no types to give, and a guess is refused.
  const declared = body.builder?.columns;
  if (declared === undefined) {
    throw new Error('the engine answered without a result builder: its columns have no types');
  }
  const names = declared.map((c) => c.name);
  const rows = body.result?.rows ?? [];
  const columns: ResultColumn[] = names.map((name, i) => {
    const values: Scalar[] = new Array(rows.length);
    for (let r = 0; r < rows.length; r++) {
      values[r] = (rows[r]?.values?.[i] ?? null) as Scalar;
    }
    // the builder's type is the compiler's (the engine's plan), read by the one
    // reader of both vocabularies
    const declaredType = declared[i]?.type;
    if (declaredType === undefined) throw new Error(`the engine typed no column '${name}'`);
    const type = pureType(declaredType);
    // a timestamp as stored (the engine writes nanoseconds and a zone), exact like every plane's
    if (hasTimeOfDay(type)) {
      for (let r = 0; r < values.length; r++) {
        const v = values[r];
        if (typeof v === 'string') values[r] = timestampFromText(v);
      }
    }
    return { name, type, values };
  });
  return { columns, rowCount: rows.length, epoch, elapsedMs };
}

/**
 * A query in, rows out, through a running server's `pure/v1` API: the query's protocol tree to
 * `execution/execute`.
 */
export class LegendEngineExecutor implements RemoteExecutor {
  readonly #client: PureV1Client;

  constructor(options: LegendEngineOptions) {
    this.#client = new PureV1Client(options, (m, p) => new RemoteExecutionError(m, p));
  }

  get baseUrl(): string {
    return this.#client.baseUrl;
  }

  async execute(query: Lambda, epoch: number, signal?: AbortSignal): Promise<RemoteResult> {
    const started = Date.now();
    const body = (await this.#client.execute(query, signal)) as TdsResponse;
    // The receipt is the response's own: the address it came back from and the SQL its
    // `activities` report. The pure/v1 API issues no statement id and keeps no history to
    // ask, so the receipt has none -- nothing is added to what the engine sends.
    const ran = (body.activities ?? []).map((a) => a.sql).filter((s): s is string => s !== undefined);
    const notes = (body.activities ?? []).map((a) => a.comment).filter((s): s is string => !!s);
    return {
      rows: {
        ...toResultTable(body, epoch, Date.now() - started),
        receipt: {
          plane: 'engine',
          where: `the engine at ${hostOf(this.#client.baseUrl)}`,
          ...(ran.length > 0 ? { serverSql: ran.join(';\n') } : {}),
          ...(notes.length > 0 ? { serverNote: notes.join('; ') } : {}),
        },
      },
      sql: body.activities?.at(-1)?.sql ?? '',
    };
  }

  async relationType(query: Lambda, signal?: AbortSignal): Promise<PlanColumn[]> {
    return relationColumns(await this.#client.lambdaRelationType(query, signal));
  }

  parse(text: string, signal?: AbortSignal): Promise<Lambda> {
    return this.#client.parse(text, signal);
  }

  print(query: Lambda, style: PrintStyle = 'PRETTY', signal?: AbortSignal): Promise<string> {
    return this.#client.print(query, style, signal);
  }
}
