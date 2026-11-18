// LITE'S ONE ERROR SHAPE (lite's API v1 spec §3, the user's go of 2026-11-17): every refusal, on every binding, is
// `{kind, message, where?, raise?}` -- the server's routes answer it (core's LiteV1Api.Refusal, its status by its kind),
// the tab's planner folds it (`ERR\n` and its JSON, planner.Folded), and Python raises it. A record: it plans nothing
// and sends nothing (//engine-client:records), a product file outside legend-engine's shapes (src/legend/ is the edge's,
// H11). No legend-engine spelling -- no PARSER, COMPILATION, code -1 or errorType -- appears here.

/** The kinds (spec §3), one closed list: a kind no binding writes is refused by name. */
// notFound (the user, 2026-11-17 ~23:31): a saved query or saved page the request names does not exist, 404
export const LITE_ERROR_KINDS = ['parse', 'compile', 'unsupported', 'data', 'parameters', 'request', 'notFound', 'internal'] as const;

export type LiteErrorKind = (typeof LITE_ERROR_KINDS)[number];

/** A refusal as every binding writes it. `where`: a position or a site, where there is one. `raise`: a data refusal's
 *  raise kind, in lower camel (`divideByZero`). */
export interface LiteError {
  readonly kind: LiteErrorKind;
  readonly message: string;
  readonly where?: string;
  readonly raise?: string;
}

/** A refusal thrown: its kind, words, where and raise, and the HTTP status a server answered it with (none in the
 *  tab). */
export class LiteRefusal extends Error implements LiteError {
  readonly kind: LiteErrorKind;
  readonly where?: string;
  readonly raise?: string;
  readonly status?: number;

  constructor(error: LiteError, status?: number) {
    super(error.message);
    this.name = 'LiteRefusal';
    this.kind = error.kind;
    if (error.where !== undefined) this.where = error.where;
    if (error.raise !== undefined) this.raise = error.raise;
    if (status !== undefined) this.status = status;
  }
}

/** `json` read as lite's refusal: an object of exactly its keys, its kind one of the list, its strings strings; anything
 *  else refused by name -- a body in another shape is no refusal of lite's (never read as one by a guess). */
export function liteError(json: unknown): LiteError {
  if (typeof json !== 'object' || json === null || Array.isArray(json)) {
    throw new Error(`not lite's refusal (an object {kind, message, where?, raise?}): ${preview(json)}`);
  }
  const o = json as Record<string, unknown>;
  for (const key of Object.keys(o)) {
    if (key !== 'kind' && key !== 'message' && key !== 'where' && key !== 'raise') {
      throw new Error(`not lite's refusal: it has a key ${key} (it holds kind, message, where?, raise?): ${preview(json)}`);
    }
  }
  if (typeof o['kind'] !== 'string' || !(LITE_ERROR_KINDS as readonly string[]).includes(o['kind'])) {
    throw new Error(`not lite's refusal: its kind is none of ${LITE_ERROR_KINDS.join(', ')}: ${preview(json)}`);
  }
  if (typeof o['message'] !== 'string') throw new Error(`not lite's refusal: its message is no string: ${preview(json)}`);
  for (const key of ['where', 'raise'] as const) {
    if (o[key] !== undefined && typeof o[key] !== 'string') {
      throw new Error(`not lite's refusal: its ${key} is no string: ${preview(json)}`);
    }
  }
  return {
    kind: o['kind'] as LiteErrorKind,
    message: o['message'],
    ...(typeof o['where'] === 'string' ? { where: o['where'] } : {}),
    ...(typeof o['raise'] === 'string' ? { raise: o['raise'] } : {}),
  };
}

function preview(json: unknown): string {
  const text = JSON.stringify(json) ?? String(json);
  return text.length > 200 ? `${text.slice(0, 200)}...` : text;
}
