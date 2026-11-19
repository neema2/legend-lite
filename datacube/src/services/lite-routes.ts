// LITE API V1 OVER HTTP (docs/records/lite-api-v1-2026-11-17.md §2, §5, the user's go of 2026-11-17): what a planner's
// worker on a host -- Python's page (python-host.ts), legend-lite's server (lite-server.ts) -- needs to ask the host's
// own routes as the planner module is asked: each operation's route, its answer folded as the module folds its own,
// and a held model's request with the model added.

import type { LiteOperation, Response } from '../../../engine-client/src/legend/planner-protocol.ts';
import { folded as foldedAnswer, foldedRefusal } from '../../../engine-client/src/folded.ts';
import { liteError, type LiteError } from '../../../engine-client/src/lite-error.ts';

/** Each lite API v1 operation's route under the host's /api/lite/v1 (operation `a/b`, kind `aB`: the spec's rule). */
export const OPERATION_PATHS: Readonly<Record<LiteOperation, string>> = {
  modelCheck: 'model/check', modelElements: 'model/elements', queryParse: 'query/parse', queryPrint: 'query/print',
  queryType: 'query/type', queryPlan: 'query/plan', modelCreateClass: 'model/createClass',
  modelCreateAssociation: 'model/createAssociation', modelCreateDataSpace: 'model/createDataSpace', tsItem: 'ts/item',
  tsStatement: 'ts/statement',
};

/** A host's HTTP answer folded as the planner module folds its own (planner.Folded; the tab's one writer of it,
 *  engine-client's folded.ts, H4's P3, 2026-11-19): a 200's body, or the route's refusal -- lite API v1's one error
 *  shape {kind, message, where?, raise?} (§3), read as lite's and folded as the module folds one. A refusal that is not
 *  that shape is the host's own failure, said as one with what it answered. */
export function folded(id: number, answer: globalThis.Response, text: string): Response {
  if (answer.status === 200) return { id, ok: true, answer: foldedAnswer(text) };
  let refusal: LiteError | undefined;
  try {
    refusal = liteError(JSON.parse(text));
  } catch { /* not lite's refusal: refused below by what it answered */ }
  if (refusal !== undefined) return { id, ok: true, answer: foldedRefusal(refusal) };
  return { id, ok: false, error: `the engine answered ${answer.status} with no refusal of lite's: ${text.slice(0, 200)}` };
}

/** A held model's request (its operation's without the model: `{query, runtime}`, `{query}`) with `more` added -- spliced,
 *  never parsed, so the query's numbers keep their digits. */
export function withModel(request: string, more: Record<string, unknown>): string {
  if (!request.startsWith('{') || !request.endsWith('}') || request === '{}') {
    throw new Error(`a held model's request is a JSON object of its fields, not ${request.slice(0, 80)}`);
  }
  return `${request.slice(0, -1)},${JSON.stringify(more).slice(1)}`;
}
