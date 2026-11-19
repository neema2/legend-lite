// THE PLANNER'S WORKER ON LEGEND-LITE'S SERVER (lite API v1, docs/records/lite-api-v1-2026-11-17.md §2, §5; the edge
// audit's H7, 2026-11-17): a page planning on legend-lite's server asks its planner as it asks the module -- every
// operation on the server's own route (/api/lite/v1/<operation>), a model held here, by the handle this gives, and sent
// with each question about it (the server holds no model). Nothing else is sent: the module's other requests (a
// table's model, a file's load, a host's operations) are the tab's own, refused here by name. legend-engine's API is the
// edge's, never asked.

import type { PlannerWorker, Request } from '../../../engine-client/src/legend/planner-protocol.ts';
import { withLibrary } from '../pure-library.ts';
import { OPERATION_PATHS, folded, withModel } from './lite-routes.ts';
import { folded as foldedAnswer, foldedRefusal } from '../../../engine-client/src/folded.ts';

/** The worker over legend-lite's server at `baseUrl` (its root: the routes are under /api/lite/v1). */
export function liteServerWorker(baseUrl: string, fetcher: typeof fetch = globalThis.fetch.bind(globalThis)): PlannerWorker {
  const base = baseUrl.replace(/\/+$/, '');
  const held = new Map<number, string>();
  let next = 1;
  let alive = true;
  const worker: PlannerWorker = {
    onmessage: null,
    onerror: null,
    postMessage(message: Request): void {
      const reply = (response: Parameters<NonNullable<PlannerWorker['onmessage']>>[0]['data']): void => {
        if (alive) worker.onmessage?.({ data: response });
      };
      const { id, base: _base, ...body } = message;
      /** One route asked, its answer folded; the server not answering is the worker's death, as a crashed worker's is. */
      const call = (route: string, sent: string): void => {
        void fetcher(`${base}/api/lite/v1/${route}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: sent })
          .then(async (answer) => reply(folded(id, answer, await answer.text())), (e: unknown) => {
            if (!alive) return;
            alive = false;
            worker.onerror?.({ message: `legend-lite's server did not answer: ${String(e)}` });
          });
      };
      switch (body.kind) {
        case 'hold': {
          const handle = next++;
          held.set(handle, body.library === undefined ? body.model : withLibrary(body.model, body.library));
          reply({ id, ok: true, answer: foldedAnswer(String(handle)) });
          return;
        }
        case 'release':
          held.delete(body.handle);
          reply({ id, ok: true, answer: foldedAnswer('') });
          return;
        case 'queryPlanHeld': case 'queryTypeHeld': {
          const model = held.get(body.handle);
          if (model === undefined) {
            reply({ id, ok: true, answer: foldedRefusal({ kind: 'internal', message: `no model held as ${body.handle} here` }) });
            return;
          }
          call(body.kind === 'queryPlanHeld' ? 'query/plan' : 'query/type', withModel(body.request, { model }));
          return;
        }
        case 'modelCheck': case 'modelElements': case 'queryParse': case 'queryPrint': case 'queryType': case 'queryPlan':
        case 'modelCreateClass': case 'modelCreateAssociation': case 'modelCreateDataSpace': case 'tsItem': case 'tsStatement':
          call(OPERATION_PATHS[body.kind], body.request);
          return;
        default:
          // (legend-engine's pure/v1, refused here by name until the user's K2 ruling of 2026-11-18, left the planner
          // protocol for the engine module's requests, the edge's engine-module.ts: a page cannot send it)
          reply({ id, ok: false, error: `${body.kind} is not asked of legend-lite's server: the tab's own (a table's model,`
            + " a load, a host's operations), on the tab's own database" });
      }
    },
    terminate(): void {
      alive = false;
    },
  };
  return worker;
}
