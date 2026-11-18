// Text mode (census §5.6): the query as Pure -- read it, copy it, or edit it. Applying parses it
// (legend-lite's grammar) and rebuilds the form; what the form cannot show stays text, and still
// runs and saves. A parse error keeps the dialog open, marked.

import type { AppContext } from '../app/context.ts';
import type { Session } from '../app/session.ts';
import { buildLambda } from '../builder/build.ts';
import { loadLambda, parametersOf } from '../builder/load.ts';
import { dialog, h, toast } from './dom.ts';

export async function textDialog(app: AppContext, session: Session): Promise<void> {
  const area = h('textarea', { class: 'q-textarea', rows: 18, spellcheck: false, 'aria-label': 'Pure query' });
  const error = h('div');
  const tabs = h('div', { class: 'q-tabs' });
  let showJson = false;
  let pure = '';
  try {
    const l = session.text?.lambda ?? buildLambda(session.project.graph, session.query, { withFrom: false });
    pure = await app.engine.queryPrint(l, 'pretty');
  } catch (e) {
    pure = session.text ? '' : `// ${(e as Error).message}`;
  }
  area.value = pure;
  const drawTabs = (): void => {
    tabs.replaceChildren(
      h('button', { class: `q-tab${showJson ? '' : ' on'}`, onclick: () => { showJson = false; area.readOnly = false; area.value = pure; drawTabs(); } }, 'Pure'),
      h('button', {
        class: `q-tab${showJson ? ' on' : ''}`,
        onclick: async () => {
          showJson = true; area.readOnly = true; drawTabs();
          try { area.value = JSON.stringify(await app.engine.queryParse(area.value || pure), null, 2); } catch (e) { area.value = (e as Error).message; }
        },
      }, 'Protocol JSON'));
  };
  drawTabs();
  dialog('Query as Pure', (d) => ({
    body: [
      session.text ? h('div', { class: 'q-chip', style: 'color:var(--warn); white-space:normal' }, `The form cannot show this query: ${session.text.reason}`) : null,
      tabs, area, error,
    ],
    foot: [
      h('button', { class: 'q-btn', onclick: () => void navigator.clipboard?.writeText(area.value).then(() => toast('Copied')) }, 'Copy'),
      h('span', { class: 'q-spacer' }),
      h('button', { class: 'q-btn', onclick: () => d.close() }, 'Cancel'),
      h('button', {
        class: 'q-btn primary',
        onclick: async () => {
          if (showJson) { d.close(); return; }
          if (area.value.trim() === pure.trim()) { d.close(); return; }
          try {
            const lambda = await app.engine.queryParse(area.value);
            const src = session.query.source;
            const loaded = loadLambda(session.project.graph, lambda, { mapping: src.mapping, runtime: src.runtime, ...(src.dataSpace ? { dataSpace: src.dataSpace } : {}) });
            if (loaded.ok) {
              session.setText(undefined, loaded.query);
              toast('Applied');
            } else {
              session.update((q) => ({ ...q, parameters: parametersOf(lambda) }), true);
              session.setText({ lambda, reason: loaded.reason });
              toast('Applied as text: the form cannot show it');
            }
            d.close();
          } catch (e) {
            error.replaceChildren(h('div', { class: 'q-error-box' }, `Failed to parse: ${(e as Error).message}`));
          }
        },
      }, 'Apply'),
    ],
  }), { wide: true });
}
