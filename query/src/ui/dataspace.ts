// A data space, read before querying it (upstream's data space viewer, census §3.3): what it is,
// its curated queries, how it runs (execution contexts), the documentation of every model element
// it offers, and who supports it. "Query" opens the editor on it.

import { recent, type AppContext, type LoadedProject } from '../app/context.ts';
import { formatRoute } from '../app/routes.ts';
import type { PDataSpace, PDataSpaceExecutable } from '../../../engine-client/src/legend/pmcd.ts';
import { docOf, humanize, multiplicityText, simpleName } from '../model/graph.ts';
import { h, markdown, mount, select, type Child } from './dom.ts';

export function renderDataSpace(root: HTMLElement, app: AppContext, project: LoadedProject, path: string): void {
  const ds = project.graph.dataSpaces.get(path);
  if (!ds) {
    mount(root, h('div', { class: 'q-landing' }, h('h1', null, 'Data space not found'), h('p', { class: 'q-muted' }, path)));
    return;
  }
  recent.dataSpace(project.gav, path);
  let context = ds.defaultExecutionContext;
  const open = (): void => {
    location.hash = formatRoute({ kind: 'dataSpace', gav: project.gav, path, context });
  };
  const verified = (ds.stereotypes ?? []).some((s) => s.value === 'certified' || s.value === 'Verified');
  mount(root, h('div', { class: 'q-ds' }, h('div', { class: 'q-ds-inner' },
    h('div', { class: 'q-ds-head' },
      h('div', { style: 'flex:1; min-width:0' },
        h('h1', null, ds.title ?? humanize(ds.name), ' ', verified ? h('span', { class: 'q-chip accent' }, '✓ certified') : null),
        h('div', { class: 'mono q-faint' }, path, ' · ', project.gav)),
      ds.executionContexts.length > 1
        ? select(context, ds.executionContexts.map((c) => ({ value: c.name, label: c.title ?? c.name })), (v) => { context = v; })
        : null,
      h('button', { class: 'q-btn primary', onclick: open }, 'Query this data space')),
    h('section', null, h('h3', null, 'About'), ds.description ? markdown(ds.description) : h('p', { class: 'q-muted' }, docOf(ds) ?? 'No description.')),
    quickStart(app, project, ds),
    contexts(ds),
    modelDocs(project, ds),
    support(ds))));
}

function quickStart(app: AppContext, project: LoadedProject, ds: PDataSpace): Child {
  const execs = ds.executables ?? [];
  if (execs.length === 0) return null;
  return h('section', null, h('h3', null, 'Quick start'),
    h('div', { class: 'q-cards' }, execs.map((e, i) => executableCard(app, project, ds, e, i))));
}

function executableCard(app: AppContext, project: LoadedProject, ds: PDataSpace, e: PDataSpaceExecutable, i: number): HTMLElement {
  const code = h('pre', { class: 'q-json', style: 'padding:6px 0; white-space:pre-wrap; max-height:120px; overflow:auto' }, '');
  if (e._type === 'dataSpaceTemplateExecutable') {
    app.engine.lambdaText(e.query, 'PRETTY').then((t) => { code.textContent = t; }, (err: Error) => { code.textContent = err.message; });
  } else {
    code.textContent = e.executable.path;
  }
  const openIt = (): void => {
    location.hash = e._type === 'dataSpaceTemplateExecutable'
      ? formatRoute({ kind: 'dataSpaceTemplate', gav: project.gav, path: `${ds.package}::${ds.name}`, template: e.id ?? String(i) })
      : formatRoute({ kind: 'service', gav: project.gav, service: e.executable.path });
  };
  return h('div', { class: 'q-card', onclick: openIt },
    h('div', { class: 'title' }, e.title, h('span', { class: 'q-chip' }, e._type === 'dataSpaceTemplateExecutable' ? 'query' : simpleName(e.executable.path))),
    h('div', { class: 'desc' }, e.description ?? ''),
    code,
    h('div', null, h('button', { class: 'q-btn small primary', onclick: (ev: Event) => { ev.stopPropagation(); openIt(); } }, 'Open in Query')));
}

function contexts(ds: PDataSpace): Child {
  return h('section', null, h('h3', null, 'Execution contexts'),
    h('table', { class: 'q-table' },
      h('thead', null, h('tr', null, h('th', null, 'Name'), h('th', null, 'Mapping'), h('th', null, 'Runtime'), h('th', null, 'Description'))),
      h('tbody', null, ds.executionContexts.map((c) => h('tr', null,
        h('td', null, h('b', null, c.title ?? c.name), c.name === ds.defaultExecutionContext ? h('span', { class: 'q-chip accent', style: 'margin-left:6px' }, 'default') : null),
        h('td', { class: 'mono' }, c.mapping?.path ?? ''),
        h('td', { class: 'mono' }, c.defaultRuntime?.path ?? ''),
        h('td', { class: 'q-muted' }, c.description ?? ''))))));
}

/** Every class, enumeration and association the data space's `elements` include, documented and searchable. */
function modelDocs(project: LoadedProject, ds: PDataSpace): Child {
  const rules = ds.elements ?? [];
  const inScope = (p: string): boolean => {
    if (rules.length === 0) return true;
    let best: { len: number; exclude: boolean } | undefined;
    for (const r of rules) {
      if (p === r.path || p.startsWith(`${r.path}::`)) {
        if (!best || r.path.length > best.len) best = { len: r.path.length, exclude: r.exclude === true };
      }
    }
    return best !== undefined && !best.exclude;
  };
  const graph = project.graph;
  const rows: { element: string; kind: string; name: string; type: string; doc: string }[] = [];
  for (const e of graph.documentedElements()) {
    if (!inScope(e.path)) continue;
    rows.push({ element: e.path, kind: e.kind, name: simpleName(e.path), type: '', doc: e.doc ?? '' });
    if (e.kind === 'class') {
      for (const p of graph.ownProperties(e.path)) {
        rows.push({ element: e.path, kind: p.derived ? 'derived property' : 'property', name: `${simpleName(e.path)}.${p.name}`,
          type: `${simpleName(p.type)}${multiplicityText(p.multiplicity)}`, doc: p.doc ?? '' });
      }
    } else if (e.kind === 'enumeration') {
      for (const v of graph.enumerations.get(e.path)?.values ?? []) {
        rows.push({ element: e.path, kind: 'value', name: `${simpleName(e.path)}.${v.value}`, type: '', doc: docOf(v) ?? '' });
      }
    }
  }
  const body = h('tbody');
  const draw = (term: string): void => {
    const t = term.toLowerCase();
    mount(body, rows.filter((r) => !t || `${r.name} ${r.doc} ${r.type}`.toLowerCase().includes(t)).map((r) => h('tr', null,
      h('td', null, r.kind === 'class' || r.kind === 'enumeration' || r.kind === 'association' ? h('b', null, r.name) : r.name),
      h('td', { class: 'q-faint' }, r.kind),
      h('td', { class: 'mono' }, r.type),
      h('td', { class: 'q-muted' }, r.doc))));
  };
  const input = h('input', { class: 'q-input', type: 'search', placeholder: 'Search documentation…', oninput: () => draw(input.value) });
  draw('');
  return h('section', null, h('h3', null, 'Models documentation'),
    h('div', { style: 'margin-bottom:8px' }, input),
    h('table', { class: 'q-table' },
      h('thead', null, h('tr', null, h('th', null, 'Name'), h('th', null, 'Kind'), h('th', null, 'Type'), h('th', null, 'Documentation'))),
      body));
}

function support(ds: PDataSpace): Child {
  const s = ds.supportInfo;
  if (!s) return null;
  const items: Child[] = [];
  if (s.address) items.push(h('div', null, 'Email: ', h('a', { href: `mailto:${s.address}` }, s.address)));
  for (const e of s.emails ?? []) items.push(h('div', null, 'Email: ', h('a', { href: `mailto:${e}` }, e)));
  for (const [label, url] of [['Documentation', s.documentationUrl], ['Website', s.website], ['FAQ', s.faqUrl], ['Support', s.supportUrl]] as const) {
    if (url && /^https?:\/\//.test(url)) items.push(h('div', null, `${label}: `, h('a', { href: url, target: '_blank', rel: 'noopener' }, url)));
  }
  return h('section', null, h('h3', null, 'Support'), items);
}
