// The landing page: find something to query. Data spaces first (the curated way in), then saved
// queries, then classes and services for those who know the model -- all behind one search box.
// Recently viewed data spaces and queries come first (upstream's user-data keys).

import { formatRoute } from '../app/routes.ts';
import { recent, type AppContext, type LoadedProject } from '../app/context.ts';
import type { Query } from '../backend/wire.ts';
import { docOf, packageOf, simpleName } from '../model/graph.ts';
import { h, mount, type Child } from './dom.ts';
import { timeAgo } from './format.ts';

function matches(text: string, needle: string): boolean {
  return needle.length === 0 || text.toLowerCase().includes(needle);
}

export function renderLanding(root: HTMLElement, app: AppContext): void {
  const results = h('div');
  let term = '';
  let saved: Query[] | Error | undefined;

  const search = h('input', {
    class: 'q-input q-search', type: 'search', placeholder: 'Search data spaces, queries, classes, services…',
    'aria-label': 'Search',
    oninput: () => { term = search.value.trim().toLowerCase(); draw(); },
  });

  const draw = (): void => {
    mount(results,
      recentSection(app, term),
      dataSpaceSection(app, term),
      savedSection(app, term, saved),
      classSection(app, term),
      serviceSection(app, term),
      depotSection(app, term, draw));
  };

  mount(root, h('div', { class: 'q-landing' },
    h('h1', null, 'What do you want to do today'),
    h('div', { class: 'q-muted' }, `${app.projects.length} project${app.projects.length === 1 ? '' : 's'} · signed in as ${app.user}`),
    search,
    results));
  draw();
  search.focus();

  app.store.search({ limit: 50, sortByOption: 'SORT_BY_VIEW' })
    .then((qs) => { saved = qs; })
    .catch((e: unknown) => { saved = e instanceof Error ? e : new Error(String(e)); })
    .finally(draw);
}

function recentSection(app: AppContext, term: string): Child {
  if (term) return null;
  const r = recent.get();
  const spaces = r.dataSpaces.filter((d) => app.projects.some((p) => p.gav === d.gav && p.graph.dataSpaces.has(d.path)));
  if (spaces.length === 0) return null;
  return [
    h('h2', null, 'Recently viewed'),
    h('div', { class: 'q-cards' }, spaces.slice(0, 6).map((d) => dataSpaceCard(app.project(d.gav), d.path))),
  ];
}

function dataSpaceCard(p: LoadedProject, path: string): HTMLElement {
  const ds = p.graph.dataSpaces.get(path)!;
  const verified = (ds.stereotypes ?? []).some((s) => s.value === 'certified' || s.value === 'Verified');
  return h('div', {
    class: 'q-card', tabindex: 0, role: 'link',
    onclick: () => { location.hash = formatRoute({ kind: 'dataSpaceViewer', gav: p.gav, path }); },
    onkeydown: (e: KeyboardEvent) => { if (e.key === 'Enter') location.hash = formatRoute({ kind: 'dataSpaceViewer', gav: p.gav, path }); },
  },
  h('div', { class: 'title' }, ds.title ?? ds.name, verified ? h('span', { class: 'q-chip accent' }, '✓ certified') : null),
  h('div', { class: 'path' }, path),
  h('div', { class: 'desc' }, (ds.description ?? docOf(ds) ?? '').replace(/[#*`]/g, '').trim() || 'No description'),
  h('div', null, h('span', { class: 'q-chip' }, `${ds.executionContexts.length} context${ds.executionContexts.length === 1 ? '' : 's'}`), ' ',
    (ds.executables ?? []).length > 0 ? h('span', { class: 'q-chip' }, `${ds.executables!.length} curated quer${ds.executables!.length === 1 ? 'y' : 'ies'}`) : null));
}

function dataSpaceSection(app: AppContext, term: string): Child {
  const cards: HTMLElement[] = [];
  for (const p of app.projects) {
    for (const [path, ds] of p.graph.dataSpaces) {
      if (matches(`${path} ${ds.title ?? ''} ${ds.description ?? ''}`, term)) cards.push(dataSpaceCard(p, path));
    }
  }
  return [h('h2', null, 'Data spaces'), cards.length ? h('div', { class: 'q-cards' }, cards) : h('div', { class: 'q-empty' }, 'No data spaces match.')];
}

function savedSection(app: AppContext, term: string, saved: Query[] | Error | undefined): Child {
  let body: Child;
  if (saved === undefined) body = h('div', { class: 'q-empty' }, h('span', { class: 'q-spinner' }), ' Loading saved queries…');
  else if (saved instanceof Error) body = h('div', { class: 'q-empty q-error' }, `Saved queries are unavailable: ${saved.message}`);
  else {
    const recentIds = recent.get().queries;
    const rows = saved
      .filter((q) => matches(`${q.name} ${q.id} ${q.owner ?? ''}`, term))
      .sort((a, b) => {
        const ra = recentIds.indexOf(a.id), rb = recentIds.indexOf(b.id);
        return (ra < 0 ? 1e9 : ra) - (rb < 0 ? 1e9 : rb);
      })
      .slice(0, term ? 50 : 8);
    body = rows.length === 0
      ? h('div', { class: 'q-empty' }, term ? 'No saved queries match.' : 'No saved queries yet — build one and save it.')
      : h('div', { class: 'q-list' }, rows.map((q) => h('div', {
        class: 'q-list-row', role: 'link', tabindex: 0,
        onclick: () => { location.hash = formatRoute({ kind: 'edit', id: q.id, parameters: new Map() }); },
      },
      h('span', null, '▤'),
      h('b', null, q.name),
      h('span', { class: 'q-faint' }, q.owner === app.user ? 'me' : q.owner ?? ''),
      h('span', { class: 'q-spacer' }),
      h('span', { class: 'q-faint' }, q.lastUpdatedAt ? `updated ${timeAgo(q.lastUpdatedAt)}` : ''))));
  }
  return [h('h2', null, 'Saved queries'), body];
}

function classSection(app: AppContext, term: string): Child {
  const groups = new Map<string, HTMLElement[]>();
  for (const p of app.projects) {
    for (const [path, cls] of p.graph.classes) {
      if (!matches(`${path} ${docOf(cls) ?? ''}`, term)) continue;
      const mappings = p.graph.mappingsFor(path);
      if (mappings.length === 0) continue;
      const mapping = mappings[0]!;
      const runtime = p.graph.runtimesFor(mapping)[0];
      if (runtime === undefined) continue;
      const pkg = packageOf(path);
      const list = groups.get(pkg) ?? [];
      list.push(h('div', {
        class: 'q-list-row', role: 'link', tabindex: 0,
        onclick: () => { location.hash = formatRoute({ kind: 'manual', gav: p.gav, mapping, runtime, class: path }); },
      },
      h('span', { class: 'mono q-faint' }, 'C'),
      h('b', null, simpleName(path)),
      h('span', { class: 'q-faint' }, docOf(cls) ?? ''),
      h('span', { class: 'q-spacer' }),
      h('span', { class: 'q-chip' }, simpleName(mapping))));
      groups.set(pkg, list);
    }
  }
  if (groups.size === 0) return term ? null : [h('h2', null, 'Classes'), h('div', { class: 'q-empty' }, 'No mapped classes.')];
  return [h('h2', null, 'Classes'), [...groups].sort(([a], [b]) => a.localeCompare(b)).map(([pkg, rows]) => [
    h('div', { class: 'q-faint mono', style: 'margin: 10px 0 4px' }, pkg),
    h('div', { class: 'q-list' }, rows),
  ])];
}

function serviceSection(app: AppContext, term: string): Child {
  const rows: HTMLElement[] = [];
  for (const p of app.projects) {
    for (const [path, svc] of p.graph.services) {
      if (!matches(`${path} ${svc.pattern} ${svc.documentation ?? ''}`, term)) continue;
      rows.push(h('div', {
        class: 'q-list-row', role: 'link', tabindex: 0,
        onclick: () => { location.hash = formatRoute({ kind: 'service', gav: p.gav, service: path }); },
      },
      h('span', { class: 'mono q-faint' }, 'S'),
      h('b', null, simpleName(path)),
      h('code', { class: 'q-faint' }, svc.pattern),
      h('span', { class: 'q-spacer' }),
      h('span', { class: 'q-faint' }, svc.documentation ?? '')));
    }
  }
  if (rows.length === 0) return null;
  return [h('h2', null, 'Services'), h('div', { class: 'q-list' }, rows)];
}

/**
 * The projects in Depot not opened yet (design Phase 3): listed by name -- none is loaded at start -- and opened at
 * HEAD, the project line's snapshot; their data spaces, classes and services then join the sections above.
 */
function depotSection(app: AppContext, term: string, redraw: () => void): Child {
  const unopened = app.depotProjects.filter((p) => !app.projects.some((l) => l.config.groupId === p.groupId && l.config.artifactId === p.artifactId)
    && matches(`${p.groupId}:${p.artifactId}`, term));
  if (unopened.length === 0) return null;
  return [
    h('h2', null, 'Projects in Depot'),
    h('div', { class: 'q-cards' }, unopened.map((p) => {
      const name = `${p.groupId}:${p.artifactId}`;
      const open = h('button', { class: 'q-btn', 'data-project': name, onclick: async () => {
        open.disabled = true;
        open.textContent = 'Opening…';
        try {
          await app.ensure(`${name}:master-SNAPSHOT`);
        } catch (e) {
          open.disabled = false;
          open.textContent = 'Open at HEAD';
          open.title = e instanceof Error ? e.message : String(e);
          return;
        }
        redraw();
      } }, 'Open at HEAD');
      return h('div', { class: 'q-card' },
        h('div', { class: 'title' }, p.artifactId),
        h('div', { class: 'path' }, name),
        h('div', { class: 'desc' }, 'In Depot: opens at HEAD, the latest on its project line'),
        open);
    })),
  ];
}
