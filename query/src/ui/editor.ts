// The query editor, laid out as upstream's query builder: its header (the query's name; undo/redo,
// load, new, save, advanced, help), then properties and the explorer on the left (parameters
// when wanted), the fetch structure in the centre, the filter at the right, the results below --
// every boundary draggable (split.ts).

import { SNAPSHOT } from '../../../depot-client/src/wire.ts';
import type { AppContext } from '../app/context.ts';
import { formatRoute } from '../app/routes.ts';
import type { Session } from '../app/session.ts';
import { queryOn, withSource } from '../builder/milestoning.ts';
import { type ClassSource } from '../builder/state.ts';
import { simpleName } from '../model/graph.ts';
import { confirmDialog, h, icon, menuButton, mount, panelAction, panelHeader, select, toast, type Child } from './dom.ts';
import { workspace } from './split.ts';
import { historyDialog, infoDialog } from './history.ts';
import { renderColumns } from './columns.ts';
import { Explorer, showPreview } from './explorer.ts';
import { renderFilter } from './filter.ts';
import { suggest } from '../app/probe.ts';
import { renderConstants } from './constants.ts';
import { renderParameters } from './params.ts';
import { openQueryDialog, save, saveAs } from './queries.ts';
import { toQuery } from '../app/persist.ts';
import { queryFragment } from '../../../query-store/src/share.ts';
import { Results } from './results.ts';
import { textDialog } from './text.ts';

export interface EditorHandle {
  dispose(): void;
}

/** The classes a source offers: a data space's (its mapping's classes, narrowed by `elements`), or every mapped class. */
function offeredClasses(session: Session): string[] {
  const src = session.query.source;
  const graph = session.project.graph;
  if (src.dataSpace) {
    const ds = graph.dataSpaces.get(src.dataSpace.path);
    const roots = graph.mappedClasses(src.mapping);
    const rules = ds?.elements ?? [];
    if (rules.length === 0) return roots;
    return roots.filter((cls) => {
      let best: { len: number; exclude: boolean } | undefined;
      for (const r of rules) {
        if (cls === r.path || cls.startsWith(`${r.path}::`)) {
          if (!best || r.path.length > best.len) best = { len: r.path.length, exclude: r.exclude === true };
        }
      }
      return best !== undefined && !best.exclude;
    });
  }
  return [...graph.classes.keys()].filter((c) => graph.mappingsFor(c).length > 0).sort();
}

export function renderEditor(root: HTMLElement, app: AppContext, session: Session): EditorHandle {
  const graph = session.project.graph;
  const explorer = new Explorer(session, { humanized: true }, (path) => void showPreview(app, session, path));
  const results = new Results(app, session);
  const setup = h('div', { class: 'q-setup' });
  const params = h('div');
  const constants = h('div');
  const columns = h('div', { class: 'q-panel' });
  const filter = h('div', { class: 'q-panel' });
  const header = h('div', { class: 'q-builder__header' });

  const changeSource = async (next: ClassSource): Promise<void> => {
    const q = session.query;
    if ((q.columns.length > 0 || q.filter) && next.class !== q.source.class) {
      if (!await confirmDialog('Change the class?', 'The columns and filter are built on the current class and will be cleared.', 'Change')) {
        drawSetup();
        return;
      }
      session.update(() => queryOn(session.project.graph, next, q.parameters));
    } else {
      session.update((s) => withSource(session.project.graph, s, next));
    }
  };

  /** drawSetup, as one listener the version picker can add and dispose can take away */
  const redrawSetup = (): void => drawSetup();
  const drawSetup = (): void => {
    const src = session.query.source;
    const rows: Child[] = [];
    // the project's version, for a project opened by name (Depot): HEAD -- the line's snapshot -- or a release.
    // Choosing one reopens this source at that version (by-name.ts loads it the first time).
    const p = session.project.config;
    if (app.versions && p.models.length === 0) {
      const label = h('label', null, 'Version');
      const field = h('div', { class: 'q-field' }, label, select(p.versionId, [{ value: p.versionId, label: versionLabel(p.versionId) }], () => undefined, { 'aria-label': 'Version' }));
      rows.push(field);
      void app.versions(p.groupId, p.artifactId).then((versions) => {
        field.replaceChildren(label, select(p.versionId, versions.map((v) => ({ value: v, label: versionLabel(v) })), (v) => {
          const gav = `${p.groupId}:${p.artifactId}:${v}`;
          // unsaved work makes the app ask first; cancelled (app.ts says so), the picker shows this version again
          removeEventListener('q-navigation-cancelled', redrawSetup);
          addEventListener('q-navigation-cancelled', redrawSetup, { once: true });
          location.hash = formatRoute(src.dataSpace
            ? { kind: 'dataSpace', gav, path: src.dataSpace.path, context: src.dataSpace.context, class: src.class }
            : { kind: 'manual', gav, mapping: src.mapping, runtime: src.runtime, class: src.class });
        }, { 'aria-label': 'Version' }));
      });
    }
    if (src.dataSpace) {
      const ds = graph.dataSpaces.get(src.dataSpace.path);
      rows.push(h('div', { class: 'q-field' }, h('label', null, 'Data Space'),
        h('a', { href: formatRoute({ kind: 'dataSpaceViewer', gav: session.project.gav, path: src.dataSpace.path }), title: src.dataSpace.path }, ds?.title ?? simpleName(src.dataSpace.path))));
      if (ds) {
        rows.push(h('div', { class: 'q-field' }, h('label', null, 'Context'),
          select(src.dataSpace.context, ds.executionContexts.map((c) => ({ value: c.name, label: c.title ?? c.name })), (v) => {
            const ec = ds.executionContexts.find((c) => c.name === v)!;
            void changeSource({ ...src, mapping: ec.mapping!.path, runtime: ec.defaultRuntime!.path, dataSpace: { path: src.dataSpace!.path, context: v } });
          })));
      }
    }
    const classes = offeredClasses(session);
    rows.push(h('div', { class: 'q-field' }, h('label', null, 'Entity'),
      select(src.class, (classes.includes(src.class) ? classes : [src.class, ...classes]).map((c) => ({ value: c, label: simpleName(c) })), (cls) => {
        if (src.dataSpace) { void changeSource({ ...src, class: cls }); return; }
        const mappings = graph.mappingsFor(cls);
        const mapping = mappings.includes(src.mapping) ? src.mapping : mappings[0]!;
        const runtimes = graph.runtimesFor(mapping);
        void changeSource({ kind: 'class', class: cls, mapping, runtime: runtimes.includes(src.runtime) ? src.runtime : runtimes[0]! });
      }, { 'aria-label': 'Entity' })));
    if (!src.dataSpace) {
      rows.push(h('div', { class: 'q-field' }, h('label', null, 'Mapping'),
        select(src.mapping, graph.mappingsFor(src.class).map((m) => ({ value: m, label: simpleName(m) })), (mapping) => {
          void changeSource({ ...src, mapping, runtime: graph.runtimesFor(mapping)[0] ?? src.runtime });
        })));
    }
    rows.push(h('div', { class: 'q-field' }, h('label', null, 'Runtime'),
      select(src.runtime, graph.runtimesFor(src.mapping).map((r) => ({ value: r, label: simpleName(r) })), (runtime) => void changeSource({ ...src, runtime }))));
    mount(setup, rows);
  };

  // the explorer's header, as upstream's: its title, the search, collapse, and a menu of view toggles
  const search = h('input', { class: 'q-input q-explorer__search', type: 'search', placeholder: 'Search properties…', 'aria-label': 'Search properties', oninput: () => explorer.setSearch(search.value) });
  const explorerHead = panelHeader('explorer', [search], [
    panelAction('compress', 'Collapse all', () => explorer.collapseAll()),
    menuButton(icon('more'), () => [
      { label: `${explorer.options.humanized ? '✓ ' : ''}Humanize Property Name`, action: () => { explorer.options.humanized = !explorer.options.humanized; explorer.render(); } },
    ], { class: 'q-panel__action', title: 'Explorer options', 'aria-label': 'Explorer options' })]);

  // the centre and the right: the fetch structure and the filter -- or, for a query the form
  // cannot show, the text-mode notice
  const center = h('div', { class: 'q-slot' });
  const right = h('div', { class: 'q-slot' });
  const textOnly = h('div', { class: 'q-panel' });
  const drawWork = (): void => {
    if (session.text) {
      mount(textOnly, panelHeader('fetch structure'), h('div', { class: 'q-panel__content', style: 'padding:14px; display:flex; flex-direction:column; gap:10px' },
        h('div', { class: 'q-chip', style: 'color:var(--warn); white-space:normal; align-self:flex-start' }, `The form cannot show this query (${session.text.reason}). It still runs and saves.`),
        h('div', null, h('button', { class: 'q-btn', onclick: () => void textDialog(app, session) }, 'Edit in text mode'))));
      center.replaceChildren(textOnly);
      right.replaceChildren();
    } else {
      center.replaceChildren(columns);
      right.replaceChildren(filter);
      renderColumns(columns, app, session, () => explorer.options.humanized);
      renderFilter(filter, session, (path, prefix) => suggest(app, session, path, prefix), app);
    }
  };


  // the builder's header, as upstream's (QueryBuilder.tsx; Core_LegendQueryApplicationPlugin
  // header actions): the query's name at the left; undo/redo, load, new, save, advanced, help
  const newQuery = (): void => {
    const src = session.query.source;
    location.hash = formatRoute(src.dataSpace
      ? { kind: 'dataSpace', gav: session.project.gav, path: src.dataSpace.path, context: src.dataSpace.context, class: src.class }
      : { kind: 'manual', gav: session.project.gav, mapping: src.mapping, runtime: src.runtime, class: src.class });
  };
  const stacked = (name: 'undo' | 'redo', label: string, title: string, enabled: boolean, onclick: () => void): HTMLElement =>
    h('button', { class: 'q-undo', title, disabled: !enabled, onclick }, icon(name), h('span', null, label));
  // A SHARE LINK: the query itself in the URL (query-store/src/share.ts) -- for someone without this
  // store; it opens unsaved, and their Save keeps their own copy
  const copyShareLink = async (): Promise<void> => {
    try {
      const q = await toQuery(app, session, { id: session.saved?.id ?? '', name: session.saved?.name ?? session.sharedAs ?? 'Shared query' });
      const link = `${location.origin}${location.pathname}${location.search}${formatRoute({ kind: 'shared', link: await queryFragment(q) })}`;
      await navigator.clipboard.writeText(link);
      toast(`Share link copied (${link.length.toLocaleString()} characters): it holds the query, never its rows`);
    } catch (e) {
      toast(`Could not make the share link: ${(e as Error).message}`, 6000);
    }
  };
  const drawHeader = (): void => {
    const saved = session.saved;
    mount(header,
      h('div', { class: 'q-builder__status' },
        h('span', { class: 'q-builder__title', title: saved?.id ?? '' }, saved?.name ?? session.sharedAs ?? 'Unsaved Query'),
        !saved && session.sharedAs ? h('span', { class: 'q-chip q-chip--status', title: 'Opened from a share link: Save keeps your own copy' }, 'shared link') : null,
        session.changed ? h('span', { class: 'q-chip q-chip--status', title: 'Unsaved changes' }, 'unsaved') : null,
        saved && saved.owner && saved.owner !== app.user ? h('span', { class: 'q-chip q-chip--status' }, `owned by ${saved.owner}`) : null),
      h('span', { class: 'q-spacer' }),
      stacked('undo', 'Undo', 'Undo (Ctrl+Z)', session.canUndo, () => session.undo()),
      stacked('redo', 'Redo', 'Redo (Ctrl+Shift+Z)', session.canRedo, () => session.redo()),
      h('button', { class: 'q-header-action', title: 'Load a saved query', onclick: () => openQueryDialog(app) }, icon('load'), h('span', null, 'Load Query')),
      h('button', { class: 'q-header-action', title: 'A new query on this source', onclick: newQuery }, icon('save'), h('span', null, 'New Query')),
      h('span', { class: 'q-header-combo' },
        h('button', { class: 'q-header-action', title: 'Save (Ctrl+S)', onclick: () => void save(app, session) }, icon('save'), h('span', null, 'Save')),
        menuButton(icon('caretDown'), () => [{ label: 'Save As New Query', action: () => saveAs(app, session) }],
          { class: 'q-header-action q-header-combo__caret', title: 'More ways to save', 'aria-label': 'More ways to save' })),
      menuButton(['Advanced', icon('caretDown')], () => [
        { label: 'Edit Pure', action: () => void textDialog(app, session) },
        { label: showParams ? 'Hide Parameters' : 'Show Parameters', action: () => { showParams = !showParams; drawSide(); } },
        { label: showConstants ? 'Hide Constants' : 'Show Constants', action: () => { showConstants = !showConstants; drawSide(); } },
        'separator',
        { label: 'About this query', action: () => infoDialog(app, session) },
        { label: 'History and versions', action: () => void historyDialog(app, session), disabled: !session.saved },
        { label: 'Copy link', action: () => void navigator.clipboard?.writeText(location.href).then(() => toast('Link copied')) },
        { label: 'Copy share link', action: () => void copyShareLink() },
      ], { class: 'q-header-pill' }),
      menuButton(['Help...', icon('caretDown')], () => [
        { label: 'Keyboard shortcuts', action: () => toast('Ctrl+Enter run · Ctrl+S save · Ctrl+Z undo · Ctrl+Shift+Z redo', 6000) },
      ], { class: 'q-header-pill' }));
  };

  // the side: properties (the setup) over the explorer, then parameters and constants, each when
  // there are some or the person asks (Advanced > Show ...), as upstream hides them by default
  let showParams = session.query.parameters.length > 0;
  let showConstants = (session.query.constants ?? []).length > 0;
  const paramsWanted = (): boolean => showParams || session.query.parameters.length > 0;
  const constantsWanted = (): boolean => showConstants || (session.query.constants ?? []).length > 0;
  let shown = { params: false, constants: false };
  const side = h('div', { class: 'q-side' });
  const drawSide = (): void => {
    shown = { params: paramsWanted(), constants: constantsWanted() };
    mount(side,
      h('div', { class: 'q-panel q-panel--fit' }, panelHeader('properties'), h('div', { class: 'q-panel__content' }, setup)),
      h('div', { class: 'q-panel q-panel--grow' }, explorerHead, h('div', { class: 'q-panel__content' }, explorer.element)),
      shown.params ? h('div', { class: 'q-panel q-panel--params' }, params) : null,
      shown.constants ? h('div', { class: 'q-panel q-panel--params' }, constants) : null);
  };
  drawSide();

  mount(root, h('div', { class: 'q-builder' },
    header,
    h('div', { class: 'q-builder__main' },
      workspace(side, center, right, h('div', { class: 'q-panel q-panel--results' }, results.element)))));
  drawSetup();
  drawWork();
  drawHeader();
  renderParameters(params, session);
  renderConstants(constants, app, session);
  results.render();
  explorer.render();

  const unsubscribe = session.subscribe((c) => {
    if (c === 'query') {
      drawWork();
      if (paramsWanted() !== shown.params || constantsWanted() !== shown.constants) drawSide();
      explorer.render();
      renderParameters(params, session);
      renderConstants(constants, app, session);
      drawSetup();
      results.render();
    }
    if (c === 'params') renderParameters(params, session);
    if (c === 'run' || c === 'query') results.render();
    drawHeader();
  });

  const onKey = (e: KeyboardEvent): void => {
    const mod = e.metaKey || e.ctrlKey;
    const inField = e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement || e.target instanceof HTMLSelectElement
      // the results cube has its own undo and redo
      || (e.target instanceof Element && e.target.closest('.dc-app') !== null);
    if (mod && e.key === 'Enter') { e.preventDefault(); results.run(); }
    else if (mod && e.key.toLowerCase() === 's') { e.preventDefault(); void save(app, session); }
    else if (mod && !inField && e.key.toLowerCase() === 'z') { e.preventDefault(); if (e.shiftKey) session.redo(); else session.undo(); }
    else if (mod && !inField && e.key.toLowerCase() === 'y') { e.preventDefault(); session.redo(); }
  };
  document.addEventListener('keydown', onKey);

  return {
    dispose() {
      removeEventListener('q-navigation-cancelled', redrawSetup);
      unsubscribe();
      document.removeEventListener('keydown', onKey);
      if (session.run.status === 'running') session.run.abort.abort();
      results.dispose();
    },
  };
}

/** A version as upstream Query names it: the line's snapshot is HEAD. */
function versionLabel(v: string): string {
  return v === SNAPSHOT ? 'HEAD' : v;
}
