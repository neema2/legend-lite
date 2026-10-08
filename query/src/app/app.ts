// The app: the header, and the screen the address names -- landing, a data space, or the editor
// on a new, curated, service or saved query. Leaving a query with unsaved changes asks first.

import { queryOn } from '../builder/milestoning.ts';
import { type ClassSource } from '../builder/state.ts';
import { renderDataSpace } from '../ui/dataspace.ts';
import { h, icon, menuButton, mount, confirmDialog } from '../ui/dom.ts';
import { renderEditor, type EditorHandle } from '../ui/editor.ts';
import { renderLanding } from '../ui/landing.ts';
import { renderStart } from '../ui/start.ts';
import { openQueryDialog } from '../ui/queries.ts';
import { recent, type AppContext } from './context.ts';
import { openQuery, sessionFrom } from './persist.ts';
import { formatRoute, parseRoute, type Route } from './routes.ts';
import { Session } from './session.ts';
import { readQueryFragment } from '../../../query-store/src/share.ts';
import { followTheme, theme, toggleTheme } from '../ui/theme.ts';

export class App {
  readonly #ctx: AppContext;
  readonly #header = h('div', { class: 'q-appbar' });
  readonly #main = h('div', { class: 'q-main' });
  #editor: EditorHandle | undefined;
  #session: Session | undefined;
  #hash = '';
  /** Navigations begun: one whose render a later navigation overtook (a version still loading) draws nothing. */
  #navigations = 0;

  constructor(ctx: AppContext, root: HTMLElement) {
    this.#ctx = ctx;
    mount(root, h('div', { class: 'q-app' }, this.#header, this.#main));
    window.addEventListener('hashchange', () => void this.#navigate());
    window.addEventListener('beforeunload', (e) => {
      if (this.#session?.changed) { e.preventDefault(); e.returnValue = ''; }
    });
  }

  start(): void {
    followTheme();
    void this.#navigate();
  }

  async #navigate(): Promise<void> {
    const hash = location.hash || '#/';
    if (hash === this.#hash) return;
    if (this.#session?.changed && !this.#savedJustNow(hash)) {
      if (!await confirmDialog('Unsaved changes', 'Leave this query? Its unsaved changes will be lost.', 'Leave')) {
        history.replaceState(null, '', this.#hash);
        // a screen that started the move (the version picker) puts itself back
        dispatchEvent(new Event('q-navigation-cancelled'));
        return;
      }
      // the address moved again while this one asked (a second ask stacked on it): that move is the one to follow
      if ((location.hash || '#/') !== hash) return;
    }
    this.#hash = hash;
    const at = (this.#navigations += 1);
    this.#editor?.dispose();
    this.#editor = undefined;
    this.#session = undefined;
    // the old screen goes now, not when the new one is ready: while a version loads (by name, its test data) it stayed
    // on screen and took clicks its disposed editor dropped. A route with nothing to load replaces this before a paint.
    mount(this.#main, h('div', { class: 'q-landing', role: 'status' }, h('p', null, 'Opening…')));
    const route = parseRoute(hash);
    this.#drawHeader();
    try {
      await this.#render(route, at);
    } catch (e) {
      if (at !== this.#navigations) return;
      mount(this.#main, h('div', { class: 'q-landing' }, h('h1', null, 'Could not open this'), h('div', { class: 'q-error-box' }, (e as Error).message),
        h('p', null, h('a', { href: '#/' }, 'Back to the start'))));
    }
  }

  /** Save As moved us to the saved query's own address: not a navigation away. */
  #savedJustNow(hash: string): boolean {
    const r = parseRoute(hash);
    return r.kind === 'edit' && this.#session?.saved?.id === r.id;
  }

  /** The app bar, as upstream Legend Query's: its menu and name at the left, the theme switch at the right. */
  #drawHeader(): void {
    const dark = theme() === 'dark';
    mount(this.#header,
      menuButton(icon('menu'), () => [
        { label: 'Back to query setup', action: () => { location.hash = '#/setup'; } },
        { label: 'Load a query', action: () => openQueryDialog(this.#ctx) },
      ], { class: 'q-appbar__menu', title: 'Menu', 'aria-label': 'Menu' }),
      h('div', { class: 'q-appbar__name', onclick: () => { location.hash = '#/'; } }, 'Legend Query'),
      h('span', { class: 'q-spacer' }),
      h('button', {
        class: 'q-appbar__action', title: 'Toggle light/dark mode', 'aria-label': 'Toggle light/dark mode',
        onclick: () => { toggleTheme(); this.#drawHeader(); },
      }, icon(dark ? 'moon' : 'sun')));
  }

  #edit(session: Session): void {
    this.#session = session;
    this.#editor = renderEditor(this.#main, this.#ctx, session);
  }

  async #render(r: Route, at: number): Promise<void> {
    // a route names a version; one opened by name is loaded the first time it is asked for (AppContext.load), and its
    // test data made the tab's rows (activate) -- unless a newer navigation overtook this one meanwhile: the newer one's
    // screen and rows stay
    if ('gav' in r) {
      const p = await this.#ctx.load(r.gav);
      if (at !== this.#navigations) return;
      await this.#ctx.activate(p);
    }
    if (at !== this.#navigations) return;
    const app = this.#ctx;
    switch (r.kind) {
      // as upstream: / opens the query builder; the setup page (every way to start) is /setup
      case 'home': renderStart(this.#main, app); return;
      case 'setup': renderLanding(this.#main, app); return;
      case 'notFound': mount(this.#main, h('div', { class: 'q-landing' }, h('h1', null, 'Nothing here'), h('a', { href: '#/' }, 'Back to the start'))); return;
      case 'dataSpaceViewer': renderDataSpace(this.#main, app, app.project(r.gav), r.path); return;
      case 'dataSpace': {
        const p = app.project(r.gav);
        const ds = p.graph.dataSpaces.get(r.path);
        if (!ds) throw new Error(`no data space ${r.path} in ${r.gav}`);
        const ctxName = r.context ?? ds.defaultExecutionContext;
        const ec = ds.executionContexts.find((c) => c.name === ctxName);
        if (!ec?.mapping || !ec.defaultRuntime) throw new Error(`the data space has no execution context '${ctxName}' with a mapping and runtime`);
        recent.dataSpace(p.gav, r.path, ctxName);
        const cls = r.class ?? p.graph.mappedClasses(ec.mapping.path)[0];
        if (!cls) throw new Error(`the mapping ${ec.mapping.path} maps no class`);
        const source: ClassSource = {
          kind: 'class', class: cls, mapping: ec.mapping.path, runtime: r.runtime ?? ec.defaultRuntime.path,
          dataSpace: { path: r.path, context: ctxName },
        };
        this.#edit(new Session(p, queryOn(p.graph, source)));
        return;
      }
      case 'dataSpaceTemplate': {
        const p = app.project(r.gav);
        const ds = p.graph.dataSpaces.get(r.path);
        const execs = ds?.executables ?? [];
        const e = execs.find((x) => x.id === r.template) ?? execs[Number(r.template)];
        if (!ds || !e || e._type !== 'dataSpaceTemplateExecutable') throw new Error(`no curated query '${r.template}' in ${r.path}`);
        const ctxName = e.executionContextKey ?? ds.defaultExecutionContext;
        const ec = ds.executionContexts.find((c) => c.name === ctxName)!;
        this.#edit(sessionFrom(p, e.query, { mapping: ec.mapping!.path, runtime: ec.defaultRuntime!.path, dataSpace: { path: r.path, context: ctxName } }));
        return;
      }
      case 'manual': {
        const p = app.project(r.gav);
        const cls = r.class ?? p.graph.mappedClasses(r.mapping)[0];
        if (!cls) throw new Error(`the mapping ${r.mapping} maps no class`);
        this.#edit(new Session(p, queryOn(p.graph, { kind: 'class', class: cls, mapping: r.mapping, runtime: r.runtime })));
        return;
      }
      case 'service': {
        const p = app.project(r.gav);
        const svc = p.graph.services.get(r.service);
        const ex = svc?.execution;
        if (!svc || !ex?.func || !ex.mapping || !ex.runtime?.runtime) throw new Error(`the service ${r.service} has no single execution with a mapping and runtime`);
        this.#edit(sessionFrom(p, ex.func, { mapping: ex.mapping, runtime: ex.runtime.runtime }));
        return;
      }
      case 'shared': {
        // the query as the link carries it, unsaved: Save keeps a copy in this person's store
        const shared = await readQueryFragment(r.link);
        if (at !== this.#navigations) return;
        const session = await openQuery(app, { ...shared, id: '' }, new Map(), false);
        if (at !== this.#navigations) return;
        await app.activate(session.project);
        if (at !== this.#navigations) return;
        this.#edit(session);
        return;
      }
      case 'edit': {
        const q = await app.store.get(r.id);
        const session = await openQuery(app, q, r.parameters);
        if (at !== this.#navigations) return;
        await app.activate(session.project);
        if (at !== this.#navigations) return;
        recent.query(q.id);
        this.#edit(session);
        return;
      }
    }
  }
}

export { formatRoute };
