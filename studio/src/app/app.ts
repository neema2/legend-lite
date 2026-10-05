// Studio's app: where projects live (connect), the compiler in a worker, Monaco, and two screens --
// workspace setup (`#/`, `#/project/<id>`) and the editor (`#/edit/<project>/<workspace>`).

import * as monaco from 'monaco-editor/editor/editor.api';
import 'monaco-editor/features/register.all';

import { startDuckDbInTab } from '../../../engine-client/src/duckdb-tab.ts';
import type { QueryEngine } from '../../../engine-client/src/engine.ts';
import { BrowserEngine } from '../../../engine-client/src/legend/browser-engine.ts';
import { HttpEngine } from '../../../engine-client/src/legend/engine.ts';
import { TabTables } from '../../../engine-client/src/tab-data.ts';
import { WasmGrammar } from '../../../engine-client/src/legend/wasm-grammar.ts';
import { Compiler, WorkerPort } from '../backend/planner.ts';
import { queryBuilder, type BuilderSession } from '../backend/query-builder.ts';
import { runner, type RunSession } from '../backend/run.ts';
import { connect, type StudioConfig } from '../backend/sdlc.ts';
import { clear, h } from '../ui/dom.ts';
import { renderEditor } from '../ui/editor.ts';
import { registerPure } from '../ui/pure-language.ts';
import { loadDemoProjects, type Manifest } from './demo-projects.ts';
import { renderSetup } from '../ui/setup.ts';
import { followTheme } from '../ui/theme.ts';

export async function start(root: HTMLElement, config: StudioConfig, workerUrl: string): Promise<void> {
  (globalThis as unknown as { MonacoEnvironment: unknown }).MonacoEnvironment = {
    getWorker: () => new Worker(new URL('./editor.worker.js', globalThis.location.href), { type: 'module' }),
  };
  followTheme();     // the kept theme before anything paints (theme.ts)
  // the code editor's font, loaded before any editor is made, so Monaco measures with it (upstream:
  // CodeEditorUtils.ts:215-228); a font that will not load leaves the fallback, not a broken page
  await Promise.all(["400 14px 'Roboto Mono'", "700 14px 'Roboto Mono'"].map((f) => document.fonts.load(f))).catch(() => undefined);
  registerPure(monaco);
  root.append(h('div', { class: 'loading' }, 'Loading Legend Studio…'));
  const { client, depot, where } = await connect(config);
  // the session's engine (plan A1): a legend server's pure/v1 when the config names one, else the one in this tab
  const inTab = config.engine ? undefined : new WasmGrammar(new WorkerPort(workerUrl, `${config.vendor}planner/`));
  const compiler = inTab
    ? new Compiler(inTab, () => inTab.warm({ _type: 'text', code: '' }))
    : new Compiler(new HttpEngine(config.engine!));
  // running a function (plan A3): in the tab, DuckDB started the first time and the model's own rows loaded before each
  // run; on a server, its pure/v1 execute
  const user = config.user?.userId ?? 'local';
  const session: RunSession & BuilderSession = inTab ? inTabSession(inTab, `${config.vendor}duckdb/`, user) : serverSession(config.engine!, user);
  const run = runner(session);
  // the query builder (plan A5): Query's, over the same session
  const builder = queryBuilder(session);
  let dispose: (() => void) | undefined;

  const route = async (): Promise<void> => {
    dispose?.();
    dispose = undefined;
    clear(root);
    const hash = decodeURIComponent(globalThis.location.hash.slice(1));
    const edit = /^\/edit\/([^/]+)\/([^/]+)$/.exec(hash);
    const open = (project: string, workspace: string): void => {
      globalThis.location.hash = `#/edit/${encodeURIComponent(project)}/${encodeURIComponent(workspace)}`;
    };
    try {
      if (edit) {
        const project = edit[1]!;
        dispose = await renderEditor(root, {
          client, depot, compiler, run, builder, monaco, project, workspace: edit[2]!,
          back: () => { globalThis.location.hash = `#/project/${encodeURIComponent(project)}`; },
        });
      } else {
        const selected = /^\/project\/([^/]+)$/.exec(hash)?.[1];
        await renderSetup(root, {
          client, where, open,
          loadDemo: async (progress) => {
            const base = new URL('./projects/', globalThis.location.href);
            const manifest = await (await fetch(new URL('manifest.json', base))).json() as Manifest;
            await loadDemoProjects(client, compiler, manifest, async (f) => (await fetch(new URL(f, base))).text(), progress);
          },
        }, selected);
      }
    } catch (e) {
      root.append(h('div', { class: 'fatal' }, h('div', { class: 'fatal-title' }, 'Studio could not open this'),
        h('div', {}, e instanceof Error ? e.message : String(e)),
        h('button', { class: 'btn', onclick: () => { globalThis.location.hash = '#/'; } }, 'Back to projects')));
    }
  };
  globalThis.addEventListener('hashchange', () => void route());
  await route();
}

/** Runs on a legend server: its pure/v1 execute, on the server's own data; its grid reads rows from the server too. */
function serverSession(api: string, user: string): RunSession & BuilderSession {
  const http = new HttpEngine(api);
  return {
    modelJson: (text) => http.modelJson(text),
    lambdaJson: (text) => http.lambdaJson(text),
    engine: () => Promise.resolve(http),
    plane: () => Promise.resolve({
      execution: { kind: 'server', engine: api }, engine: http, planner: undefined, user,
      // the server answers pure/v1 at its root; the config names its /api (as Query's does)
      cubeRows: { kind: 'server', baseUrl: api.replace(/\/api\/?$/, '') },
    }),
  };
}

/** A run in this tab: the planner writes the SQL, DuckDB here runs it on the model's own test data (plan A2, A3). */
function inTabSession(grammar: WasmGrammar, duckdbVendor: string, user: string): RunSession & BuilderSession {
  let enumerations = new Set<string>();
  type Started = { engine: BrowserEngine; sql: QueryEngine; tables: TabTables };
  let started: Promise<Started> | undefined;
  const start = (): Promise<Started> => (started ??= startDuckDbInTab(duckdbVendor)
    .then((tab) => ({ engine: new BrowserEngine(grammar, tab.engine, (t) => enumerations.has(t), user), sql: tab.engine, tables: new TabTables(tab.data, tab.engine) })));
  return {
    modelJson: (text) => grammar.modelJson(text),
    lambdaJson: (text) => grammar.lambdaJson(text),
    engine: async () => (await start()).engine,
    sqlEngine: async () => (await start()).sql,
    plane: async () => {
      const tab = await start();
      return { execution: { kind: 'duckdb-wasm', user }, engine: tab.engine, planner: grammar, cubeRows: { kind: 'sql', engine: tab.sql }, user };
    },
    async loadData(model) {
      enumerations = new Set(model.elements.filter((e) => e._type === 'Enumeration').map((e) => `${e.package}::${e.name}`));
      // the model's own rows, but a table a person's file fills keeps that file's (the Data panel)
      await (await start()).tables.load(model.elements as Parameters<TabTables['load']>[0]);
    },
    tabTables: async () => (await start()).tables,
  };
}
