// Studio's app: where projects live (connect), the compiler in a worker, Monaco, and two screens --
// workspace setup (`#/`, `#/project/<id>`) and the editor (`#/edit/<project>/<workspace>`).

import * as monaco from 'monaco-editor/editor/editor.api';
import 'monaco-editor/features/register.all';

import { openDuckDbInTab } from '../../../engine-client/src/doors/tab-duckdb.ts';
import type { QueryEngine } from '../../../engine-client/src/query-engine.ts';
import { TabEngine } from '../../../engine-client/src/legend/browser-engine.ts';
import { LiteServer } from '../../../engine-client/src/doors/lite-server.ts';
import { TabTables } from '../../../engine-client/src/tab-data.ts';
import { ProjectDoor, projectRunner } from '../../../engine-client/src/doors/project-data.ts';
import { WasmGrammar } from '../../../engine-client/src/legend/wasm-grammar.ts';
import { Compiler, WorkerPort } from '../backend/planner.ts';
import { queryBuilder, type BuilderSession } from '../backend/query-builder.ts';
import { playground, runner, type RunSession } from '../backend/run.ts';
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
  // the session's engine (plan A1): legend-lite's, on a server's /api/lite/v1 when the config names one, else the one
  // in this tab (the edge program's H3 and H7, 2026-11-17: legend-engine's pure/v1 is the edge's, never asked here)
  const user = config.user?.userId ?? 'local';
  const inTab = config.engine ? undefined : new WasmGrammar(new WorkerPort(workerUrl, config.vendor));
  const compiler = inTab
    ? new Compiler(inTab, () => inTab.warm({ _type: 'text', code: '' }))
    : new Compiler(new LiteServer(config.engine!));
  // running a function (plan A3): in the tab, DuckDB started the first time and the model's own rows loaded before each
  // run; on a server, lite's plan and run there
  const session: RunSession & BuilderSession = inTab ? inTabSession(inTab, config.vendor, user) : serverSession(config.engine!, user);
  const run = runner(session);
  // upstream's SQL playground: a person's own SQL, the one statement no plan carries, handed to the editor alone
  const sqlPlayground = playground(session);
  // the query builder (plan A5): Query's, over the same session
  const builder = queryBuilder(session);
  let dispose: (() => void) | undefined;

  const route = async (): Promise<void> => {
    dispose?.();
    dispose = undefined;
    clear(root);
    const hash = decodeURIComponent(globalThis.location.hash.slice(1));
    const edit = /^\/edit\/([^/]+)\/([^/]+)$/.exec(hash);
    // upstream's project viewer: a released version, or the project line's head, read-only
    const view = /^\/view\/([^/]+)(?:\/([^/]+))?$/.exec(hash);
    const open = (project: string, workspace: string): void => {
      globalThis.location.hash = `#/edit/${encodeURIComponent(project)}/${encodeURIComponent(workspace)}`;
    };
    try {
      if (view) {
        const project = view[1]!;
        dispose = await renderEditor(root, {
          client, depot, compiler, run, playground: sqlPlayground, builder, monaco, project, workspace: 'view', view: { version: view[2] },
          back: () => { globalThis.location.hash = `#/project/${encodeURIComponent(project)}`; },
        });
      } else if (edit) {
        const project = edit[1]!;
        dispose = await renderEditor(root, {
          client, depot, compiler, run, playground: sqlPlayground, builder, monaco, project, workspace: edit[2]!,
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

/** Runs on a legend-lite server: its /api/lite/v1 plans and runs, on the server's own data; its grid reads rows from
 *  the server too. */
function serverSession(api: string, user: string): RunSession & BuilderSession {
  const lite = new LiteServer(api);
  return {
    modelJson: (text) => lite.modelJson(text),
    lambdaJson: (text) => lite.lambdaJson(text),
    lambdaText: (lambda, style) => lite.lambdaText(lambda, style),
    engine: () => Promise.resolve(lite),
    plane: () => Promise.resolve({
      execution: { kind: 'server', engine: api }, engine: lite, planner: undefined, user,
      // the cube's rows: the server's own plan and run, through the engine (as Query's are)
      cubeRows: { kind: 'server' },
    }),
  };
}

/** A run in this tab: lite's planner plans it, the hosts' runner runs the plan on DuckDB here, on the model's own test
 *  data (plan A2, A3). */
function inTabSession(grammar: WasmGrammar, duckdbVendor: string, user: string): RunSession & BuilderSession {
  type Started = { engine: TabEngine; sql: QueryEngine; databaseType: string; tables: TabTables };
  let started: Promise<Started> | undefined;
  const start = (): Promise<Started> => (started ??= openDuckDbInTab(duckdbVendor, grammar)
    .then((tab) => {
      // the workspace's model in a database of its own inside the tab's DuckDB (C5): every screen runs on `runner`,
      // pointed there as the model loads
      const runner = projectRunner(tab.engine);
      const projects = new ProjectDoor({
        databaseType: tab.engine.databaseType, engine: tab.engine, connect: () => tab.connect(),
      }, grammar);
      return {
        engine: new TabEngine(grammar, runner, tab.engine.databaseType),
        sql: runner,
        databaseType: tab.engine.databaseType,
        tables: new TabTables(projects, runner, tab.engine, tab.data, grammar),
      };
    }));
  return {
    modelJson: (text) => grammar.modelJson(text),
    lambdaJson: (text) => grammar.lambdaJson(text),
    lambdaText: (lambda, style) => grammar.lambdaText(lambda, style),
    engine: async () => (await start()).engine,
    sqlEngine: async () => {
      const tab = await start();
      return { engine: tab.sql, databaseType: tab.databaseType };
    },
    // the playground's statement, planned for the tab's own database (Boundary.authored, 2026-11-12)
    authored: async (text) => grammar.authored((await start()).databaseType, text),
    plane: async () => {
      const tab = await start();
      return { execution: { kind: 'duckdb-wasm', user }, engine: tab.engine, planner: grammar, cubeRows: { kind: 'sql', engine: tab.sql }, user };
    },
    async loadData(model, text) {
      // the model's own rows, but a table a person's file fills keeps that file's (the Data panel)
      await (await start()).tables.load({ text, elements: model.elements as Parameters<TabTables['load']>[0]['elements'] });
    },
    tabTables: async () => (await start()).tables,
  };
}
