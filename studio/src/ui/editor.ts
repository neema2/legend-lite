// The editor (upstream Studio's layout, census B §1): an activity bar, a side bar (explorer, local
// changes, review, project), the element tabs over one Monaco editor, the Problems panel and the status
// bar. Text-first (design S13): every element is its own file of Pure text, compiled as you type (S4).

import type * as Monaco from 'monaco-editor/editor/editor.api';

import type { DepotClient } from '../../../depot-client/src/client.ts';
import type { SdlcClient } from '../../../sdlc-client/src/client.ts';
import { SdlcError } from '../../../sdlc-client/src/client.ts';
import type { Compiler } from '../backend/planner.ts';
import type { Runner } from '../backend/run.ts';
import type { RawTable } from '../../../engine-client/src/engine.ts';
import { isTds, type ExecutionResult } from '../../../engine-client/src/legend/wire.ts';
import { ELEMENT_KINDS, splitPath } from '../model/templates.ts';
import { Workspace, type OpenFile, type Problem } from '../model/workspace.ts';
import { icon } from '../../../legend-art/src/icon.ts';
import { typeIcon } from '../../../legend-art/src/type-icon.ts';
import type { IconName } from '../../../legend-art/src/icons.ts';
import { clear, dialog, h, headerAction, menu, sideHead, subPanel, toast } from './dom.ts';
import { editorTheme, PURE } from './pure-language.ts';
import { field } from './setup.ts';
import { renderProject, renderReview } from './sdlc-panels.ts';
import { theme, toggleTheme } from './theme.ts';

export interface EditorContext {
  readonly client: SdlcClient;
  readonly depot: DepotClient;
  readonly compiler: Compiler;
  /** Runs a function on the session's engine (plan A3). */
  readonly run: Runner;
  readonly monaco: typeof Monaco;
  readonly project: string;
  readonly workspace: string;
  back(): void;
}

type Activity = 'explorer' | 'changes' | 'review' | 'project';

/** The element a file's text declares, read without the compiler (for labels while typing). */
const DECLARES = /^\s*(?:Class|Enum|Association|Profile|function|Mapping|Runtime|Database|Service|Measure|RelationalDatabaseConnection|Data|DataSpace|Diagram)\s+(?:<<[^>]*>>\s*)?(?:\{[^}]*\}\s*)?([\w$]+(?:::[\w$]+)+)/m;

export function fileLabel(f: OpenFile): string {
  if (f.savedPath !== undefined) return f.savedPath;
  return DECLARES.exec(f.text.replace(/\/\/.*$/gm, ''))?.[1] ?? f.key;
}

export async function renderEditor(root: HTMLElement, ctx: EditorContext): Promise<() => void> {
  const { monaco } = ctx;
  const ws = new Workspace(ctx.client, ctx.compiler, ctx.project, ctx.workspace, ctx.depot);
  await ws.load();

  const models = new Map<string, Monaco.editor.ITextModel>();
  const viewStates = new Map<string, Monaco.editor.ICodeEditorViewState | null>();
  let tabs: string[] = [];
  let active: string | undefined;
  let problems: Problem[] = [];
  let activity: Activity = 'explorer';
  let compiling = false;
  let compileTimer: ReturnType<typeof setTimeout> | undefined;
  const collapsed = new Set<string>();

  // ---- layout ----
  const sideBar = h('div', { class: 'side-bar' });
  const tabsBar = h('div', { class: 'tabs', 'data-testid': 'tabs' });
  const editorHost = h('div', { class: 'editor-host' });
  // upstream's empty-editor splash (census 4.2), its shortcuts block: the ones this Studio has, in upstream's words
  // (upstream's cards -- showcases, documentation -- have nothing to point at here yet)
  const empty = h('div', { class: 'editor-empty' },
    h('div', { class: 'editor-empty__content' },
      h('div', { class: 'editor-empty__title' }, 'Essential Keyboard Shortcuts'),
      h('div', { class: 'shortcuts' },
        shortcut('Push Local Changes', ['Ctrl', 'S']), shortcut('Compile', ['F9']), shortcut('New Element', ['Ctrl', 'Shift', 'N']))));
  // upstream's panel group (census 6): closed at first; the PROBLEMS tab with its count, expand and close; opened
  // to 300px from the status bar (its problems counts, or the terminal toggle)
  const problemsPanel = h('div', { class: 'panel-group__content', 'data-testid': 'problems' });
  const problemsBadge = h('div', { class: 'panel-group__badge' });
  // a function's run (plan A3): its rows, or what refused it
  const resultsPanel = h('div', { class: 'panel-group__content', 'data-testid': 'results' },
    h('div', { class: 'panel-group__empty' }, 'Run a function to see its result here.'));
  // upstream's SQL playground: SQL on the tab's DuckDB, the model's own rows loaded first (plan A3)
  const sqlText = h('textarea', { class: 'input sql-playground__text', 'data-testid': 'sql-text', placeholder: 'SELECT * FROM "PARTY"."PARTY"', spellcheck: 'false' });
  const sqlOut = h('div', { class: 'sql-playground__out', 'data-testid': 'sql-out' });
  const runSql = async (): Promise<void> => {
    clear(sqlOut);
    sqlOut.append(h('div', { class: 'panel-group__empty' }, 'Running…'));
    const started = performance.now();
    try {
      const t = await ctx.run.sql(sqlText.value, ws.model().text);
      clear(sqlOut);
      sqlOut.append(rawView(t, Math.round(performance.now() - started)));
    } catch (e) {
      clear(sqlOut);
      sqlOut.append(h('div', { class: 'panel-group__run-error' }, icon('error'), h('span', {}, e instanceof Error ? e.message : String(e))));
    }
  };
  sqlText.addEventListener('keydown', (e) => { if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') { e.preventDefault(); void runSql(); } });
  const sqlPanel = h('div', { class: 'panel-group__content sql-playground', 'data-testid': 'sql-playground' },
    h('div', { class: 'sql-playground__editor' }, sqlText,
      h('button', { class: 'btn btn-primary', 'data-testid': 'sql-run', title: 'Run the SQL (Ctrl + Enter)', onclick: () => void runSql() }, icon('play', '10px'), 'Run')),
    sqlOut);
  let panelOpen = false;
  let panelMaximised = false;
  let panelTab: 'problems' | 'results' | 'sql' = 'problems';
  const panel = h('div', { class: 'panel-group' });
  const main = h('div', { class: 'main' });
  const renderPanel = (): void => {
    panel.classList.toggle('panel-group--closed', !panelOpen);
    main.classList.toggle('main--panel-maximised', panelOpen && panelMaximised);
    clear(panel);
    const tab = (t: 'problems' | 'results' | 'sql', label: string, badge?: HTMLElement): HTMLElement =>
      h('button', { class: `panel-group__tab${panelTab === t ? ' panel-group__tab--active' : ''}`, 'data-panel-tab': t,
        onclick: () => { panelTab = t; renderPanel(); } }, label, badge);
    panel.append(
      h('div', { class: 'panel-group__header' },
        h('div', { class: 'panel-group__tabs' }, tab('problems', 'Problems', problemsBadge), tab('results', 'Results'), tab('sql', 'SQL Playground')),
        h('div', { class: 'panel-group__actions' },
          h('button', { class: 'panel-group__action', title: 'Toggle expand/collapse', onclick: () => { panelMaximised = !panelMaximised; renderPanel(); } },
            icon(panelMaximised ? 'chevronDown' : 'chevronUp', '18px')),
          h('button', { class: 'panel-group__action', title: 'Close', onclick: () => { panelOpen = false; renderPanel(); renderStatus(); } }, icon('x', '18px')))),
      panelTab === 'problems' ? problemsPanel : panelTab === 'results' ? resultsPanel : sqlPanel);
  };
  const openPanel = (): void => {
    panelTab = 'problems';
    panelOpen = true;
    renderPanel();
    renderStatus();
  };
  const status = h('div', { class: 'status-bar' });
  const activityBar = h('div', { class: 'activity-bar' });

  main.append(tabsBar, h('div', { class: 'editor-area' }, editorHost, empty), panel);
  root.append(h('div', { class: 'studio' }, activityBar, sideBar, main, status));
  renderPanel();

  // upstream's options (census 5.2, CodeEditorUtils.ts:51-78); what it leaves unset (minimap, line height, scrolling
  // past the end) stays Monaco's default here too
  const editor = monaco.editor.create(editorHost, {
    model: null,
    theme: editorTheme(theme() === 'light'),
    automaticLayout: true,
    fontFamily: "'Roboto Mono'",
    fontSize: 14,
    fontLigatures: true,
    tabSize: 2,
    detectIndentation: false,
    contextmenu: false,
    copyWithSyntaxHighlighting: false,
    bracketPairColorization: { enabled: false },
    fixedOverflowWidgets: true,
    renderValidationDecorations: 'on',
  });
  editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyS, () => void save());
  editor.addCommand(monaco.KeyCode.F9, () => void compile());
  editor.addCommand(monaco.KeyCode.F5, () => void runActive());
  const onKey = (e: KeyboardEvent): void => {
    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 's') { e.preventDefault(); void save(); }
    if (e.key === 'F9') { e.preventDefault(); void compile(); }
    if (e.key === 'F5') { e.preventDefault(); void runActive(); }
    if ((e.ctrlKey || e.metaKey) && e.shiftKey && e.key.toLowerCase() === 'n') { e.preventDefault(); void newElement(); }
    if (e.ctrlKey && e.key === '`') { e.preventDefault(); panelOpen = !panelOpen; renderPanel(); renderStatus(); }
  };
  document.addEventListener('keydown', onKey);

  // ---- models ----
  const modelOf = (key: string): Monaco.editor.ITextModel => {
    let m = models.get(key);
    if (!m) {
      const f = ws.file(key);
      m = monaco.editor.createModel(f?.text ?? '', PURE);
      m.onDidChangeContent(() => {
        if (!ws.file(key)) return;
        ws.edit(key, m!.getValue());
        renderTabs();
        renderStatus();
        if (activity === 'explorer' || activity === 'changes') renderSide();
        scheduleCompile();
      });
      models.set(key, m);
    }
    return m;
  };

  /** Drops every model and re-opens the tabs that still have a file (after a load or a save). */
  const resync = (renamed: Map<string, string>): void => {
    for (const m of models.values()) m.dispose();
    models.clear();
    viewStates.clear();
    const keys = new Set(ws.files().map((f) => f.key));
    tabs = tabs.map((k) => renamed.get(k) ?? k).filter((k) => keys.has(k));
    if (active !== undefined) active = renamed.get(active) ?? active;
    if (active !== undefined && !keys.has(active)) active = tabs[0];
    show(active);
  };

  // ---- tabs and the editor ----
  const show = (key: string | undefined): void => {
    if (active !== undefined && models.has(active)) viewStates.set(active, editor.saveViewState());
    active = key;
    if (key === undefined) {
      editor.setModel(null);
      empty.style.display = '';
      editorHost.style.visibility = 'hidden';
    } else {
      if (!tabs.includes(key)) tabs.push(key);
      editor.setModel(modelOf(key));
      const vs = viewStates.get(key);
      if (vs) editor.restoreViewState(vs);
      empty.style.display = 'none';
      editorHost.style.visibility = '';
      editor.focus();
    }
    applyMarkers();
    renderTabs();
    renderSide();
  };

  const close = (key: string): void => {
    tabs = tabs.filter((k) => k !== key);
    if (active === key) show(tabs[tabs.length - 1]);
    else renderTabs();
  };

  const renderTabs = (): void => {
    clear(tabsBar);
    for (const key of tabs) {
      const f = ws.file(key);
      if (!f) continue;
      const label = fileLabel(f);
      const name = label.split('::').pop() ?? label;
      const errors = problems.filter((p) => p.key === key).length;
      // upstream's tab (census 4.1): the type icon and the name, the full path as tooltip; the close button shows on
      // the active or hovered tab; a middle click closes. No unsaved marker (upstream shows that in the status bar
      // and on the activity bar). A tab with compile errors keeps lite's red name.
      tabsBar.append(h('div', {
        class: `tab${key === active ? ' active' : ''}${errors ? ' has-errors' : ''}`,
        title: label, 'data-key': key, onclick: () => show(key),
        onauxclick: (e: Event) => { if ((e as MouseEvent).button === 1) { e.preventDefault(); close(key); } },
      },
      h('div', { class: 'tab__label' }, typeIcon(kindOf(f.text)), h('span', { class: 'tab-name' }, name)),
      h('button', { class: 'tab__close', title: 'Close', onclick: (e: Event) => { e.stopPropagation(); close(key); } }, icon('times', '12px'))));
    }
    if (active !== undefined && ws.file(active)) {
      const runnable = ['function', 'Service'].includes(kindOf(ws.file(active)!.text) ?? '');
      tabsBar.append(h('div', { class: 'tabs-spacer' }),
        ...(runnable ? [h('button', { class: 'btn btn-small btn-primary tabs__run', 'data-testid': 'run-function', title: 'Run (F5)', onclick: () => void runActive() },
          icon('play', '10px'), 'Run')] : []),
        h('button', { class: 'btn btn-small', 'data-testid': 'delete-element', title: 'Delete this element', onclick: () => void remove(active!) }, 'Delete'));
    }
  };

  /** Runs the open function or service on the session's engine (plan A3): its rows in RESULTS, or what refused it. */
  const runActive = async (): Promise<void> => {
    const f = active === undefined ? undefined : ws.file(active);
    if (!f || !['function', 'Service'].includes(kindOf(f.text) ?? '')) return;
    // a function's parameters first (upstream's parameter dialog): each value as Pure, which the compiler reads
    let values: Map<string, string> | undefined;
    try {
      const parameters = await ctx.run.parameters(f.text);
      if (parameters.length > 0) {
        const inputs = parameters.map((p) => ({ p, input: h('input', { class: 'input', placeholder: placeholderFor(p.type), 'data-param': p.name }) }));
        values = await dialog('Run with parameters', h('div', { class: 'form' },
          ...inputs.map(({ p, input }) => field(`${p.name}: ${p.type}${p.multiplicity}`, input))),
        () => {
          const empty = inputs.find(({ input }) => input.value.trim() === '');
          return empty ? `a value for ${empty.p.name} is needed` : { ok: new Map(inputs.map(({ p, input }) => [p.name, input.value])) };
        }, 'Run');
        if (!values) return;
      }
    } catch (e) {
      toast(e instanceof Error ? e.message : String(e), 'error');
      return;
    }
    panelTab = 'results';
    panelOpen = true;
    clear(resultsPanel);
    resultsPanel.append(h('div', { class: 'panel-group__empty', 'data-testid': 'run-status' }, 'Running…'));
    renderPanel();
    renderStatus();
    const started = performance.now();
    try {
      const result = await ctx.run.run(f.text, ws.model().text, values);
      clear(resultsPanel);
      resultsPanel.append(resultView(result, Math.round(performance.now() - started)));
    } catch (e) {
      clear(resultsPanel);
      resultsPanel.append(h('div', { class: 'panel-group__run-error', 'data-testid': 'run-status' }, icon('error'), h('span', {}, e instanceof Error ? e.message : String(e))));
    }
  };

  // ---- the side bar ----
  // upstream's activity bar (census 2.1): the menu cell, the activities in upstream's order with upstream's icons
  // and sizes, and the theme switch pinned to the bottom. Active and hover change only the colour.
  const renderActivityBar = (): void => {
    clear(activityBar);
    const changed = ws.removed().length + ws.files().filter((f) => ws.isChanged(f.key)).length;
    const item = (a: Activity, glyph: IconName, size: string, label: string, badge?: HTMLElement): HTMLElement =>
      h('button', { class: `activity-bar__item${activity === a ? ' activity-bar__item--active' : ''}`, title: label, 'data-activity': a,
        onclick: () => { activity = a; renderActivityBar(); renderSide(); } },
      h('div', { class: 'activity-bar__item__icon-with-indicator' }, icon(glyph, size), badge));
    const counter = changed === 0 ? undefined
      : h('div', { class: 'activity-bar__local-change-counter', 'data-testid': 'activity-changes-count' }, changed > 99 ? '99+' : String(changed));
    const menuCell: HTMLButtonElement = h('button', { class: 'activity-bar__menu', title: 'Menu', 'data-testid': 'activity-menu',
      onclick: () => menu(menuCell, [{ label: 'Back to workspace setup', run: () => ctx.back(), testId: 'menu-back' }]) },
    icon('menu', '23px'));
    const dark = theme() === 'dark';
    activityBar.append(
      menuCell,
      h('div', { class: 'activity-bar__items' },
        item('explorer', 'fileTray', '23px', 'Explorer (Ctrl + Shift + X)'),
        item('changes', 'codeBranch', '20px', `Local Changes (Ctrl + Shift + G)${changed ? ` - ${changed} unpushed change${changed === 1 ? '' : 's'}` : ''}`, counter),
        item('review', 'gitPullRequest', '23px', 'Review (Ctrl + Shift + M)'),
        item('project', 'repo', '23px', 'Project')),
      h('button', { class: 'activity-bar__item', title: dark ? 'Switch to light theme' : 'Switch to dark theme', 'data-testid': 'theme-toggle',
        onclick: () => { const next = toggleTheme(); monaco.editor.setTheme(editorTheme(next === 'light')); renderActivityBar(); } },
      icon(dark ? 'sun' : 'moon', '20px')));
  };

  const renderSide = (): void => {
    clear(sideBar);
    if (activity === 'explorer') renderExplorer();
    else if (activity === 'changes') void renderChanges();
    else {
      const panel = {
        client: ctx.client, depot: ctx.depot, project: ctx.project, workspace: ctx.workspace, ws, reload,
        gone: (message: string) => { toast(message); ctx.back(); },
      };
      const render = activity === 'review' ? renderReview : renderProject;
      render(sideBar, panel).catch((e: unknown) => toast(e instanceof Error ? e.message : String(e), 'error'));
    }
  };

  const renderExplorer = (): void => {
    // upstream's explorer (census 3): the side-bar header, then the sub-header -- the "workspace" chip, its id, and
    // the actions this Studio has (New Element, Collapse All) -- then the tree
    const head = h('div', { class: 'side-head' }, h('span', { class: 'side-head__title' }, 'Explorer'));
    const subHead = h('div', { class: 'explorer__header' },
      h('div', { class: 'explorer__header__chip' }, 'workspace'),
      h('div', { class: 'explorer__header__title', title: ctx.workspace }, ctx.workspace),
      h('div', { class: 'panel__header__actions' },
        h('button', { class: 'panel__header__action', 'data-testid': 'new-element', title: 'New Element... (Ctrl + Shift + N)', onclick: () => void newElement() }, icon('plus')),
        h('button', { class: 'panel__header__action', title: 'Collapse All', onclick: () => { for (const p of packagePaths()) collapsed.add(p); renderSide(); } }, icon('compress'))));
    const tree = h('div', { class: 'tree', 'data-testid': 'explorer' });
    const files = ws.files();
    if (files.length === 0) tree.append(h('div', { class: 'side-empty' }, 'This workspace is empty: create an element.'));
    // package → its files and sub-packages
    interface Node { readonly pkgs: Map<string, Node>; readonly files: { name: string; file: OpenFile }[] }
    const rootNode: Node = { pkgs: new Map(), files: [] };
    for (const f of files) {
      const parts = fileLabel(f).split('::');
      let node = rootNode;
      for (const p of parts.slice(0, -1)) {
        let next = node.pkgs.get(p);
        if (!next) node.pkgs.set(p, next = { pkgs: new Map(), files: [] });
        node = next;
      }
      node.files.push({ name: parts[parts.length - 1]!, file: f });
    }
    // a row (census 3.2): 22px, indented 10px a level; a 40px icon block -- the expand chevron, then the folder or
    // the element's type icon -- then the label, whose tooltip is the full path
    const row = (depth: number, cls: string, expand: HTMLElement | undefined, type: HTMLElement, label: string, title: string, onclick?: () => void, path?: string): HTMLElement =>
      h('div', { class: `tree-node ${cls}`, style: `padding-left:${depth * 10}px`, 'data-path': path, onclick },
        h('div', { class: 'tree-node__icon' }, h('div', { class: 'tree-node__icon__expand' }, expand), h('div', { class: 'tree-node__icon__type' }, type)),
        h('div', { class: 'tree-node__label', title }, label));
    const draw = (node: Node, prefix: string, depth: number): void => {
      for (const [name, sub] of [...node.pkgs].sort(([a], [b]) => a.localeCompare(b))) {
        const path = prefix ? `${prefix}::${name}` : name;
        const open = !collapsed.has(path);
        tree.append(row(depth, 'package', icon(open ? 'chevronDown' : 'chevronRight', '10px'), icon(open ? 'folderOpen' : 'folder'), name, path,
          () => { if (open) collapsed.add(path); else collapsed.delete(path); renderSide(); }));
        if (open) draw(sub, path, depth + 1);
      }
      for (const { name, file } of node.files.sort((a, b) => a.name.localeCompare(b.name))) {
        const errors = problems.some((p) => p.key === file.key);
        tree.append(row(depth, `element${file.key === active ? ' selected' : ''}${ws.isChanged(file.key) ? ' changed' : ''}${errors ? ' has-errors' : ''}`,
          undefined, typeIcon(kindOf(file.text)), name, fileLabel(file), () => show(file.key), fileLabel(file)));
      }
    };
    draw(rootNode, '', 0);
    for (const path of ws.removed()) tree.append(row(0, 'element removed', undefined, icon('trash'), path, `${path} (removed, not yet saved)`));
    sideBar.append(head, h('div', { class: 'explorer' }, subHead, tree));
  };

  /** Every package path in the workspace, for Collapse All. */
  const packagePaths = (): string[] => {
    const out = new Set<string>();
    for (const f of ws.files()) {
      const parts = fileLabel(f).split('::').slice(0, -1);
      for (let i = 1; i <= parts.length; i++) out.add(parts.slice(0, i).join('::'));
    }
    return [...out];
  };

  const renderChanges = async (): Promise<void> => {
    // upstream's Local Changes (census 8.1): the header with Push, then the CHANGES sub-panel -- a diff row per
    // change: the name, the path in grey, upstream's letter (N new, M modified, D deleted) in its colour. A change
    // the compiler refuses (lite's: it cannot be saved yet) shows its message.
    const changesBody = h('div', { class: 'side-bar__body' });
    sideBar.append(sideHead('Local Changes', headerAction('cloudUpload', 'Push local changes (Ctrl + S)', ws.hasChanges() ? () => void save() : undefined, { 'data-testid': 'save' })), changesBody);
    const { changes, problems: blocked } = ws.hasChanges() ? await ws.pending() : { changes: [], problems: [] };
    if (activity !== 'changes') return;
    const LETTER: Record<string, string> = { CREATE: 'N', MODIFY: 'M', DELETE: 'D' };
    const rows = changes.map((c) => {
      const name = c.path.split('::').pop() ?? c.path;
      return h('div', { class: `side-bar__panel__item diff-item diff-item--${c.type.toLowerCase()}`, title: c.path },
        h('div', { class: 'diff-item__name' }, name), h('div', { class: 'diff-item__path' }, c.path),
        h('div', { class: 'diff-item__type' }, LETTER[c.type] ?? c.type[0]));
    });
    for (const p of blocked) {
      rows.push(h('div', { class: 'side-bar__panel__item diff-item diff-item--blocked', title: p.message },
        h('div', { class: 'diff-item__name' }, ws.file(p.key ?? '') ? fileLabel(ws.file(p.key!)!) : ''), h('div', { class: 'diff-item__path' }, p.message),
        h('div', { class: 'diff-item__type' }, '!')));
    }
    changesBody.append(subPanel('Changes', { info: 'All local changes that have not been yet pushed with the server', count: changes.length, testId: 'changes' },
      ...(rows.length ? rows : [h('div', { class: 'side-bar__panel__empty' }, 'No local changes')])));
  };

  // ---- problems and compile ----
  const applyMarkers = (): void => {
    for (const [key, model] of models) {
      const own = problems.filter((p) => p.key === key);
      monaco.editor.setModelMarkers(model, 'compile', own.map((p) => {
        const line = Math.min(Math.max(p.line ?? 1, 1), model.getLineCount());
        return {
          severity: monaco.MarkerSeverity.Error, message: p.message,
          startLineNumber: line, startColumn: p.column ?? 1, endLineNumber: line, endColumn: model.getLineMaxColumn(line),
        };
      }));
    }
  };

  const renderProblems = (): void => {
    clear(problemsPanel);
    problemsBadge.textContent = String(problems.length);
    // census 6: upstream's empty text; a row per problem -- the error icon, the message (its full text as tooltip),
    // and where: the element and [Ln, Col] (upstream's text mode shows [Ln, Col]; lite's text is per element)
    if (problems.length === 0) problemsPanel.append(h('div', { class: 'panel-group__empty' }, compiling ? '' : 'No problems have been detected in the workspace.'));
    const list = h('div', { class: 'panel-group__list' });
    for (const p of problems) {
      const f = p.key ? ws.file(p.key) : undefined;
      list.append(h('button', { class: 'panel-group__problem', title: p.message, onclick: () => {
        if (!p.key) return;
        show(p.key);
        if (p.line) { editor.revealLineInCenter(p.line); editor.setPosition({ lineNumber: p.line, column: p.column ?? 1 }); }
      } },
      h('div', { class: 'panel-group__problem__icon' }, icon('error')),
      h('div', { class: 'panel-group__problem__message' }, p.message),
      h('div', { class: 'panel-group__problem__source' }, f ? `${fileLabel(f)}${p.line ? ` [Ln ${p.line}${p.column ? `, Col ${p.column}` : ''}]` : ''}` : '')));
    }
    if (problems.length > 0) problemsPanel.append(list);
  };

  const setProblems = (next: Problem[]): void => {
    problems = next;
    applyMarkers();
    renderProblems();
    renderTabs();
    renderStatus();
    if (activity === 'explorer') renderSide();
  };

  const compile = async (): Promise<void> => {
    if (compileTimer) clearTimeout(compileTimer);
    compiling = true;
    renderProblems();
    renderStatus();
    try {
      setProblems(await ws.compile());
    } catch (e) {
      setProblems([{ message: e instanceof Error ? e.message : String(e) }]);
    } finally {
      compiling = false;
      renderProblems();
      renderStatus();
    }
  };

  const scheduleCompile = (): void => {
    if (compileTimer) clearTimeout(compileTimer);
    compileTimer = setTimeout(() => void compile(), 700);
  };

  // ---- actions ----
  const save = async (): Promise<void> => {
    if (!ws.hasChanges()) {
      toast('Nothing to save.');
      return;
    }
    const pending = await ws.pending();
    if (pending.problems.length > 0) {
      setProblems([...pending.problems, ...problems.filter((p) => !pending.problems.some((q) => q.key === p.key))]);
      toast('Some files cannot be saved: see Problems.', 'error');
      return;
    }
    const input = h('input', { class: 'input', value: `${pending.changes.length} change${pending.changes.length === 1 ? '' : 's'} from Legend Studio` });
    const message = await dialog('Save local changes', h('div', { class: 'form' },
      h('div', { class: 'dialog-list' }, ...pending.changes.map((c) => h('div', {}, `${c.type}  ${c.path}`))),
      field('Message', input)),
    () => (input.value.trim() === '' ? 'A save needs a message.' : { ok: input.value.trim() }), 'Save');
    if (!message) return;
    // the open tabs' keys become paths once saved
    const before = new Map(ws.files().map((f) => [f.key, fileLabel(f)]));
    try {
      const blocked = await ws.save(message);
      if (blocked.length > 0) {
        setProblems(blocked);
        return;
      }
    } catch (e) {
      if (e instanceof SdlcError && e.status === 409) {
        toast('The workspace has moved on since you opened it (saved elsewhere): reload it to continue.', 'error');
      } else {
        toast(e instanceof Error ? e.message : String(e), 'error');
      }
      return;
    }
    const renamed = new Map<string, string>();
    for (const [key, label] of before) if (ws.file(label)) renamed.set(key, label);
    resync(renamed);
    renderSide();
    renderStatus();
    toast(`Saved: ${ws.revision?.id.slice(0, 8) ?? ''}`, 'success');
    void compile();
  };

  const newElement = async (): Promise<void> => {
    const kind = h('select', { class: 'input', 'data-testid': 'new-kind' }, ...ELEMENT_KINDS.map((k, i) => h('option', { value: String(i) }, k.label)));
    const path = h('input', { class: 'input', placeholder: 'model::domain::Person', 'data-testid': 'new-path' });
    const made = await dialog('New element', h('div', { class: 'form' }, field('Type', kind), field('Path', path)), () => {
      const split = splitPath(path.value);
      if (!split) return 'A full path, like model::domain::Person (a package, then a name).';
      if (ws.files().some((f) => fileLabel(f) === path.value.trim())) return `${path.value.trim()} already exists.`;
      return { ok: ELEMENT_KINDS[Number(kind.value)]!.template(split.pkg, split.name) };
    }, 'Create');
    if (made === undefined) return;
    show(ws.add(made));
    renderSide();
    renderStatus();
    scheduleCompile();
  };

  const remove = async (key: string): Promise<void> => {
    const f = ws.file(key);
    if (!f) return;
    const ok = await dialog(`Delete ${fileLabel(f)}?`, h('div', {}, 'It is removed from the workspace when you save.'), () => ({ ok: true }), 'Delete');
    if (!ok) return;
    ws.remove(key);
    models.get(key)?.dispose();
    models.delete(key);
    close(key);
    renderSide();
    renderStatus();
    scheduleCompile();
  };

  const reload = async (): Promise<void> => {
    await ws.load();
    tabs = [];
    resync(new Map());
    renderSide();
    renderStatus();
    void compile();
  };

  // ---- status bar ----
  const renderStatus = (): void => {
    clear(status);
    const changed = ws.removed().length + ws.files().filter((f) => ws.isChanged(f.key)).length;
    // upstream's status bar (census 7): left, the branch icon, project / workspace (a * for unpushed changes; each
    // goes back to setup) and the problems counts (they open Problems); right, the sync text, the push button,
    // Compile (the hammer wiggles while compiling) and the panel toggle. `data-*` are the harness's to read.
    const errors = problems.length;
    status.append(
      h('div', { class: 'status-bar__left' },
        h('div', { class: 'status-bar__workspace' },
          icon('codeBranch'),
          h('button', { class: 'status-bar__workspace__project', title: 'Go back to workspace setup using the specified project', onclick: () => ctx.back() }, ctx.project),
          '/',
          h('button', { class: 'status-bar__workspace__workspace', title: 'Go back to workspace setup using the specified workspace', onclick: () => ctx.back() },
            `${ctx.workspace}${changed ? '*' : ''}`)),
        h('button', { class: 'status-bar__problems', title: `Error: ${errors}, Warnings: 0`, 'data-testid': 'problems-count',
          'data-errors': errors, 'data-state': compiling ? 'compiling' : 'idle', onclick: openPanel },
        icon('error'), h('div', { class: 'status-bar__problems__count' }, String(errors)),
        icon('vscWarning'), h('div', { class: 'status-bar__problems__count' }, '0'))),
      h('div', { class: 'status-bar__right' },
        h('div', { class: 'status-bar__sync', 'data-testid': 'changes-count', title: ws.revision?.id ?? '' },
          changed ? `${changed} unpushed change${changed === 1 ? '' : 's'}` : 'no changes detected'),
        h('button', { class: 'status-bar__push', title: 'Push local changes (Ctrl + S)', 'data-testid': 'save-status', disabled: changed === 0, onclick: () => void save() },
          icon('cloudUpload', '16px')),
        h('button', { class: `status-bar__action${compiling ? ' status-bar__action--compiling' : ''}`, title: 'Compile (F9)', 'data-testid': 'compile', onclick: () => void compile() },
          icon('hammer')),
        h('button', { class: `status-bar__action status-bar__toggler${panelOpen ? ' status-bar__toggler--on' : ''}`, title: 'Toggle panel (Ctrl + `)',
          onclick: () => { panelOpen = !panelOpen; renderPanel(); renderStatus(); } },
        icon('terminal'))));
    renderActivityBar();     // its local-change counter follows the same count
  };

  renderActivityBar();
  show(undefined);
  renderStatus();
  renderProblems();
  void compile();

  return () => {
    document.removeEventListener('keydown', onKey);
    if (compileTimer) clearTimeout(compileTimer);
    editor.dispose();
    for (const m of models.values()) m.dispose();
  };
}

/** A shortcut on the splash (upstream's hotkey look): the label, then each key as a key cap, a plus between. */
function shortcut(label: string, keys: readonly string[]): HTMLElement {
  const caps: Node[] = [];
  keys.forEach((k, i) => {
    if (i > 0) caps.push(icon('plus'));
    caps.push(h('kbd', { class: 'hotkey__key' }, k));
  });
  return h('div', { class: 'shortcut' }, h('div', { class: 'shortcut__label' }, label), h('div', { class: 'hotkey' }, ...caps));
}

/** The keyword an element's text declares it with (`Class`, `function`, ...), for its type icon. */
function kindOf(text: string): string | undefined {
  return DECLARES.exec(text.replace(/\/\/.*$/gm, ''))?.[0]?.trim().split(/\s/)[0];
}

/** A run's answer: a TDS as a table -- how many rows, how long, the SQL the tab ran -- else its JSON. */
function resultView(r: ExecutionResult, ms: number): HTMLElement {
  if (!isTds(r)) return h('pre', { class: 'run-result__json' }, JSON.stringify(r, null, 2));
  const sql = r.activities?.find((a) => a.sql)?.sql;
  const rows = r.result.rows;
  return h('div', { class: 'run-result' },
    h('div', { class: 'run-result__bar', 'data-testid': 'run-status' },
      `${rows.length} row${rows.length === 1 ? '' : 's'} in ${ms} ms`,
      sql ? h('span', { class: 'run-result__sql', title: sql }, 'SQL') : null),
    h('table', { class: 'run-result__table', 'data-testid': 'run-rows' },
      h('thead', {}, h('tr', {}, ...r.result.columns.map((c) => h('th', {}, c)))),
      h('tbody', {}, ...rows.map((row) => h('tr', {}, ...row.values.map((v) => h('td', {}, v === null || v === undefined ? '' : String(v))))))));
}

/** A hint of how a value of `type` is written in Pure, for the parameter dialog's field. */
function placeholderFor(type: string): string {
  switch (type) {
    case 'String': return "'text'";
    case 'Integer': return '42';
    case 'Float': case 'Number': case 'Decimal': return '4.2';
    case 'Boolean': return 'true';
    case 'Date': case 'StrictDate': return '%2024-06-01';
    case 'DateTime': return '%2024-06-01T09:30:00';
    default: return `a ${type}, as Pure`;
  }
}

/** Raw SQL's answer (the SQL playground): its columns and rows, how many, how long. */
function rawView(t: RawTable, ms: number): HTMLElement {
  const rows = Array.from({ length: t.rowCount }, (_, i) => t.columns.map((c) => c.values[i]));
  return h('div', { class: 'run-result' },
    h('div', { class: 'run-result__bar', 'data-testid': 'sql-status' }, `${t.rowCount} row${t.rowCount === 1 ? '' : 's'} in ${ms} ms`),
    h('table', { class: 'run-result__table', 'data-testid': 'sql-rows' },
      h('thead', {}, h('tr', {}, ...t.columns.map((c) => h('th', {}, c.name)))),
      h('tbody', {}, ...rows.map((row) => h('tr', {}, ...row.map((v) => h('td', {}, v === null || v === undefined ? '' : String(v))))))));
}
