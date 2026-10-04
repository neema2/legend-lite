// The editor (upstream Studio's layout, census B §1): an activity bar, a side bar (explorer, local
// changes, review, project), the element tabs over one Monaco editor, the Problems panel and the status
// bar. Text-first (design S13): every element is its own file of Pure text, compiled as you type (S4).

import type * as Monaco from 'monaco-editor/editor/editor.api';

import type { SdlcClient } from '../../../sdlc-client/src/client.ts';
import { SdlcError } from '../../../sdlc-client/src/client.ts';
import type { Compiler } from '../backend/planner.ts';
import { ELEMENT_KINDS, splitPath } from '../model/templates.ts';
import { Workspace, type OpenFile, type Problem } from '../model/workspace.ts';
import { clear, dialog, h, toast } from './dom.ts';
import { PURE } from './pure-language.ts';
import { field } from './setup.ts';
import { renderProject, renderReview } from './sdlc-panels.ts';

export interface EditorContext {
  readonly client: SdlcClient;
  readonly compiler: Compiler;
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
  const ws = new Workspace(ctx.client, ctx.compiler, ctx.project, ctx.workspace);
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
  const empty = h('div', { class: 'editor-empty' },
    h('div', { class: 'editor-empty-title' }, 'Open an element from the explorer, or create one'),
    h('div', { class: 'shortcuts' },
      shortcut('New element', 'Ctrl+Shift+N'), shortcut('Save (push local changes)', 'Ctrl+S'), shortcut('Compile', 'F9')));
  const problemsPanel = h('div', { class: 'panel-body', 'data-testid': 'problems' });
  const problemsTitle = h('div', { class: 'panel-title' }, 'Problems');
  const status = h('div', { class: 'status-bar' });
  const activityBar = h('div', { class: 'activity-bar' });

  root.append(h('div', { class: 'studio' },
    activityBar,
    sideBar,
    h('div', { class: 'main' },
      tabsBar,
      h('div', { class: 'editor-area' }, editorHost, empty),
      h('div', { class: 'panel' }, h('div', { class: 'panel-head' }, problemsTitle), problemsPanel)),
    status));

  const editor = monaco.editor.create(editorHost, {
    model: null,
    theme: 'vs-dark',
    automaticLayout: true,
    fontFamily: "'Roboto Mono', monospace",
    fontSize: 13,
    minimap: { enabled: false },
    tabSize: 2,
    scrollBeyondLastLine: false,
  });
  editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyS, () => void save());
  editor.addCommand(monaco.KeyCode.F9, () => void compile());
  const onKey = (e: KeyboardEvent): void => {
    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 's') { e.preventDefault(); void save(); }
    if (e.key === 'F9') { e.preventDefault(); void compile(); }
    if ((e.ctrlKey || e.metaKey) && e.shiftKey && e.key.toLowerCase() === 'n') { e.preventDefault(); void newElement(); }
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
      tabsBar.append(h('div', {
        class: `tab${key === active ? ' active' : ''}${ws.isChanged(key) ? ' changed' : ''}${errors ? ' has-errors' : ''}`,
        title: label, 'data-key': key, onclick: () => show(key),
      },
      h('span', { class: 'tab-name' }, name),
      h('button', { class: 'tab-close', title: 'Close', onclick: (e: Event) => { e.stopPropagation(); close(key); } }, '×')));
    }
    if (active !== undefined && ws.file(active)) {
      tabsBar.append(h('div', { class: 'tabs-spacer' }),
        h('button', { class: 'btn btn-small', 'data-testid': 'delete-element', title: 'Delete this element', onclick: () => void remove(active!) }, 'Delete'));
    }
  };

  // ---- the side bar ----
  const renderActivityBar = (): void => {
    clear(activityBar);
    const item = (a: Activity, label: string, glyph: string): HTMLElement =>
      h('button', { class: `activity${activity === a ? ' active' : ''}`, title: label, 'data-activity': a, onclick: () => { activity = a; renderActivityBar(); renderSide(); } }, glyph);
    activityBar.append(item('explorer', 'Explorer', '☰'), item('changes', 'Local changes', '±'), item('review', 'Review', '✓'), item('project', 'Project', '▣'),
      h('div', { class: 'activity-spacer' }),
      h('button', { class: 'activity', title: 'Back to workspace setup', onclick: () => ctx.back() }, '⌂'));
  };

  const renderSide = (): void => {
    clear(sideBar);
    if (activity === 'explorer') renderExplorer();
    else if (activity === 'changes') void renderChanges();
    else if (activity === 'review') void renderReview(sideBar, { client: ctx.client, project: ctx.project, workspace: ctx.workspace, hasChanges: () => ws.hasChanges(), reload });
    else void renderProject(sideBar, { client: ctx.client, project: ctx.project, workspace: ctx.workspace });
  };

  const renderExplorer = (): void => {
    const head = h('div', { class: 'side-head' }, h('span', {}, 'Explorer'),
      h('button', { class: 'btn btn-small', 'data-testid': 'new-element', title: 'New element (Ctrl+Shift+N)', onclick: () => void newElement() }, '+ New'));
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
    const draw = (node: Node, prefix: string, depth: number): void => {
      for (const [name, sub] of [...node.pkgs].sort(([a], [b]) => a.localeCompare(b))) {
        const path = prefix ? `${prefix}::${name}` : name;
        const open = !collapsed.has(path);
        tree.append(h('div', { class: 'tree-node package', style: `padding-left:${8 + depth * 12}px`, onclick: () => { if (open) collapsed.add(path); else collapsed.delete(path); renderSide(); } },
          h('span', { class: 'tree-caret' }, open ? '▾' : '▸'), h('span', {}, name)));
        if (open) draw(sub, path, depth + 1);
      }
      for (const { name, file } of node.files.sort((a, b) => a.name.localeCompare(b.name))) {
        const errors = problems.some((p) => p.key === file.key);
        tree.append(h('div', {
          class: `tree-node element${file.key === active ? ' selected' : ''}${ws.isChanged(file.key) ? ' changed' : ''}${errors ? ' has-errors' : ''}`,
          style: `padding-left:${20 + depth * 12}px`, 'data-path': fileLabel(file), onclick: () => show(file.key),
        }, h('span', { class: 'tree-icon' }, kindGlyph(file.text)), h('span', {}, name)));
      }
    };
    draw(rootNode, '', 0);
    for (const path of ws.removed()) {
      tree.append(h('div', { class: 'tree-node element removed', style: 'padding-left:8px' }, h('span', { class: 'tree-icon' }, '−'), h('span', {}, path)));
    }
    sideBar.append(head, tree);
  };

  const renderChanges = async (): Promise<void> => {
    const head = h('div', { class: 'side-head' }, h('span', {}, 'Local changes'),
      h('button', { class: 'btn btn-small btn-primary', onclick: () => void save(), 'data-testid': 'save' }, 'Save'));
    const list = h('div', { class: 'tree', 'data-testid': 'changes' });
    sideBar.append(head, list);
    if (!ws.hasChanges()) {
      list.append(h('div', { class: 'side-empty' }, 'No local changes.'));
      return;
    }
    const { changes, problems: blocked } = await ws.pending();
    if (activity !== 'changes') return;
    for (const c of changes) list.append(h('div', { class: `change change-${c.type.toLowerCase()}` }, h('span', { class: 'change-type' }, c.type[0]), h('span', {}, c.path)));
    for (const p of blocked) list.append(h('div', { class: 'change change-blocked' }, h('span', { class: 'change-type' }, '!'), h('span', {}, `${ws.file(p.key ?? '') ? fileLabel(ws.file(p.key!)!) : ''}: ${p.message}`)));
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
    problemsTitle.textContent = compiling ? 'Problems (compiling…)' : `Problems (${problems.length})`;
    if (problems.length === 0) problemsPanel.append(h('div', { class: 'panel-empty' }, compiling ? '' : 'No problems.'));
    for (const p of problems) {
      const f = p.key ? ws.file(p.key) : undefined;
      problemsPanel.append(h('div', { class: 'problem', onclick: () => {
        if (!p.key) return;
        show(p.key);
        if (p.line) { editor.revealLineInCenter(p.line); editor.setPosition({ lineNumber: p.line, column: p.column ?? 1 }); }
      } },
      h('span', { class: 'problem-icon' }, '⨯'),
      h('span', { class: 'problem-message' }, p.message),
      h('span', { class: 'problem-where' }, f ? `${fileLabel(f)}${p.line ? ` [Ln ${p.line}${p.column ? `, Col ${p.column}` : ''}]` : ''}` : '')));
    }
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
    () => (input.value.trim() === '' ? 'A save needs a message.' : input.value.trim()), 'Save');
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
    toast(`Saved: ${ws.revision?.id.slice(0, 8) ?? ''}`);
    void compile();
  };

  const newElement = async (): Promise<void> => {
    const kind = h('select', { class: 'input', 'data-testid': 'new-kind' }, ...ELEMENT_KINDS.map((k, i) => h('option', { value: String(i) }, k.label)));
    const path = h('input', { class: 'input', placeholder: 'model::domain::Person', 'data-testid': 'new-path' });
    const made = await dialog('New element', h('div', { class: 'form' }, field('Type', kind), field('Path', path)), () => {
      const split = splitPath(path.value);
      if (!split) return 'A full path, like model::domain::Person (a package, then a name).';
      if (ws.files().some((f) => fileLabel(f) === path.value.trim())) return `${path.value.trim()} already exists.`;
      return ELEMENT_KINDS[Number(kind.value)]!.template(split.pkg, split.name);
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
    const ok = await dialog(`Delete ${fileLabel(f)}?`, h('div', {}, 'It is removed from the workspace when you save.'), () => true, 'Delete');
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
    status.append(
      h('button', { class: 'status-item', onclick: () => ctx.back(), title: 'Back to workspace setup' }, `${ctx.project}`),
      h('span', { class: 'status-item' }, `workspace: ${ctx.workspace}`),
      h('span', { class: 'status-item', title: ws.revision?.id ?? '' }, `revision ${ws.revision?.id.slice(0, 8) ?? '—'}`),
      h('span', { class: `status-item${changed ? ' warn' : ''}`, 'data-testid': 'changes-count' }, changed ? `${changed} local change${changed === 1 ? '' : 's'}` : 'no local changes'),
      h('span', { class: `status-item${problems.length ? ' error' : ''}`, 'data-testid': 'problems-count' }, compiling ? 'compiling…' : `${problems.length} problem${problems.length === 1 ? '' : 's'}`),
      h('div', { class: 'status-spacer' }),
      h('button', { class: 'status-item status-action', onclick: () => void compile(), 'data-testid': 'compile' }, 'Compile (F9)'),
      h('button', { class: 'status-item status-action primary', onclick: () => void save(), 'data-testid': 'save-status' }, 'Save (Ctrl+S)'));
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

function shortcut(label: string, keys: string): HTMLElement {
  return h('div', { class: 'shortcut' }, h('span', {}, label), h('kbd', {}, keys));
}

function kindGlyph(text: string): string {
  const k = DECLARES.exec(text.replace(/\/\/.*$/gm, ''))?.[0]?.trim().split(/\s/)[0];
  switch (k) {
    case 'Class': return 'C';
    case 'Enum': return 'E';
    case 'Association': return 'A';
    case 'Profile': return 'P';
    case 'function': return 'ƒ';
    case 'Mapping': return 'M';
    case 'Runtime': return 'R';
    case 'Database': return 'D';
    case 'Service': return 'S';
    default: return '•';
  }
}
