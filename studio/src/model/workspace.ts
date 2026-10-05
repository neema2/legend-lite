// A workspace open in Studio: its files as the SDLC holds them (at the revision the editor started
// from), the text being edited, what a save sends, and what a compile says -- with no DOM, so tests
// drive it with the real SDLC and compiler.
//
// One file per element (design S5). A file is known by a KEY that does not change while it is edited:
// its path when it came from the SDLC, `new:<n>` when it was made here. What path a file is FOR is read
// from its text at save time (the compiler's grammarToJson), so renaming an element in its text is a
// delete of the old path and a create of the new one -- the text is the truth, not a form field.

import type { DepotClient } from '../../../depot-client/src/client.ts';
import type { SdlcClient } from '../../../sdlc-client/src/client.ts';
import type { ProjectConfiguration, PureChange, PureFile, Revision } from '../../../sdlc-client/src/wire.ts';
import type { Compiler } from '../backend/planner.ts';

/** One file in the editor. */
export interface OpenFile {
  readonly key: string;
  /** The path it has at the SDLC's revision; undefined for a file made here and not saved yet. */
  readonly savedPath: string | undefined;
  text: string;
}

/** A problem a compile or a save reports, attached to a file and line when it can be. */
export interface Problem {
  readonly message: string;
  readonly key?: string;
  /** 1-based, in the file. */
  readonly line?: number;
  readonly column?: number;
}

/** What the next save would send, or why it cannot. */
export interface Pending {
  readonly changes: PureChange[];
  readonly problems: Problem[];
}

export class Workspace {
  readonly project: string;
  readonly workspace: string;
  readonly #client: SdlcClient;
  readonly #compiler: Compiler;
  #revision: Revision | undefined;
  /** path → text at the revision. */
  #saved = new Map<string, string>();
  /** key → file being edited (deleted files are absent). */
  #files = new Map<string, OpenFile>();
  #next = 1;
  readonly #depot: DepotClient | undefined;
  /** Model text of the dependencies, compiled with the workspace (their files from Depot, nearest wins). */
  dependencies = '';
  /** Why the dependencies could not be read, when they could not (shown as a problem). */
  dependencyProblem: string | undefined;
  #configuration: ProjectConfiguration | undefined;
  /** Viewing, read-only (upstream's project viewer): a released version, or the project line's head (undefined). */
  readonly view: { readonly version: string | undefined } | undefined;

  constructor(client: SdlcClient, compiler: Compiler, project: string, workspace: string, depot?: DepotClient,
    view?: { readonly version: string | undefined }) {
    this.#client = client;
    this.#compiler = compiler;
    this.project = project;
    this.workspace = workspace;
    this.#depot = depot;
    this.view = view;
  }

  /** A project viewed (a version, or the line's head), not a workspace: nothing here is saved. */
  get readOnly(): boolean {
    return this.view !== undefined;
  }

  get revision(): Revision | undefined {
    return this.#revision;
  }

  get configuration(): ProjectConfiguration | undefined {
    return this.#configuration;
  }

  /** Reads the workspace's current revision: every file, as saved, and its dependencies. Local edits are dropped. */
  async load(): Promise<void> {
    let files: PureFile[];
    if (this.view?.version !== undefined) {
      // a released version: its files and configuration as released
      files = await this.#client.versionPure(this.project, this.view.version);
      this.#revision = undefined;
      this.#configuration = await this.#client.versionConfiguration(this.project, this.view.version);
    } else {
      const where = this.view ? { project: this.project } : { project: this.project, workspace: this.workspace };
      const revision = await this.#client.revision(where);
      files = await this.#client.pure({ ...where, revision: revision.id });
      this.#revision = revision;
      this.#configuration = await this.#client.configuration({ ...where, revision: revision.id });
    }
    this.#saved = new Map(files.map((f) => [f.path, f.pureCode]));
    this.#files = new Map(files.map((f) => [f.path, { key: f.path, savedPath: f.path, text: f.pureCode }]));
    await this.#loadDependencies();
  }

  /** The dependencies' files, as Depot resolves them (the same closure the SDLC's gates compile with). */
  async #loadDependencies(): Promise<void> {
    this.dependencies = '';
    this.dependencyProblem = undefined;
    const declared = this.#configuration?.projectDependencies ?? [];
    if (declared.length === 0 || !this.#depot) return;
    try {
      const versions = await this.#depot.dependencyFiles(declared.map((d) => {
        const [groupId = '', artifactId = ''] = d.projectId.split(':');
        return { groupId, artifactId, versionId: d.versionId };
      }));
      this.dependencies = versions.flatMap((v) => v.files.map((f) => `###Pure\n${f.pureCode}`)).join('\n');
    } catch (e) {
      this.dependencyProblem = `the dependencies could not be read: ${e instanceof Error ? e.message : String(e)}`;
    }
  }

  files(): OpenFile[] {
    return [...this.#files.values()].sort((a, b) => (a.savedPath ?? a.key).localeCompare(b.savedPath ?? b.key));
  }

  file(key: string): OpenFile | undefined {
    return this.#files.get(key);
  }

  /** A new file with `text`; its key. */
  add(text: string): string {
    const key = `new:${this.#next++}`;
    this.#files.set(key, { key, savedPath: undefined, text });
    return key;
  }

  edit(key: string, text: string): void {
    const f = this.#files.get(key);
    if (!f) throw new Error(`no file ${key} in the workspace`);
    f.text = text;
  }

  remove(key: string): void {
    this.#files.delete(key);
  }

  /** Whether a file differs from what is saved (new, edited); a removed file is in `removed()`. */
  isChanged(key: string): boolean {
    const f = this.#files.get(key);
    if (!f) return false;
    return f.savedPath === undefined || this.#saved.get(f.savedPath) !== f.text;
  }

  /**
   * A change discarded (upstream's per-change discard): a saved element back to its saved text (a rename undone too),
   * a removed one restored, a new one dropped. Answers the file's key, or undefined when the file is gone.
   */
  discard(key: string): string | undefined {
    const f = this.#files.get(key);
    if (!f) return this.restore(key);
    if (f.savedPath === undefined) {
      this.#files.delete(key);
      return undefined;
    }
    f.text = this.#saved.get(f.savedPath) ?? f.text;
    return key;
  }

  /** A removed element back as it is saved (a delete undone before the save); its key. */
  restore(path: string): string {
    const text = this.#saved.get(path);
    if (text === undefined) throw new Error(`${path} is not saved in this workspace: there is nothing to restore`);
    if (!this.removed().includes(path)) throw new Error(`${path} is not removed`);
    this.#files.set(path, { key: path, savedPath: path, text });
    return path;
  }

  /** An element's text at the revision (what a change is compared with); undefined when it is not saved. */
  savedText(path: string): string | undefined {
    return this.#saved.get(path);
  }

  removed(): string[] {
    const kept = new Set([...this.#files.values()].map((f) => f.savedPath));
    return [...this.#saved.keys()].filter((p) => !kept.has(p)).sort();
  }

  hasChanges(): boolean {
    return this.removed().length > 0 || [...this.#files.keys()].some((k) => this.isChanged(k));
  }

  /**
   * What a save would send: each changed file read by the compiler for the element it is for. A file
   * whose text cannot be read, or holds no element or more than one, is a problem, and nothing is sent.
   */
  async pending(): Promise<Pending> {
    const changes: PureChange[] = [];
    const problems: Problem[] = [];
    for (const path of this.removed()) changes.push({ type: 'DELETE', path });
    for (const f of this.files()) {
      if (!this.isChanged(f.key)) continue;
      let path: string;
      try {
        const elements = await this.#compiler.elements(f.text);
        if (elements.length !== 1) {
          problems.push({ key: f.key, message: elements.length === 0 ? 'No element found' : `Expected one element, found ${elements.length}` });
          continue;
        }
        path = elements[0]!.path;
      } catch (e) {
        problems.push({ key: f.key, ...located(e instanceof Error ? e.message : String(e), 0) });
        continue;
      }
      if (f.savedPath === undefined) changes.push({ type: 'CREATE', path, pureCode: f.text });
      else if (f.savedPath === path) changes.push({ type: 'MODIFY', path, pureCode: f.text });
      else changes.push({ type: 'DELETE', path: f.savedPath }, { type: 'CREATE', path, pureCode: f.text });
    }
    return { changes, problems };
  }

  /**
   * Saves every change as one revision, with upstream's lock (the revision the editor started from),
   * then reads the workspace again. Answers the problems that stopped it (none when it saved).
   */
  async save(message: string): Promise<Problem[]> {
    if (this.readOnly) throw new Error(`${this.project} is being viewed: nothing is saved from here`);
    const { changes, problems } = await this.pending();
    if (problems.length > 0) return problems;
    if (changes.length === 0) return [];
    await this.#client.performPureChanges(this.project, this.workspace, {
      message, revisionId: this.#revision?.id ?? null, changes,
    });
    await this.load();
    return [];
  }

  /**
   * The model as the compiler sees it -- the dependencies, then every file, each in its own section --
   * and where each file starts in it (its first line, 1-based, after the section line added before it).
   */
  model(): { text: string; starts: { key: string; line: number; lines: number }[] } {
    let text = this.dependencies === '' ? '' : `${this.dependencies}\n`;
    let line = text === '' ? 1 : text.split('\n').length;
    const starts: { key: string; line: number; lines: number }[] = [];
    for (const f of this.files()) {
      // every file starts in the Pure section: a file before it may have ended in another
      text += '###Pure\n';
      line += 1;
      const lines = f.text.split('\n').length;
      starts.push({ key: f.key, line, lines });
      text += `${f.text}\n`;
      line += lines;
    }
    return { text, starts };
  }

  /** Compiles everything, and attaches each error to the file (and line) it comes from when it can. */
  async compile(): Promise<Problem[]> {
    const { text, starts } = this.model();
    const errors = await this.#compiler.compile(text);
    const problems = errors.map((message) => this.#attach(message, starts));
    return this.dependencyProblem ? [{ message: this.dependencyProblem }, ...problems] : problems;
  }

  #attach(message: string, starts: { key: string; line: number; lines: number }[]): Problem {
    // "[line:column] ..." points into the model text
    const at = /^\[(\d+):(\d+)\]\s*/.exec(message);
    if (at) {
      const line = Number(at[1]);
      const column = Number(at[2]);
      const file = starts.find((s) => line >= s.line && line < s.line + s.lines);
      if (file) return { key: file.key, line: line - file.line + 1, column, message: message.slice(at[0].length) };
      return { message: message.slice(at[0].length) };
    }
    // "in function 'a::b'", "in class 'a::B'": the element it names
    const named = /\bin \w+ '([^']+)'/.exec(message);
    if (named) {
      const name = named[1]!;
      for (const f of this.files()) {
        const path = f.savedPath ?? '';
        if (path === name || path.startsWith(`${name}_`) || f.text.includes(name)) return { key: f.key, message };
      }
    }
    return { message };
  }
}

/** A parser message's "[line:column]" prefix as a location in a file of its own. */
function located(message: string, offset: number): { message: string; line?: number; column?: number } {
  const at = /^\[(\d+):(\d+)\]\s*/.exec(message);
  if (!at) return { message };
  return { message: message.slice(at[0].length), line: Number(at[1]) - offset, column: Number(at[2]) };
}
