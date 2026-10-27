// A saved cube reopens its file from WHERE THE USER PICKED IT (the user, 2026-09-28).
//
// A page cannot open a file by its path. The File System Access API (Chrome and Edge) gives a
// HANDLE instead: a reference to the file on disk -- never its bytes -- that the browser lets
// the page keep in IndexedDB and use again after a reload, once the user allows it. So:
//   - a handle is kept per saved cube, in THIS browser only: never in the cube's document, an
//     export or anything shared;
//   - reopening reads the file as it is NOW at that place (its fingerprint says if it changed);
//   - after a reload the browser usually asks again, and only a click may ask: one click, no
//     file dialog; a browser that remembers the permission needs none;
//   - a browser without the API, a moved or deleted file, or a refusal falls back to asking
//     for the file.

/** The parts of a `FileSystemFileHandle` used here. */
export interface FileHandle {
  readonly kind: 'file';
  readonly name: string;
  getFile(): Promise<File>;
  queryPermission?(descriptor: { mode: 'read' }): Promise<PermissionState>;
  requestPermission?(descriptor: { mode: 'read' }): Promise<PermissionState>;
}

interface PickerWindow {
  showOpenFilePicker?(options: {
    types?: { description: string; accept: Record<string, string[]> }[];
    excludeAcceptAllOption?: boolean;
    multiple?: boolean;
  }): Promise<FileHandle[]>;
}

/** Whether `x` is a file's handle, as a drop gives one (DataTransferItem.getAsFileSystemHandle: Chrome, Edge). */
export function isFileHandle(x: unknown): x is FileHandle {
  return typeof x === 'object' && x !== null && (x as FileHandle).kind === 'file' && typeof (x as FileHandle).getFile === 'function';
}

/** Whether this browser can pick a file with a handle to keep. */
export function canKeepHandles(win: unknown = globalThis): boolean {
  return typeof (win as PickerWindow).showOpenFilePicker === 'function';
}

/** The files DataCube reads, for the picker. */
const DATA_FILES = {
  description: 'Data files',
  accept: {
    'text/csv': ['.csv'],
    'application/json': ['.json', '.jsonl', '.ndjson'],
    'application/vnd.apache.parquet': ['.parquet'],
    // any of them gzipped (upload.ts fileParts unpacks it)
    'application/gzip': ['.gz'],
  },
};

/** Pick a data file and keep its handle; undefined when the user cancels. */
export async function pickDataFile(win: unknown = globalThis): Promise<{ file: File; handle: FileHandle } | undefined> {
  const picker = (win as PickerWindow).showOpenFilePicker;
  if (!picker) return undefined;
  try {
    const [handle] = await picker.call(win, { types: [DATA_FILES], multiple: false });
    if (!handle) return undefined;
    return { file: await handle.getFile(), handle };
  } catch (e) {
    // the user closing the picker is an AbortError, not a failure
    if (e instanceof Error && e.name === 'AbortError') return undefined;
    throw e;
  }
}

/** What reading a kept handle gave (`readHandle`). */
export type HandleRead = { state: 'file'; file: File } | { state: 'needs-click' } | { state: 'unavailable'; why: string };

/**
 * Files for a text box (text-files.ts; docs/DATACUBE_UI_REVIEW_2026_10_24.md §8), each with the handle to keep where the
 * browser gives one (Chrome, Edge), else picked through a file input: the person's choice, none when they cancel.
 */
export async function pickTextFiles(doc: Document, extensions: readonly string[], multiple = true):
  Promise<{ file: File; handle?: FileHandle }[]> {
  const win = doc.defaultView;
  const picker = (win as PickerWindow | null)?.showOpenFilePicker;
  if (picker) {
    try {
      const handles = await picker.call(win, { types: [{ description: 'Text files', accept: { 'text/plain': [...extensions] } }], multiple });
      return Promise.all(handles.map(async (handle) => ({ file: await handle.getFile(), handle })));
    } catch (e) {
      if (e instanceof Error && e.name === 'AbortError') return [];
      throw e;
    }
  }
  // no handles to give: a file input, in the page while it asks (where a test, too, finds it)
  const input = doc.createElement('input');
  input.type = 'file';
  input.className = 'dc-file-input';
  input.accept = extensions.join(',');
  input.multiple = multiple;
  input.hidden = true;
  doc.body.append(input);
  return new Promise((resolve) => {
    const done = (): void => {
      input.remove();
      resolve([...(input.files ?? [])].map((file) => ({ file })));
    };
    input.addEventListener('change', done, { once: true });
    input.addEventListener('cancel', done, { once: true });
    input.click();
  });
}

/**
 * The file behind a kept handle. `click` says whether this call runs inside a user's click,
 * the only place the browser lets a page ASK for permission.
 *   file          read (permission was already granted, or the click granted it)
 *   needs-click   permission must be asked, and this call was not a click
 *   unavailable   refused, moved or deleted: ask for the file instead
 */
export async function readHandle(handle: FileHandle, click: boolean): Promise<HandleRead> {
  try {
    let permission = (await handle.queryPermission?.({ mode: 'read' })) ?? 'granted';
    if (permission === 'prompt') {
      if (!click) return { state: 'needs-click' };
      permission = (await handle.requestPermission?.({ mode: 'read' })) ?? 'denied';
    }
    if (permission !== 'granted') return { state: 'unavailable', why: 'the browser was not allowed to read it' };
    return { state: 'file', file: await handle.getFile() };
  } catch (e) {
    // NotFoundError: moved or deleted since
    return { state: 'unavailable', why: e instanceof Error ? e.message : String(e) };
  }
}

/**
 * Where a saved page keeps a file's handle: by the page and the file's contents, its hash -- so every grid reading that
 * file finds it, one of its own or a join's leaf (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §18.3: what a grid reads is
 * its leaves, never one file of its own).
 */
export function handleKey(pageId: string, sha256: string): string {
  return `${pageId}#${sha256}`;
}

/** Kept handles in the cubes' own browser database, by `handleKey`. */
export class FileHandles {
  static readonly STORE = 'handles';
  readonly #db: Promise<IDBDatabase>;

  constructor(db: Promise<IDBDatabase>) {
    this.#db = db;
  }

  async get(cubeId: string): Promise<FileHandle | undefined> {
    const found = await this.#request<unknown>('readonly', (s) => s.get(cubeId));
    return found === undefined ? undefined : found as FileHandle;
  }

  async put(cubeId: string, handle: FileHandle): Promise<void> {
    await this.#request('readwrite', (s) => s.put(handle, cubeId));
  }

  async remove(cubeId: string): Promise<void> {
    await this.#request('readwrite', (s) => s.delete(cubeId));
  }

  /** Every handle a saved page kept (`page#…`), and one an older writer kept under the page's id alone. */
  async removePage(pageId: string): Promise<void> {
    await this.remove(pageId);
    await this.#request('readwrite', (s) => s.delete(IDBKeyRange.bound(`${pageId}#`, `${pageId}#\uffff`)));
  }

  async #request<T>(mode: IDBTransactionMode, go: (s: IDBObjectStore) => IDBRequest): Promise<T> {
    const db = await this.#db;
    return new Promise<T>((resolve, reject) => {
      const tx = db.transaction(FileHandles.STORE, mode);
      const request = go(tx.objectStore(FileHandles.STORE));
      let result: T;
      request.onsuccess = () => { result = request.result as T; };
      tx.oncomplete = () => resolve(result);
      tx.onerror = () => reject(tx.error ?? new Error('the browser refused the write'));
      tx.onabort = () => reject(tx.error ?? new Error('the browser aborted the write'));
    });
  }
}
