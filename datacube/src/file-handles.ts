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

/**
 * The file behind a kept handle. `click` says whether this call runs inside a user's click,
 * the only place the browser lets a page ASK for permission.
 *   file          read (permission was already granted, or the click granted it)
 *   needs-click   permission must be asked, and this call was not a click
 *   unavailable   refused, moved or deleted: ask for the file instead
 */
export async function readHandle(
  handle: FileHandle,
  click: boolean,
): Promise<{ state: 'file'; file: File } | { state: 'needs-click' } | { state: 'unavailable'; why: string }> {
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
 * Kept handles in the cubes' own browser database: by saved page id and grid (`page#grid`), or -- saved before a page
 * held several grids -- by the page's id alone.
 */
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

  /** Every handle a saved page kept: its own, and each of its grids' (`page#grid`). */
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
