// THE SAVED PAGES IN THIS BROWSER: the tab's (services/browser.ts) and any page of DataCube's opened on its own over a
// server of its own -- the trader screens on Deephaven (demo/deephaven/market/traders.ts) -- the one store and its
// rules, kept by this browser for this origin.

import type { QueriesModule } from '../../../query-store/src/index.ts';
import {
  BrowserRecords, CubeStoreClient, LOCAL_CUBES_API, MemoryRecords, openCubeDatabase, persistStorage, wasmCubeServer, type CubeRecords,
} from '../cube-store.ts';
import { FileHandles } from '../file-handles.ts';
import type { PageService } from '../app/services.ts';

/** SAVED PAGES, in this browser: IndexedDB when the browser gives it, else memory (this visit only, and said so), by
 *  legend-lite's own rules (core's SavedCubes, compiled to WebAssembly; the program's B6). One database holds the pages
 *  and their files' handles. */
export async function browserPages(load: () => Promise<QueriesModule>): Promise<PageService> {
  let records: CubeRecords;
  let handles: FileHandles | undefined;
  let memoryOnly = false;
  try {
    const database = openCubeDatabase();
    await database;
    records = new BrowserRecords(database);
    handles = new FileHandles(database);
  } catch {
    records = new MemoryRecords();
    memoryOnly = true;
  }
  const user = 'this browser';
  return {
    store: new CubeStoreClient(LOCAL_CUBES_API, wasmCubeServer({ load, records, user }).fetch, user),
    where: memoryOnly ? 'for this visit only (this browser refused storage)' : 'in this browser',
    memoryOnly,
    ...(handles ? { handles } : {}),
    persist: persistStorage,
  };
}
