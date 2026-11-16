// THE DOOR TO A WAREHOUSE (the C5-T last round's T1, 2026-11-15): the warehouse's driver (warehouse.ts, the run side's)
// and its one binding (vendor/warehouse/), loaded once a page signs in; a session signed in; a table opened live. An app
// reaches the driver here alone, fetching this module when it first signs in (datacube/src/services/browser.ts).

import { BINDING_OPTIONS, connect, listObjects, signInWithKey, WarehouseBinding, WarehouseEngine, type SqlApiModule } from '../warehouse.ts';
import type { CatalogObject, LiveWarehouse, WarehouseSession } from '../warehouse-session.ts';

/**
 * The binding's module, from the directory holding its `classes.wasm` and TeaVM's runtime (`<vendor>warehouse/`, the
 * deployment's configured vendor directory): the runtime imported from there, the binding made of the module it loads.
 * A door's wiring: the run side takes the loaded binding and never loads a module (2026-11-15, the C5-T fold-in: the
 * run layers hold no computed specifier, //tools/deps:layers_alone; until then this load was warehouse.ts's, one of
 * the run layer's two named vendor loads). Nothing a caller hands in chooses the module (the C5-T last round's T3).
 */
export async function loadWarehouseBinding(base: string): Promise<WarehouseBinding> {
  const dir = base.endsWith('/') ? base : `${base}/`;
  const runtimeUrl = `${dir}wasm-gc-module-runtime.js`;
  const runtime = (await import(/* @vite-ignore */ runtimeUrl)) as {
    load(src: string, options?: unknown): Promise<SqlApiModule>;
  };
  return new WarehouseBinding(await runtime.load(`${dir}classes.wasm`, BINDING_OPTIONS));
}

/** A warehouse as an app signs in to it and reads it. */
export interface Warehouse {
  connect(url: string, user: string, password: string): Promise<{ readonly session: WarehouseSession; readonly objects: CatalogObject[] }>;
  signInWithKey(url: string, key: string): Promise<{ readonly session: WarehouseSession; readonly objects: CatalogObject[] }>;
  live(session: WarehouseSession, catalog: string, databaseType: string): LiveWarehouse;
}

/** The warehouse's driver over the binding at `base` (its folder's URL). */
export async function warehouseAt(base: string): Promise<Warehouse> {
  const binding = await loadWarehouseBinding(base);
  return {
    connect: (url, user, password) => connect(binding, url, user, password),
    signInWithKey: async (url, key) => {
      const session = await signInWithKey(binding, url, key);
      return { session, objects: await listObjects(binding, session) };
    },
    live: (session, catalog, databaseType) => new WarehouseEngine(binding, session, catalog, databaseType),
  };
}
