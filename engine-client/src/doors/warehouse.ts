// THE DOOR TO A WAREHOUSE (the C5-T last round's T1, 2026-11-15): the warehouse's driver (warehouse.ts, the run side's)
// and its one binding (vendor/warehouse/), loaded once a page signs in; a session signed in; a table opened live. An app
// reaches the driver here alone, fetching this module when it first signs in (datacube/src/services/browser.ts).

import { connect, listObjects, loadWarehouseBinding, signInWithKey, WarehouseEngine } from '../warehouse.ts';
import type { CatalogObject, LiveWarehouse, WarehouseSession } from '../warehouse-session.ts';

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
