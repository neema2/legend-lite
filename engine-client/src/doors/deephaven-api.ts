// DEEPHAVEN'S JS LIBRARY, LOADED BY ITS DOOR (C5-T, 2026-11-15: a vendor load is a door's, and the run side takes the
// loaded object, never a loader -- tools/deps/layer-alone.test.ts COMPUTED_DOORS). The library is the server's own,
// served at its version from the server it reads (never bundled: docs/DEEPHAVEN_PLAN_2026_10_22.md DH11), so its module
// is chosen at run time, by the server's address. The live reader (deephaven-live.ts LiveClient) follows results with it.

import type { DhModule } from '../deephaven-live.ts';

/** The library the Deephaven server at `serverUrl` serves, loaded: as the live reader reads it (`DhModule`), or as a
 *  caller that reads more of it names it (a test's console, `M`). */
export async function deephavenApi<M = DhModule>(serverUrl: string): Promise<M> {
  const url = `${serverUrl}/jsapi/dh-core.js`;
  return ((await import(/* @vite-ignore */ url)) as { default: M }).default;
}
