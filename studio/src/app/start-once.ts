// A start made once and shared by every ask -- unless it fails: a failed start is forgotten, so the next ask starts
// again rather than holding the failure for the page's life (2026-11-15, the TypeScript hunt's T-B: Studio's DuckDB in
// the tab, once refused -- a download that failed, a worker that did not load -- stayed refused until a reload).

/** `make`, begun at the first ask and shared by every ask after it; begun again at the next ask after it fails. */
export function startOnce<T>(make: () => Promise<T>): () => Promise<T> {
  let started: Promise<T> | undefined;
  return () => (started ??= make().catch((e: unknown) => {
    started = undefined;
    throw e;
  }));
}
