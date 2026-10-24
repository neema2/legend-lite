// A PART OF THE APP FETCHED WHEN IT IS FIRST WANTED (a chart's drawing, an example page, the Join window): through here,
// so that a part whose file is gone -- the site was updated while this page was open, and the old build's files went
// with it -- says so in words a person can act on, never the browser's "Failed to fetch dynamically imported module:
// .../chunk-OHVH3L56.js" (the user, 2026-10-23: "a cryptic error on the open dialog, had to refresh browser").

/** What a part that could not be fetched says: the page is older than the site, and a reload brings the new one. */
export const UPDATED = 'DataCube was updated since this page was opened: reload the page to use the new version (your saved pages are kept).';

/** Whether `e` is a browser's refusal to fetch a module the page asked for (Chrome's, Firefox's and Safari's words). */
export function partGone(e: unknown): boolean {
  return e instanceof TypeError
    && /Failed to fetch dynamically imported module|error loading dynamically imported module|Importing a module script failed/i.test(e.message);
}

/** `load` (a dynamic import), its file gone said as UPDATED; any other failure as it is. */
export async function lazily<T>(load: () => Promise<T>): Promise<T> {
  try {
    return await load();
  } catch (e) {
    if (partGone(e)) throw new Error(UPDATED, { cause: e });
    throw e;
  }
}
