// DuckDB's worker for a tab, printing no link's query string (2026-11-16, the TypeScript hunt's T-G). DuckDB-WASM's
// runtime prints a remote file's URL as it is, query string and all, on paths a presigned link takes as a matter of
// course -- "HEAD request failed: <url>" (a link presigned for GET refuses a HEAD), "Range request for <url> did not
// return a partial response", "fall back to full HTTP read for: <url>" -- so a presigned link's token reached the
// browser's console (read in the library's runtime_browser.ts, @duckdb/duckdb-wasm 1.33.1-dev57.0). Here every console
// call in the worker is handed its text with each URL's query string left out, and then DuckDB's own worker runs (a
// classic script, loaded beside this one in the page's vendor folder).
'use strict';
(() => {
  /** `text` with each URL's query string left out: its address kept, so what failed is still named, and a mark of the
   *  text's own after it (a comma, a full stop) kept too. */
  const unsigned = (text) => text.replace(/\b([a-z][a-z0-9+.-]*:\/\/[^\s?#'"`]*)\?[^\s#'"`]*?(?=[,.;:)]?(?:[\s'"`]|$))/gi,
    '$1?[query string not shown]');
  const shown = (value) => (typeof value === 'string' ? unsigned(value)
    : value instanceof Error ? Object.assign(new Error(unsigned(String(value.message))), { name: value.name })
      : value);
  for (const level of ['log', 'info', 'warn', 'error', 'debug', 'trace']) {
    const print = console[level].bind(console);
    console[level] = (...args) => print(...args.map(shown));
  }
})();
importScripts('./duckdb-browser-eh.worker.js');
