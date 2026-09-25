// The one place a demo server turns a request URL into a file on disk.
//
// Every server here (serve.mjs and the browser harnesses) used to do it
// with `path.normalize(url.pathname)` and a `..`-stripping regex. On
// Windows that is a different function: `normalize('/')` is `'\\'`, so
// a `=== '/'` check never matched, and the regex only knew `/`. A URL
// path is resolved as a URL -- against the root's own `file:` URL -- and
// only the final URL becomes a platform path, by Node's converter.

import { sep } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

/**
 * The file under `root` (an absolute directory) that `requestUrl` --
 * `req.url`, a path and query -- names, or undefined when it names
 * none: it climbs out of `root`, or it encodes a separator (`%2F`,
 * `%5C`) that would split into a different path. `/` is `index`, a
 * `/`-separated path under root.
 */
export function servedPath(root: string, requestUrl: string | undefined,
  index = 'demo/index.html'): string | undefined {
  // URL parsing collapses `.`, `..` and their %-encodings, and drops the query.
  const { pathname } = new URL(requestUrl ?? '/', 'http://x'); // portable: resolved as a URL below
  const base = pathToFileURL(root.endsWith(sep) ? root : `${root}${sep}`);
  const file = new URL(pathname === '/' ? index : `.${pathname}`, base);
  if (!file.href.startsWith(base.href)) return undefined;
  if (/%2f|%5c/i.test(file.href)) return undefined;
  try {
    return fileURLToPath(file);
  } catch {
    return undefined;
  }
}
