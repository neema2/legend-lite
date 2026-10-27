// The proxy a test's environment names, as Chromium's flags (pinned-chromium.mjs `hermetic` adds them to every launch):
// no side effects, so its own test reads it without a browser (proxy_args_test.mjs).

/**
 * BEHIND A PROXY (2026-10-25, a cloud session: its ~/.bazelrc passes HTTPS_PROXY and NO_PROXY to every test): Chromium
 * hands a proxied request's host to the proxy unresolved, so the resolver's rule (pinned-chromium.mjs HERMETIC_ARGS) never
 * met extensions.duckdb.org
 * -- //datacube:browser_guard_test met the proxy's certificate instead (ERR_CERT_AUTHORITY_INVALID), and a Chromium
 * that trusts that certificate downloaded the extension. The proxy the environment names is given as Chromium's own
 * flag, each scheme to its own as the environment has it, with that host (and NO_PROXY's) bypassing it: so it is
 * resolved here, and refused; every other host still goes through the proxy. No proxy named: no flag.
 */
export function proxyArgs(env = process.env) {
  // the variables named as Chromium names them itself (its Linux proxy settings, proxy_config_service_linux.cc): each
  // lowercase first, then uppercase -- a lowercase one set empty is none, the uppercase not read (base/environment.cc
  // GetVar finds it set); all_proxy, when named, the one proxy for every scheme, else https_proxy and http_proxy each
  // for its own (the timeseries review's G6, docs/TIMESERIES_REVIEW_2026_10_25.md, 2026-10-25: ALL_PROXY lost to
  // HTTPS_PROXY here). Unlike Chromium's own reading, which takes every one for an HTTP proxy, its scheme is kept.
  const read = (name) => {
    const v = env[name] !== undefined ? env[name] : env[name.toUpperCase()];
    return v === undefined || v === '' ? undefined : v;
  };
  const all = read('all_proxy');
  const https = read('https_proxy');
  const http = read('http_proxy');
  const server = all !== undefined ? proxyServer(all)
    : [...(https ? [`https=${proxyServer(https)}`] : []), ...(http ? [`http=${proxyServer(http)}`] : [])].join(';');
  if (!server) return [];
  const bypass = (read('no_proxy') ?? '').split(',').map((h) => h.trim()).filter(Boolean).flatMap(bypassed);
  return [`--proxy-server=${server}`, `--proxy-bypass-list=${['extensions.duckdb.org', ...bypass].join(';')}`];
}

/**
 * A proxy as Chromium's flag takes it: its scheme, host and port. Its credentials and any path are left out -- the flag
 * refuses a URL that has them, and every host then goes direct (G6); a proxy that wants credentials answers each request
 * 407, said by the request, as the flag cannot carry them.
 */
function proxyServer(value) {
  const v = value.trim();
  const at = v.indexOf('://');
  const scheme = at < 0 ? '' : v.slice(0, at + 3);
  // its host and port: after any credentials, before any path -- a name, or an IPv6 address in brackets
  const authority = (at < 0 ? v : v.slice(at + 3)).split('/')[0].replace(/^[^@]*@/, '');
  // a scheme the flag takes (http, https, socks4, socks5, socks): another (socks5h, ftp) it drops, every host then going
  // direct (the second audit's G notes)
  if ((scheme !== '' && !/^(?:https?|socks[45]?):\/\/$/i.test(scheme)) || !/^(?:\[[\da-f:.]+\]|[^:@\s[\]]+)(?::\d+)?$/i.test(authority)) {
    throw new Error(`the proxy ${JSON.stringify(value)} is no proxy URL Chromium's --proxy-server takes`);
  }
  return `${scheme}${authority}`;
}

/**
 * A NO_PROXY entry as Chromium's bypass list says it: a domain and every host under it (`example.com`: it and
 * `*.example.com`; `.example.com`: the hosts under it), an address or a range of them, or a pattern (`*`, `*.x`), as it
 * is (G6: an entry matched its host alone).
 */
function bypassed(entry) {
  if (entry.startsWith('*')) return [entry];
  if (entry.startsWith('.')) return [`*${entry}`];
  // an IPv4 address or range, an IPv6 one (bracketed or not), or Chromium's own <local>
  if (/^\d+(?:\.\d+){3}(?:\/\d+)?$/.test(entry) || /^\[?[\da-f]*:[\da-f:.]*\]?(?:\/\d+)?$/i.test(entry) || entry.startsWith('<')) return [entry];
  return [entry, `*.${entry}`];
}
