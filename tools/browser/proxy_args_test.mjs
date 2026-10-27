// The proxy a test's environment names, as Chromium's flags (proxy-args.mjs; the timeseries review's G6,
// docs/TIMESERIES_REVIEW_2026_10_25.md): read as Chromium reads the same variables itself, every flag one Chromium takes.
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { proxyArgs } from './proxy-args.mjs';

const server = (env) => proxyArgs(env)[0];
const bypass = (env) => proxyArgs(env)[1];

describe('the proxy the environment names, as Chromium flags it', () => {
  it('no proxy named: no flag', () => {
    assert.deepEqual(proxyArgs({}), []);
    assert.deepEqual(proxyArgs({ NO_PROXY: 'localhost' }), []);
  });

  it('each scheme to its own; all_proxy, named, the one for every scheme (Chromium reads it first)', () => {
    assert.equal(server({ HTTPS_PROXY: 'http://p:3128', HTTP_PROXY: 'http://q:3128' }), '--proxy-server=https=http://p:3128;http=http://q:3128');
    assert.equal(server({ HTTPS_PROXY: 'http://p:3128', ALL_PROXY: 'socks5://s:1080' }), '--proxy-server=socks5://s:1080');
    // the lowercase first, as Chromium reads them -- one set empty is none, the uppercase not read (Chromium's GetVar)
    assert.equal(server({ https_proxy: 'http://low:1', HTTPS_PROXY: 'http://up:2' }), '--proxy-server=https=http://low:1');
    assert.deepEqual(proxyArgs({ https_proxy: '', HTTPS_PROXY: 'http://up:2' }), []);
  });

  it('a proxy URL without its credentials or its path: the flag refuses them, and every host would go direct', () => {
    assert.equal(server({ HTTPS_PROXY: 'http://rita:s3cret@p:3128/' }), '--proxy-server=https=http://p:3128');
    assert.equal(server({ HTTPS_PROXY: 'http://p:3128/some/path' }), '--proxy-server=https=http://p:3128');
    assert.equal(server({ HTTPS_PROXY: 'p:3128' }), '--proxy-server=https=p:3128');
    assert.equal(server({ HTTPS_PROXY: 'http://[::1]:3128' }), '--proxy-server=https=http://[::1]:3128');
    assert.throws(() => proxyArgs({ HTTPS_PROXY: 'http://' }), /is no proxy URL Chromium's --proxy-server takes/);
    assert.throws(() => proxyArgs({ HTTPS_PROXY: 'http://p:port' }), /is no proxy URL/);
    // a scheme the flag drops, every host then direct: refused
    assert.throws(() => proxyArgs({ ALL_PROXY: 'socks5h://s:1080' }), /is no proxy URL/);
    assert.throws(() => proxyArgs({ HTTPS_PROXY: 'ftp://p:21' }), /is no proxy URL/);
  });

  it("NO_PROXY's entries: a domain and the hosts under it; addresses, ranges and patterns as they are; the guarded host first", () => {
    const list = bypass({ HTTPS_PROXY: 'http://p:1', NO_PROXY: 'example.com, .corp.test,*.svc,127.0.0.1,10.0.0.0/8,::1,[fe80::1],fe80::/10,<local>,*' });
    assert.equal(list, '--proxy-bypass-list=extensions.duckdb.org;example.com;*.example.com;*.corp.test;*.svc;127.0.0.1;10.0.0.0/8;::1;[fe80::1];fe80::/10;<local>;*');
    assert.equal(bypass({ HTTPS_PROXY: 'http://p:1' }), '--proxy-bypass-list=extensions.duckdb.org');
  });
});
