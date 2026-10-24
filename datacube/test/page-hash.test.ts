// THE PAGE HASH (src/page-hash.ts; docs/DATACUBE_LIVE_EDITING_2026_10_24.md §6, W17): SHA-256 over the canonical JSON,
// the digest held to the platform's (Web Crypto) on texts of every block-boundary length, the canonical form to its
// rules (keys sorted, compact, numbers their own digits), and a page's hash the same whichever order its keys came in.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { canonicalJson, pageHash, sha256Hex } from '../src/page-hash.ts';
import { readPage, pageToJson } from '../src/page-document.ts';
import { corpus } from './fixtures/pages/make.ts';

const platform = async (text: string): Promise<string> => {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(text));
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, '0')).join('');
};

describe('the page hash', () => {
  it("is the platform's SHA-256, at every length around the block boundaries, of UTF-8", async () => {
    assert.equal(sha256Hex(''), 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855');
    assert.equal(sha256Hex('abc'), 'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad');
    for (const n of [0, 1, 54, 55, 56, 57, 63, 64, 65, 119, 120, 128, 1000]) {
      const text = 'é'.repeat(n % 7) + 'x'.repeat(n);
      assert.equal(sha256Hex(text), await platform(text), `${n} characters`);
    }
  });

  it('canonical JSON: keys in code point order, compact, numbers as their digits', () => {
    assert.equal(canonicalJson('{"b": [1, 2.50, {"z": null, "a": true}], "a": "x", "A": 1e3}'), '{"A":1e3,"a":"x","b":[1,2.50,{"a":true,"z":null}]}');
  });

  it('a page hashes alike however its keys are ordered, and differently once it differs', async () => {
    const trades = (await corpus())['trades.page.json']!;
    const text = pageToJson(trades);
    const shuffled = JSON.parse(text) as Record<string, unknown>;
    const reversed = Object.fromEntries(Object.entries(shuffled).reverse());
    assert.equal(pageHash(trades), pageHash(readPage(reversed)));
    assert.notEqual(pageHash(trades), pageHash({ ...trades, name: 'another' }));
  });
});
