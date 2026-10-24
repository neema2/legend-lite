// FOLLOWING PYTHON'S PAGE (src/app/live-follow.ts, src/services/python-host.ts livePage;
// docs/DATACUBE_LIVE_EDITING_REDESIGN_2026_10_24.md, the middle version): a page move is followed by putting the screen
// at the keeper's page, read whole; a frame's move alone by the grids over it. The keeper's page is read with its exact
// numbers, a write's answer carries the keeper's page when it made another, a refusal is told from an answer that never
// came, and every ask says who the page open here is.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { followBy } from '../src/app/live-follow.ts';
import { LiveRefused } from '../src/app/services.ts';
import { pageToJson } from '../src/page-document.ts';
import { askedVersions, livePage } from '../src/services/python-host.ts';
import { corpus } from './fixtures/pages/make.ts';

const trades = (await corpus())['trades.page.json']!;

describe('how a move of the page is followed', () => {
  it('the page moved: the screen put at the keeper\'s page (a frame that moved with it read again after, W16); by its frames when the page did not move', () => {
    assert.equal(followBy({ version: 5, frames: { trades: 3 } }, 4), 'page');
    assert.equal(followBy({ version: 5, frames: { trades: 4 } }, 4), 'page', 'a frame moved with the page: the page, then the frame');
    assert.equal(followBy({ version: 4, frames: { trades: 4 } }, 4), 'frames');
    assert.equal(followBy({ frames: { trades: 4 } }, 4), 'frames', 'no page version said');
  });
});

describe("the keeper's page read as the page reads it", () => {
  /** A link answering every ask with `text` and `status`. */
  const link = (text: string, status = 200) => ({
    baseUrl: 'http://engine',
    fetch: (async () => new Response(text, { status })) as unknown as typeof fetch,
  });

  it('the page as it is now, with the ids it has held and its hash; the last write taken from the writer asking', async () => {
    const said = await livePage(link(`{"version": 7, "page": ${pageToJson(trades)}, "held": ["grid", "grid-9"], "hash": "h", "taken": 4}`), 'q3').read('w1');
    assert.equal(said.version, 7);
    assert.equal(said.page.name, trades.name);
    assert.deepEqual(said.held, ['grid', 'grid-9'], 'the ids the keeper holds, read with the page (B12)');
    assert.equal(said.hash, 'h', "the keeper's hash, read with the page (W17)");
    assert.equal(said.taken, 4);
    // asked by a writer, an answer that says not which of its writes the keeper took is no keeper's: refused
    await assert.rejects(livePage(link(`{"version": 7, "page": ${pageToJson(trades)}, "held": [], "hash": "h"}`), 'q3').read('w1'),
      /said not which of this page's writes it took/);
    assert.equal((await livePage(link(`{"version": 7, "page": ${pageToJson(trades)}, "held": [], "hash": "h"}`), 'q3').read()).taken, undefined,
      'asked by no writer: none said');
    // a page said with no hash is no page the keeper serves either
    await assert.rejects(livePage(link(`{"version": 7, "page": ${pageToJson(trades)}, "held": []}`), 'q3').read(), /no hash of the page/);
    // a page said with no ids held is no page the keeper serves: refused, never taken as holding none
    await assert.rejects(livePage(link(`{"version": 7, "page": ${pageToJson(trades)}}`), 'q3').read(), /no ids the page has held/);
  });

  it("a write's answer: at its version, the version and hash alone; made another page of, the keeper's page with it", async () => {
    const write = { edits: [], writer: 'w1', seq: 1 };
    assert.deepEqual(await livePage(link('{"version": 3, "hash": "h"}'), 'q3').report(2, write), { version: 3, hash: 'h' });
    const combined = await livePage(link(`{"version": 4, "hash": "h", "page": ${pageToJson(trades)}, "held": ["grid"]}`), 'q3').report(2, write);
    assert.equal(combined.page?.name, trades.name);
    assert.deepEqual(combined.held, ['grid']);
  });

  it("a refusal is the keeper's 409, in its words, or any failure the engine answers; only a proxy giving up (502, 503, 504) is a lost answer (the middle version's audit's 3)", async () => {
    const write = { edits: [], writer: 'w1', seq: 1 };
    await assert.rejects(livePage(link('grid-2 is no longer on the page', 409), 'q3').report(2, write),
      (e: unknown) => e instanceof LiveRefused && /grid-2 is no longer on the page/.test(e.message));
    // the engine's own failure, a page it no longer serves: answered, so refused, said -- never written again for ever
    for (const status of [500, 404, 400]) {
      await assert.rejects(livePage(link('KeyError: x', status), 'q3').report(2, write),
        (e: unknown) => e instanceof LiveRefused && new RegExp(`the engine answered ${status}: KeyError`).test(e.message), `${status}`);
    }
    for (const status of [502, 503, 504]) {
      await assert.rejects(livePage(link('down', status), 'q3').report(2, write),
        (e: unknown) => !(e instanceof LiveRefused) && new RegExp(`${status}`).test(String(e)), `${status}`);
    }
  });
});

describe('a page open on Python\'s says who it is and the version it shows (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md Z17)', () => {
  // Python keeps the copies the version it shows reads, and every later version's, until it says it moved past them:
  // the keeper's known limit 6, closed
  const asking = () => {
    const urls: string[] = [];
    const bodies: string[] = [];
    const link = {
      baseUrl: 'http://engine',
      fetch: (async (url: string, init?: RequestInit) => {
        urls.push(url.replace('http://engine/', ''));
        if (typeof init?.body === 'string') bodies.push(init.body);
        return new Response(url.includes('version.json') ? '{"version": 4, "frames": {}}'
          : url.includes('&version=') ? '{"version": 4, "hash": "h"}'
          : `{"version": 4, "page": ${pageToJson(trades)}, "held": [], "hash": "h", "taken": 0}`);
      }) as unknown as typeof fetch,
    };
    return { link, urls, bodies };
  };

  it('as it asks the versions, reads and writes; asking with no version on screen, it says none; each write says its id', async () => {
    const { link, urls, bodies } = asking();
    let shown: number | undefined = 3;
    const follower = { id: 'f00d', shown: () => shown };
    await askedVersions(link, { page: 'q 3' }, follower);
    shown = undefined;
    await askedVersions(link, { page: 'q 3' }, follower);
    const page = livePage(link, 'q 3', 'f00d');
    await page.read('w1');
    await page.read('w1', 2);
    await page.report(3, { edits: [], writer: 'w1', seq: 2 });
    assert.deepEqual(urls, [
      'version.json?page=q%203&follower=f00d&at=3',
      'version.json?page=q%203&follower=f00d',
      'page.json?page=q%203&follower=f00d&writer=w1',
      'page.json?page=q%203&follower=f00d&writer=w1&fence=2',
      'page.json?page=q%203&version=3&follower=f00d',
    ]);
    assert.deepEqual(bodies, ['{"edits":[],"writer":"w1","seq":2}']);
  });
});
