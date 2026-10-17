// The headless host's web layer (src/headless/web.ts) held to its standards, case for case, by a conformant engine's
// own built-ins: every case below runs against Node's TextEncoder, TextDecoder, Event, EventTarget, DOMException,
// AbortController, AbortSignal, atob and btoa, and against ours, and the two must agree. Node's pass shows each case
// asserts the standard's behaviour; ours shows the layer has it. The UTF-8 cases are the Encoding standard's own
// (its decoder's error handling: one U+FFFD per maximal invalid subpart). Where ours refuses what Node does (another
// encoding, a streaming decode), the refusal is tested by itself.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import * as ours from '../src/headless/web.ts';
import { installUrl } from '../src/headless/url.ts';

interface Web {
  readonly TextEncoder: typeof globalThis.TextEncoder;
  readonly TextDecoder: typeof globalThis.TextDecoder;
  readonly Event: typeof globalThis.Event;
  readonly EventTarget: typeof globalThis.EventTarget;
  readonly DOMException: typeof globalThis.DOMException;
  readonly AbortController: typeof globalThis.AbortController;
  readonly AbortSignal: typeof globalThis.AbortSignal;
  readonly atob: typeof globalThis.atob;
  readonly btoa: typeof globalThis.btoa;
}

const NODE: Web = globalThis;
const OURS = ours as unknown as Web;
const BOTH: readonly (readonly [string, Web])[] = [['Node', NODE], ['ours', OURS]];

const bytes = (...b: number[]): Uint8Array => Uint8Array.from(b);
const units = (s: string): number[] => [...s].map((c) => c.codePointAt(0)!);
const tick = (ms = 0): Promise<void> => new Promise((done) => setTimeout(done, ms));

for (const [who, w] of BOTH) {
  describe(`${who}: UTF-8, by the Encoding standard`, () => {
    it('encodes every width, and a lone surrogate as U+FFFD', () => {
      const e = new w.TextEncoder();
      assert.equal(e.encoding, 'utf-8');
      assert.deepEqual([...e.encode('')], []);
      assert.deepEqual([...e.encode('aé€\u{1F600}')], [0x61, 0xc3, 0xa9, 0xe2, 0x82, 0xac, 0xf0, 0x9f, 0x98, 0x80]);
      assert.deepEqual([...e.encode('\ud800')], [0xef, 0xbf, 0xbd]);
      assert.deepEqual([...e.encode('x\udc00y')], [0x78, 0xef, 0xbf, 0xbd, 0x79]);
      assert.deepEqual([...e.encode('😀\ud83d')], [0xf0, 0x9f, 0x98, 0x80, 0xef, 0xbf, 0xbd]);
      assert.deepEqual([...e.encode(42 as unknown as string)], [0x34, 0x32]);
    });

    it('encodes into a buffer whole scalar values only, saying what it read and wrote', () => {
      const e = new w.TextEncoder();
      const cases: readonly (readonly [string, number, { read: number; written: number }, number[]])[] = [
        ['a\u20ac\u{1F600}', 4, { read: 2, written: 4 }, [0x61, 0xe2, 0x82, 0xac]],
        ['a\u20ac\u{1F600}', 8, { read: 4, written: 8 }, [0x61, 0xe2, 0x82, 0xac, 0xf0, 0x9f, 0x98, 0x80]],
        ['\u{1F600}', 3, { read: 0, written: 0 }, [0, 0, 0]],
        ['abc', 2, { read: 2, written: 2 }, [0x61, 0x62]],
        ['', 2, { read: 0, written: 0 }, [0, 0]],
      ];
      for (const [text, size, said, written] of cases) {
        const into = new Uint8Array(size);
        assert.deepEqual(e.encodeInto(text, into), said, `${JSON.stringify(text)} into ${size}`);
        assert.deepEqual([...into], written);
      }
    });

    it('decodes every width, from a buffer, a view or a slice of one', () => {
      const d = new w.TextDecoder();
      const all = bytes(0x61, 0xc3, 0xa9, 0xe2, 0x82, 0xac, 0xf0, 0x9f, 0x98, 0x80);
      assert.equal(d.decode(all), 'aé€\u{1F600}');
      assert.equal(d.decode(all.buffer), 'aé€\u{1F600}');
      assert.equal(d.decode(new DataView(all.buffer, 1, 2)), 'é');
      assert.equal(d.decode(all.subarray(3, 6)), '€');
      assert.equal(d.decode(), '');
    });

    it("replaces each maximal invalid subpart with one U+FFFD (the standard's decoder)", () => {
      const d = new w.TextDecoder();
      const cases: readonly (readonly [number[], number[]])[] = [
        [[0xed, 0xa0, 0x80], [0xfffd, 0xfffd, 0xfffd]], // a surrogate's encoding: ED takes 80-9F only
        [[0xc0, 0x80], [0xfffd, 0xfffd]], // overlong: C0 never starts a sequence
        [[0xe0, 0x80, 0x80], [0xfffd, 0xfffd, 0xfffd]], // overlong: E0 takes A0-BF only
        [[0xf0, 0x80, 0x80, 0x80], [0xfffd, 0xfffd, 0xfffd, 0xfffd]], // overlong: F0 takes 90-BF only
        [[0xf4, 0x90, 0x80, 0x80], [0xfffd, 0xfffd, 0xfffd, 0xfffd]], // past U+10FFFF: F4 takes 80-8F only
        [[0xf5, 0x80], [0xfffd, 0xfffd]],
        [[0xe2, 0x82], [0xfffd]], // truncated at the end: one error
        [[0xe2, 0x82, 0x41], [0xfffd, 0x41]], // cut short by an ASCII byte, which is read again
        [[0xf0, 0x9f, 0x98], [0xfffd]],
        [[0x80, 0xbf], [0xfffd, 0xfffd]], // lone continuations
        [[0xff, 0x61, 0xfe], [0xfffd, 0x61, 0xfffd]],
        [[0xc3], [0xfffd]],
      ];
      for (const [input, expected] of cases) {
        assert.deepEqual(units(d.decode(Uint8Array.from(input))), expected, input.map((b) => b.toString(16)).join(' '));
      }
    });

    it('strips a leading BOM unless told to keep it; fatal throws a TypeError', () => {
      assert.equal(new w.TextDecoder().decode(bytes(0xef, 0xbb, 0xbf, 0x61)), 'a');
      assert.equal(new w.TextDecoder('utf-8', { ignoreBOM: true }).decode(bytes(0xef, 0xbb, 0xbf, 0x61)), '﻿a');
      assert.equal(new w.TextDecoder().decode(bytes(0x61, 0xef, 0xbb, 0xbf)), 'a﻿', 'only a leading one');
      const fatal = new w.TextDecoder('utf-8', { fatal: true });
      assert.equal(fatal.fatal, true);
      assert.throws(() => fatal.decode(bytes(0x61, 0xff)), TypeError);
      assert.equal(fatal.decode(bytes(0x61)), 'a');
    });

    it("takes UTF-8's labels, trimmed and in any case", () => {
      for (const label of ['utf-8', 'UTF8', ' unicode-1-1-utf-8\n', 'x-unicode20utf8', 'unicode11utf8']) {
        assert.equal(new w.TextDecoder(label).encoding, 'utf-8', label);
      }
      assert.throws(() => new w.TextDecoder('no-such-encoding'), RangeError);
    });

    it('round-trips a long text in one decode', () => {
      const text = 'xé€\u{1F600}'.repeat(5000);
      assert.equal(new w.TextDecoder().decode(new w.TextEncoder().encode(text)), text);
    });
  });

  describe(`${who}: DOMException, Event and EventTarget, by the DOM standard`, () => {
    it('names a DOMException and gives its legacy code', () => {
      const e = new w.DOMException('stopped', 'AbortError');
      assert.ok(e instanceof Error);
      assert.equal(e.name, 'AbortError');
      assert.equal(e.message, 'stopped');
      assert.equal(e.code, 20);
      assert.equal(new w.DOMException('t', 'TimeoutError').code, 23);
      assert.equal(new w.DOMException('x', 'MadeUpError').code, 0);
      assert.equal(new w.DOMException().name, 'Error');
      assert.equal(new w.DOMException().message, '');
    });

    it('runs listeners in the order added, each once however often added (a capture flag makes another)', () => {
      const t = new w.EventTarget();
      const order: string[] = [];
      const a = (): void => { order.push('a'); };
      t.addEventListener('x', a);
      t.addEventListener('x', () => order.push('b'));
      t.addEventListener('x', a);
      t.addEventListener('x', { handleEvent: () => order.push('object') });
      t.dispatchEvent(new w.Event('x'));
      assert.deepEqual(order, ['a', 'b', 'object']);
    });

    it('a once listener runs once; a removed one not at all; a signal removes one', () => {
      const t = new w.EventTarget();
      let once = 0;
      let removed = 0;
      let signalled = 0;
      t.addEventListener('x', () => { once += 1; }, { once: true });
      const r = (): void => { removed += 1; };
      t.addEventListener('x', r);
      t.removeEventListener('x', r);
      const c = new w.AbortController();
      t.addEventListener('x', () => { signalled += 1; }, { signal: c.signal });
      t.dispatchEvent(new w.Event('x'));
      c.abort();
      t.dispatchEvent(new w.Event('x'));
      assert.deepEqual([once, removed, signalled], [1, 0, 1]);
      t.addEventListener('x', () => { signalled += 1; }, { signal: c.signal });
      t.dispatchEvent(new w.Event('x'));
      assert.equal(signalled, 1, 'an aborted signal adds nothing');
    });

    it('stopImmediatePropagation ends the dispatch; the target is set while it runs and kept after', () => {
      const t = new w.EventTarget();
      const seen: unknown[] = [];
      t.addEventListener('x', (e) => { seen.push(e.target === t, e.currentTarget === t, e.eventPhase); e.stopImmediatePropagation(); });
      t.addEventListener('x', () => seen.push('not reached'));
      const e = new w.Event('x');
      assert.equal(e.eventPhase, 0);
      t.dispatchEvent(e);
      assert.deepEqual(seen, [true, true, 2]);
      assert.equal(e.target, t);
      assert.equal(e.currentTarget, null);
      assert.equal(e.eventPhase, 0);
    });

    it('preventDefault cancels only a cancelable event', () => {
      const t = new w.EventTarget();
      t.addEventListener('x', (e) => e.preventDefault());
      const e = new w.Event('x', { cancelable: true });
      assert.equal(t.dispatchEvent(e), false);
      assert.equal(e.defaultPrevented, true);
      assert.equal(e.returnValue, false);
      assert.equal(t.dispatchEvent(new w.Event('x')), true);
      assert.deepEqual([e.type, e.bubbles, e.cancelable, e.isTrusted], ['x', false, true, false]);
    });

    it('an Event needs its type', () => {
      assert.throws(() => new (w.Event as unknown as new () => Event)(), TypeError);
    });
  });

  describe(`${who}: AbortController and AbortSignal, by the DOM standard`, () => {
    it('aborts once, with an AbortError unless given a reason; onabort runs in the order it was set', () => {
      const c = new w.AbortController();
      const order: string[] = [];
      c.signal.addEventListener('abort', () => order.push('first'));
      c.signal.onabort = () => order.push('onabort');
      c.signal.addEventListener('abort', () => order.push('last'));
      assert.equal(c.signal.aborted, false);
      assert.equal(c.signal.reason, undefined);
      c.abort();
      c.abort('again');
      assert.equal(c.signal.aborted, true);
      const reason: unknown = c.signal.reason;
      assert.ok((reason as object) instanceof w.DOMException);
      assert.equal((reason as DOMException).name, 'AbortError');
      assert.deepEqual(order, ['first', 'onabort', 'last'], 'one abort event, the handler in its place');
      assert.throws(() => c.signal.throwIfAborted(), (e: unknown) => e === c.signal.reason);
    });

    it('carries a reason given; onabort cleared runs no more', () => {
      const c = new w.AbortController();
      let ran = 0;
      c.signal.onabort = () => { ran += 1; };
      c.signal.onabort = null;
      c.abort({ why: 'superseded' });
      assert.deepEqual(c.signal.reason, { why: 'superseded' });
      assert.equal(ran, 0);
      assert.equal(c.signal.onabort, null);
    });

    it('abort(), any() and timeout() make signals as the standard says', async () => {
      assert.equal(w.AbortSignal.abort().aborted, true);
      assert.equal(w.AbortSignal.abort(7).reason, 7);
      const a = new w.AbortController();
      const b = new w.AbortController();
      const either = w.AbortSignal.any([a.signal, b.signal]);
      assert.equal(either.aborted, false);
      b.abort('b went');
      assert.equal(either.aborted, true);
      assert.equal(either.reason, 'b went');
      assert.equal(w.AbortSignal.any([w.AbortSignal.abort('early')]).reason, 'early');
      const t = w.AbortSignal.timeout(5);
      assert.equal(t.aborted, false);
      await tick(30);
      assert.equal(t.aborted, true);
      assert.equal((t.reason as DOMException).name, 'TimeoutError');
    });
  });

  describe(`${who}: atob and btoa, by HTML's forgiving-base64`, () => {
    it('encodes Latin-1 text, and refuses a character past it', () => {
      assert.equal(w.btoa(''), '');
      assert.equal(w.btoa('f'), 'Zg==');
      assert.equal(w.btoa('fo'), 'Zm8=');
      assert.equal(w.btoa('foobar'), 'Zm9vYmFy');
      assert.equal(w.btoa('ÿ\u0000'), '/wA=');
      assert.throws(() => w.btoa('Ā'), (e: unknown) => (e as DOMException).name === 'InvalidCharacterError');
    });

    it('decodes forgivingly: whitespace and missing padding taken, a bad string refused', () => {
      assert.equal(w.atob('Zm9vYmFy'), 'foobar');
      assert.equal(w.atob(' Zm 9v\nYg== '), 'foob');
      assert.equal(w.atob('Zm9vYg'), 'foob');
      assert.equal(w.atob('/wA='), 'ÿ\u0000');
      assert.equal(w.atob(''), '');
      for (const bad of ['Zm9vY', 'Zm9v!', 'Zg===', '=Zg=', 'Z']) {
        assert.throws(() => w.atob(bad), (e: unknown) => (e as DOMException).name === 'InvalidCharacterError', bad);
      }
    });
  });
}

describe('ours alone: where Node 22 is not the DOM standard, browsers are, and ours is', () => {
  // Node runs a target's listeners in the order added whatever their capture flag, and lets a passive listener cancel;
  // the DOM standard (and every browser DataCube runs in) runs capture listeners first at the target and ignores a
  // passive listener's preventDefault (https://dom.spec.whatwg.org/#concept-event-dispatch, "in passive listener flag")
  it('runs capture listeners first at the target, then the others, each in the order added', () => {
    const t = new ours.EventTarget();
    const order: string[] = [];
    const a = (): void => { order.push('a'); };
    t.addEventListener('x', a);
    t.addEventListener('x', () => order.push('b'));
    t.addEventListener('x', () => order.push('capture'), { capture: true });
    t.addEventListener('x', a, true);
    t.dispatchEvent(new ours.Event('x'));
    assert.deepEqual(order, ['capture', 'a', 'a', 'b']);
  });

  it("ignores a passive listener's preventDefault", () => {
    const t = new ours.EventTarget();
    t.addEventListener('x', (e) => e.preventDefault(), { passive: true });
    const e = new ours.Event('x', { cancelable: true });
    assert.equal(t.dispatchEvent(e), true);
    assert.equal(e.defaultPrevented, false);
  });
});

describe('ours alone: what this host refuses, and how it is installed', () => {
  it('knows UTF-8 alone: another encoding is refused, never read as UTF-8; a streaming decode is refused', () => {
    assert.throws(() => new ours.TextDecoder('windows-1252'), RangeError);
    assert.throws(() => new ours.TextDecoder('utf-16le'), RangeError);
    assert.throws(() => new ours.TextDecoder().decode(bytes(0x61), { stream: true }), TypeError);
  });

  it("reports a listener's exception and goes on to the next", () => {
    const reported: unknown[] = [];
    const g = globalThis as { reportError?: (e: unknown) => void };
    const before = g.reportError;
    g.reportError = (e) => reported.push(e);
    try {
      const t = new ours.EventTarget();
      let after = false;
      t.addEventListener('x', () => { throw new Error('boom'); });
      t.addEventListener('x', () => { after = true; });
      assert.equal(t.dispatchEvent(new ours.Event('x')), true);
      assert.equal(after, true);
      assert.equal((reported[0] as Error).message, 'boom');
    } finally {
      if (before) g.reportError = before;
      else delete g.reportError;
    }
  });

  it('refuses an event already being dispatched', () => {
    const t = new ours.EventTarget();
    const e = new ours.Event('x');
    let inner: unknown;
    t.addEventListener('x', () => {
      try { t.dispatchEvent(e); } catch (x) { inner = x; }
    });
    t.dispatchEvent(e);
    assert.equal((inner as DOMException).name, 'InvalidStateError');
  });

  it('installs what an engine lacks and keeps what it has', () => {
    const empty: Record<string, unknown> = { setTimeout, clearTimeout };
    ours.installWeb(empty);
    installUrl(empty);
    for (const name of ['TextEncoder', 'TextDecoder', 'DOMException', 'Event', 'EventTarget', 'AbortSignal', 'AbortController',
      'atob', 'btoa', 'queueMicrotask', 'performance', 'setInterval', 'clearInterval', 'URL', 'URLSearchParams']) {
      assert.notEqual(empty[name], undefined, name);
    }
    assert.equal(empty['TextEncoder'], ours.TextEncoder);
    const Url = empty['URL'] as typeof URL;
    assert.equal(new Url('a b?q=1#f', 'http://x/y/').href, new URL('a b?q=1#f', 'http://x/y/').href, "whatwg-url's URL");
    const before = { ...Object.fromEntries(['TextEncoder', 'Event', 'URL', 'atob'].map((n) => [n, (globalThis as Record<string, unknown>)[n]])) };
    ours.installWeb();
    installUrl();
    for (const [n, v] of Object.entries(before)) assert.equal((globalThis as Record<string, unknown>)[n], v, `Node's own ${n} kept`);
  });

  it('runs an interval as a chain of the engine\'s timeouts, until cleared', async () => {
    const g: Record<string, unknown> = { setTimeout, clearTimeout };
    ours.installWeb(g);
    let runs = 0;
    const setInterval = g['setInterval'] as (f: () => void, ms: number) => number;
    const clearInterval = g['clearInterval'] as (id: number) => void;
    const id = setInterval(() => {
      runs += 1;
      if (runs === 3) clearInterval(id);
    }, 1);
    // waited for, not slept on: past a fixed wait, a stalled loop expires the chain's first timeout and the wait's in
    // one turn, so it ended after one run (Windows CI, 2026-10-15). Up to five seconds by the clock for the three runs,
    // then a wait more in which a fourth would come if the clear had not held
    const deadline = Date.now() + 5000;
    while (runs < 3 && Date.now() < deadline) await tick(1);
    await tick(20);
    assert.equal(runs, 3);
  });
});
