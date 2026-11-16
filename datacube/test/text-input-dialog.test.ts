// A Text Input belongs to its embedding, including when its apply operation is still waiting on file ingestion.
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { JSDOM } from 'jsdom';
import { textInputDialog } from '../src/ui/text-input-dialog.ts';

describe('an embedded Text Input window', () => {
  it('closes its UI and document listener when its owner is disposed', async () => {
    const dom = new JSDOM('<!doctype html><div id="host"></div>');
    const doc = dom.window.document;
    const host = doc.getElementById('host')!;
    const lifetime = new AbortController();
    const options = { host, signal: lifetime.signal, apply: async () => assert.fail('no table submitted') };
    const closed = textInputDialog(doc, options);
    assert.ok(host.querySelector('.dc-text-input'));
    assert.equal(doc.body.querySelector(':scope > .dc-picker-backdrop'), null);
    lifetime.abort();
    assert.equal(await closed, false);
    assert.equal(host.childElementCount, 0);
    assert.equal(doc.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape', cancelable: true })), true);
    assert.equal(await textInputDialog(doc, options), false, 'late module loading cannot reopen a disposed dialog');
    assert.equal(host.childElementCount, 0);
    dom.window.close();
  });

  it('finishes cancellation before its pending import and ignores the late answer', async () => {
    const dom = new JSDOM('<!doctype html><div id="host"></div>');
    const doc = dom.window.document;
    const host = doc.getElementById('host')!;
    const lifetime = new AbortController();
    let answer!: (value: string | null) => void;
    let submitted = 0;
    const closed = textInputDialog(doc, {
      host, signal: lifetime.signal,
      start: { name: 'trades', columns: ['qty'], rows: [['1']] },
      apply: () => { submitted++; return new Promise((resolve) => { answer = resolve; }); },
    });
    host.querySelector<HTMLButtonElement>('.dc-primary')!.click();
    assert.equal(submitted, 1);
    lifetime.abort();
    assert.equal(await closed, false);
    answer(null);
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(host.childElementCount, 0);
    dom.window.close();
  });
});
