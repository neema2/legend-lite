// A text tile's words drawn on a page (src/page-text.ts textElement): elements whose every word is text -- markup typed
// is shown, never run -- a link opening in a new tab.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { readText, textEditor, textElement } from '../src/page-text.ts';

describe("a text tile's words drawn", () => {
  it('as elements whose every word is text: markup typed is shown, never run', () => {
    const doc = new JSDOM('').window.document;
    const el = textElement(doc, readText('## <b>Note</b>\n\n<img src=x onerror=alert(1)> [go](https://x.test)\n\n1. one\n2. two'));
    assert.equal(el.querySelector('h4')?.textContent, '<b>Note</b>');
    assert.equal(el.querySelector('img'), null);
    assert.match(el.querySelector('p')?.textContent ?? '', /<img src=x/);
    const a = el.querySelector('a');
    assert.deepEqual([a?.getAttribute('href'), a?.target, a?.rel], ['https://x.test', '_blank', 'noopener noreferrer']);
    assert.deepEqual([...el.querySelectorAll('ol li')].map((li) => li.textContent), ['one', 'two']);
  });

  it('its editor: OK judged by the rule first, the words told; Escape cancels; Ctrl+Enter is OK', () => {
    const dom = new JSDOM('');
    const doc = dom.window.document;
    const told: string[] = [];
    let cancelled = 0;
    const editor = textEditor(doc, { words: '# T', onOk: (w) => told.push(w), onCancel: () => { cancelled += 1; } });
    doc.body.append(...editor.elements);
    const ok = [...doc.querySelectorAll('button')].find((b) => b.textContent === 'OK')!;
    editor.area.value = '  ';
    ok.click();
    assert.match(doc.querySelector('.dc-text-problem')?.textContent ?? '', /blank/);
    assert.deepEqual(told, [], 'refused: nothing told');
    editor.area.value = 'New';
    editor.area.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Enter', ctrlKey: true }));
    assert.deepEqual(told, ['New']);
    editor.area.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape' }));
    assert.equal(cancelled, 1);
  });
});
