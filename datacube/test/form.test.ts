import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import {
  checkbox,
  colorPicker,
  dropdown,
  normaliseHex,
  numberInput,
  textInput,
  toggle,
  toggleGroup,
} from '../src/ui/form.ts';

let dom: JSDOM;
let doc: Document;

beforeEach(() => {
  dom = new JSDOM('<!doctype html><body></body>');
  doc = dom.window.document;
});

function fire(el: Element, type: string): void {
  el.dispatchEvent(new dom.window.Event(type));
}

describe('numberInput', () => {
  it('reports a BLANK as absent, not as zero', () => {
    // "No row limit" and "row limit 0" are opposite instructions; a
    // coerced 0 silently picks the wrong one.
    let seen: number | undefined = 5;
    const el = numberInput(doc, 5, (v) => (seen = v));
    el.value = '';
    fire(el, 'change');
    assert.equal(seen, undefined);
  });

  it('reports rubbish as absent rather than NaN', () => {
    let seen: number | undefined = 5;
    const el = numberInput(doc, 5, (v) => (seen = v));
    el.value = 'abc';
    fire(el, 'change');
    assert.equal(seen, undefined);
  });

  it('clamps to the bounds it was given', () => {
    // A row limit of -1 reaches the query builder as a limit clause
    // the engine rejects; clamping at the control is the cheapest
    // place to stop it.
    let seen: number | undefined;
    const el = numberInput(doc, 10, (v) => (seen = v), { min: 1, max: 100 });
    el.value = '-5';
    fire(el, 'change');
    assert.equal(seen, 1);
    el.value = '5000';
    fire(el, 'change');
    assert.equal(seen, 100);
  });

  it('shows an absent value as an empty box, not as 0', () => {
    assert.equal(numberInput(doc, undefined, () => {}).value, '');
  });
});

describe('textInput', () => {
  it('trims, and an all-space entry is absent', () => {
    let seen: string | undefined = 'x';
    const el = textInput(doc, 'x', (v) => (seen = v));
    el.value = '   ';
    fire(el, 'change');
    assert.equal(seen, undefined);
  });
});

describe('dropdown', () => {
  it('offers (None) only when asked, and reports it as absent', () => {
    let seen: string | undefined = 'a';
    const el = dropdown(
      doc,
      'a',
      [
        { value: 'a', label: 'A' },
        { value: 'b', label: 'B' },
      ],
      (v) => (seen = v),
      { allowNone: true },
    );
    assert.equal(el.options.length, 3);
    el.value = '';
    fire(el, 'change');
    assert.equal(seen, undefined);

    const strict = dropdown(doc, 'a', [{ value: 'a', label: 'A' }], () => {});
    assert.equal(strict.options.length, 1);
  });

  it('shows the value it was given as selected', () => {
    const el = dropdown(
      doc,
      'b',
      [
        { value: 'a', label: 'A' },
        { value: 'b', label: 'B' },
      ],
      () => {},
    );
    assert.equal(el.value, 'b');
  });
});

describe('checkbox', () => {
  it('reads an absent setting as off', () => {
    const el = checkbox(doc, 'Show root aggregation', undefined, () => {});
    const box = el.querySelector('input') as HTMLInputElement;
    assert.equal(box.checked, false);
  });

  it('labels the box, so clicking the words works', () => {
    const el = checkbox(doc, 'Show leaf count', true, () => {});
    assert.equal(el.tagName, 'LABEL');
    assert.equal(el.textContent, 'Show leaf count');
  });
});

describe('colours', () => {
  it('expands short hex and rejects anything else', () => {
    assert.equal(normaliseHex('#abc'), '#aabbcc');
    assert.equal(normaliseHex('AABBCC'), '#aabbcc');
    assert.equal(normaliseHex('rebeccapurple'), undefined);
    assert.equal(normaliseHex(undefined), undefined);
  });

  it('can be CLEARED back to inheriting', () => {
    // <input type=color> has no absent state, so without this every
    // column ends up pinned to whatever the picker opened on.
    let seen: string | undefined = '#ff0000';
    const el = colorPicker(doc, '#ff0000', (v) => (seen = v));
    const clear = el.querySelector('.dc-color-clear') as HTMLButtonElement;
    clear.click();
    assert.equal(seen, undefined);
    assert.equal(el.classList.contains('dc-color-unset'), true);
  });

  it('marks an unset swatch, so it does not read as black', () => {
    const el = colorPicker(doc, undefined, () => {});
    assert.equal(el.classList.contains('dc-color-unset'), true);
  });
});

describe('toggles', () => {
  it('reports pressed state to a screen reader', () => {
    const b = toggle(doc, 'B', true, () => {});
    assert.equal(b.getAttribute('aria-pressed'), 'true');
  });

  it('flips an independent toggle', () => {
    let seen: boolean | undefined;
    toggle(doc, 'B', true, (v) => (seen = v)).click();
    assert.equal(seen, false);
  });

  it('a group picks one, and re-clicking clears only if allowed', () => {
    const choices = [
      { value: 'left', label: 'L' },
      { value: 'center', label: 'C' },
    ] as const;
    let seen: string | undefined = 'left';
    const g = toggleGroup(doc, 'left', choices, (v) => (seen = v), {
      allowNone: true,
    });
    (g.children[0] as HTMLButtonElement).click();
    assert.equal(seen, undefined, 'clicking the active one clears it');

    const g2 = toggleGroup(doc, 'left', choices, (v) => (seen = v));
    (g2.children[0] as HTMLButtonElement).click();
    assert.equal(seen, 'left', 'without allowNone it stays set');
  });
});
