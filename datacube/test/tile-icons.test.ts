// The page's chrome drawn in one set (ui/tile-icons.ts chromeIcon, closeMark; docs/DATACUBE_UI_REVIEW_2026_10_24.md,
// found 4): its marks were typed glyphs, each from whatever font the browser found.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { chromeIcon, closeMark, type ChromeIcon } from '../src/ui/tile-icons.ts';

const doc = new JSDOM('<!doctype html><body></body>').window.document;

describe("the chrome's marks", () => {
  it('are drawn on the set\'s square in its stroke, named for a test, hidden from a screen reader', () => {
    const names: readonly ChromeIcon[] = ['layout', 'maximise', 'restore', 'close', 'menu', 'more', 'chevronRight', 'chevronDown', 'chevronLeft'];
    for (const name of names) {
      const svg = chromeIcon(doc, name);
      assert.equal(svg.getAttribute('data-icon'), name);
      assert.equal(svg.getAttribute('viewBox'), '0 0 16 16');
      assert.equal(svg.getAttribute('aria-hidden'), 'true');
      assert.equal(svg.getAttribute('class'), 'dc-chrome-icon');
      const paths = [...svg.querySelectorAll('path')];
      assert.ok(paths.length > 0, name);
      assert.ok(paths.every((p) => p.getAttribute('stroke') === 'currentColor' && p.getAttribute('fill') === 'none'), `${name}: stroked in the text's colour`);
    }
  });

  it('a close button drawn with the cross, and named where its glyph was all that named it', () => {
    const bare = doc.createElement('button');
    bare.textContent = '×';
    closeMark(bare);
    assert.equal(bare.textContent, '', 'the glyph gone');
    assert.equal(bare.querySelector('svg')?.getAttribute('data-icon'), 'close');
    assert.equal(bare.getAttribute('aria-label'), 'Close', 'named, its glyph having been its name');
    // its hover words name it, else the label given; one already named keeps its name
    const titled = doc.createElement('button');
    titled.title = 'Clear colour';
    assert.equal(closeMark(titled).getAttribute('aria-label'), 'Clear colour');
    assert.equal(closeMark(doc.createElement('button'), 'Remove').getAttribute('aria-label'), 'Remove');
    const named = doc.createElement('button');
    named.setAttribute('aria-label', 'Take this condition off');
    assert.equal(closeMark(named).getAttribute('aria-label'), 'Take this condition off');
  });
});
