// The band board in a DOM (layout/band-board.ts): tiles drawn at their boxes, the gestures -- a tile dragged onto an
// edge, onto a tile, between bands; a divider; a band's edge -- each applied on let go and undone by Escape, a
// cancelled pointer or a lost capture; the keyboard; the narrow page stacked; view mode; maximise; a preset previewed and
// arranged. jsdom has no layout, so the board's size is set by hand (1000 x 600 unless a test says otherwise) and the
// canvas sits at the page's top left: a pointer's client position is its place on the board.

import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { BandBoard, type BandBoardOptions, type BandTile } from '../src/layout/band-board.ts';
import { type Bands, problems, tiles } from '../src/layout/bands.ts';

let dom: JSDOM;
let host: HTMLElement;

function sized(width: number, height: number): void {
  Object.defineProperty(host, 'clientWidth', { configurable: true, get: () => width });
  Object.defineProperty(host, 'clientHeight', { configurable: true, get: () => height });
}

beforeEach(() => {
  dom = new JSDOM('<!doctype html><body><div id="host"></div></body>');
  host = dom.window.document.getElementById('host')!;
  sized(1000, 600);
});

function tile(id: string, extra: Partial<BandTile> = {}): BandTile {
  const element = dom.window.document.createElement('div');
  element.textContent = `content of ${id}`;
  return { id, title: id.toUpperCase(), element, ...extra };
}

function root(id: string): HTMLElement {
  return host.querySelector(`[data-tile="${id}"]`)!;
}

function box(id: string): { x: number; y: number; w: number; h: number } {
  const s = root(id).style;
  return { x: parseFloat(s.left), y: parseFloat(s.top), w: parseFloat(s.width), h: parseFloat(s.height) };
}

function live(): string {
  return host.querySelector('.dc-board-live')!.textContent ?? '';
}

function pointer(el: Element, type: string, x: number, y: number): void {
  el.dispatchEvent(new dom.window.PointerEvent(type, { pointerId: 1, clientX: x, clientY: y, button: 0, bubbles: true }));
}

function key(el: HTMLElement, k: string, shiftKey = false): void {
  el.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: k, shiftKey, bubbles: true }));
}

/** A board with `ids` side by side in one band, the layout's changes counted. */
function sideBySide(ids: string[], options: BandBoardOptions = {}): { board: BandBoard; changes: Bands[] } {
  const changes: Bands[] = [];
  const board = new BandBoard(host, { ...options, onChange: (layout) => changes.push(layout) });
  ids.forEach((id, i) => board.add(tile(id), i === 0 ? undefined : ids[i - 1]));
  return { board, changes };
}

function wellFormed(board: BandBoard): void {
  assert.deepEqual(problems(board.layout), []);
}

describe('the band board', () => {
  it('draws every tile at its box, by position; a new tile goes beside the one it came from', () => {
    const { board } = sideBySide(['a', 'b']);
    assert.equal(board.size, 2);
    assert.deepEqual(box('a'), { x: 0, y: 0, w: 496, h: 300 });
    assert.deepEqual(box('b'), { x: 504, y: 0, w: 496, h: 300 });
    assert.equal(root('b').querySelector('.dc-tile-body')!.textContent, 'content of b');
    // one divider between them, in the gap
    const dividers = host.querySelectorAll<HTMLElement>('.dc-band-divider-row');
    assert.equal(dividers.length, 1);
    assert.equal(dividers[0]!.style.left, '496px');
    assert.equal(dividers[0]!.style.width, '8px');
    wellFormed(board);
  });

  it('treats every tile alike: the first one removed, the rest close over its place', () => {
    const removed: string[] = [];
    const { board } = sideBySide(['a', 'b', 'c'], { onRemove: (id) => removed.push(id) });
    root('a').querySelector<HTMLButtonElement>('.dc-tile-remove:not(.dc-tile-layout):not(.dc-tile-maximise)')!.click();
    assert.deepEqual(removed, ['a'], 'the caller is asked; the board does not remove it itself');
    const content = root('a').querySelector('.dc-tile-body')!.firstElementChild!;
    board.remove('a');
    assert.equal(content.textContent, 'content of a', 'its content element is left whole');
    assert.deepEqual(tiles(board.layout), ['b', 'c']);
    assert.deepEqual(box('b'), { x: 0, y: 0, w: 496, h: 300 });
    assert.deepEqual(box('c'), { x: 504, y: 0, w: 496, h: 300 });
    wellFormed(board);
  });

  it('drags a divider: its parts follow at once, the same divider element held throughout, applied on let go', () => {
    const { board, changes } = sideBySide(['a', 'b']);
    const divider = host.querySelector<HTMLElement>('.dc-band-divider-row')!;
    pointer(divider, 'pointerdown', 500, 100);
    pointer(divider, 'pointermove', 600, 100);
    assert.equal(box('a').w, 595, 'drawn as it is dragged');
    assert.equal(host.querySelector('.dc-band-divider-row'), divider, 'not replaced under the pointer');
    assert.equal(divider.style.left, '595px');
    assert.equal(changes.length, 0, 'nothing saved until let go');
    pointer(divider, 'pointerup', 600, 100);
    assert.equal(changes.length, 1);
    assert.equal(box('a').w, 595);
    assert.equal(box('b').x, 603);
    wellFormed(board);
  });

  for (const [how, end] of [
    ['Escape', (_el: HTMLElement) => dom.window.document.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape' }))],
    ['a cancelled pointer', (el: HTMLElement) => pointer(el, 'pointercancel', 600, 100)],
    ['a lost capture', (el: HTMLElement) => pointer(el, 'lostpointercapture', 600, 100)],
  ] as const) {
    it(`undoes a divider's drag on ${how}, and the next drag starts afresh`, () => {
      const { board, changes } = sideBySide(['a', 'b']);
      const before = board.layout;
      const divider = host.querySelector<HTMLElement>('.dc-band-divider-row')!;
      pointer(divider, 'pointerdown', 500, 100);
      pointer(divider, 'pointermove', 700, 100);
      end(divider);
      assert.equal(board.layout, before);
      assert.equal(box('a').w, 496, 'drawn as it was');
      assert.equal(changes.length, 0);
      // over: a later move does nothing, and a new drag works
      pointer(divider, 'pointermove', 800, 100);
      assert.equal(box('a').w, 496);
      pointer(divider, 'pointerdown', 500, 100);
      pointer(divider, 'pointerup', 400, 100);
      assert.equal(changes.length, 1);
      assert.ok(box('a').w < 496);
    });
  }

  it('evens out a split on a divider\'s double click', () => {
    const { board } = sideBySide(['a', 'b']);
    const divider = host.querySelector<HTMLElement>('.dc-band-divider-row')!;
    pointer(divider, 'pointerdown', 500, 100);
    pointer(divider, 'pointerup', 650, 100);
    assert.notEqual(box('a').w, 496);
    host.querySelector('.dc-band-divider-row')!.dispatchEvent(new dom.window.MouseEvent('dblclick', { bubbles: true }));
    assert.equal(box('a').w, 496);
    wellFormed(board);
  });

  it('drags a tile by its title bar onto another\'s edge: the zone outlined while dragging, the tile placed on let go', () => {
    const { board, changes } = sideBySide(['a', 'b']);
    const head = root('a').querySelector<HTMLElement>('.dc-tile-head')!;
    const zone = host.querySelector<HTMLElement>('.dc-bands-zone')!;
    pointer(head, 'pointerdown', 50, 10);
    pointer(head, 'pointermove', 980, 150);
    assert.equal(root('a').style.transform, 'translate(930px, 140px)', 'the tile follows the pointer');
    assert.ok(root('a').classList.contains('dc-tile-dragging'));
    assert.equal(zone.hidden, false);
    assert.deepEqual([zone.style.left, zone.style.width], ['752px', '248px'], 'the right half of b');
    assert.equal(changes.length, 0, 'nothing re-laid out while dragging');
    pointer(head, 'pointerup', 980, 150);
    assert.equal(zone.hidden, true);
    assert.equal(root('a').style.transform, '');
    assert.deepEqual(tiles(board.layout), ['b', 'a']);
    assert.equal(changes.length, 1);
    assert.equal(live(), 'A moved.');
    wellFormed(board);
  });

  it('drags a tile onto another\'s middle (a swap) and below the last band (a band of its own)', () => {
    const { board } = sideBySide(['a', 'b', 'c']);
    const head = (id: string) => root(id).querySelector<HTMLElement>('.dc-tile-head')!;
    pointer(head('a'), 'pointerdown', 50, 10);
    pointer(head('a'), 'pointerup', 840, 150);
    assert.deepEqual(tiles(board.layout), ['c', 'b', 'a'], 'a and c traded places');
    pointer(head('b'), 'pointerdown', 400, 10);
    pointer(head('b'), 'pointermove', 500, 450);
    assert.ok(host.querySelector('.dc-bands-zone')!.classList.contains('dc-bands-zone-line'), 'a line where the band goes');
    pointer(head('b'), 'pointerup', 500, 450);
    assert.equal(board.layout.bands.length, 2);
    assert.deepEqual(box('b'), { x: 0, y: 308, w: 1000, h: 300 });
    wellFormed(board);
  });

  it('undoes a tile\'s drag on Escape: back in place, nothing moved', () => {
    const { board, changes } = sideBySide(['a', 'b']);
    const before = board.layout;
    const head = root('a').querySelector<HTMLElement>('.dc-tile-head')!;
    pointer(head, 'pointerdown', 50, 10);
    pointer(head, 'pointermove', 980, 150);
    dom.window.document.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape' }));
    assert.equal(root('a').style.transform, '');
    assert.equal(host.querySelector<HTMLElement>('.dc-bands-zone')!.hidden, true);
    assert.equal(board.layout, before);
    assert.equal(changes.length, 0);
  });

  it('never starts a drag from a title bar\'s button', () => {
    const { board } = sideBySide(['a', 'b']);
    const before = board.layout;
    const button = root('a').querySelector<HTMLElement>('.dc-tile-maximise')!;
    pointer(button, 'pointerdown', 50, 10);
    pointer(button, 'pointermove', 980, 150);
    assert.equal(root('a').style.transform, '');
    assert.equal(board.layout, before);
  });

  it('drags a band\'s edge: on a page that scrolls, the band\'s height', () => {
    const { board, changes } = sideBySide(['a', 'b']);
    const edge = host.querySelector<HTMLElement>('.dc-band-edge')!;
    assert.equal(edge.style.top, '300px');
    pointer(edge, 'pointerdown', 500, 304);
    pointer(edge, 'pointermove', 500, 364);
    assert.equal(box('a').h, 360);
    pointer(edge, 'pointerup', 500, 364);
    assert.equal(changes.length, 1);
    assert.equal(board.layout.bands[0]!.height, 0.6);
    wellFormed(board);
  });

  it('drags a band\'s edge: on a page that fits its window, the two bands trade, the page still the window\'s height', () => {
    const board = new BandBoard(host);
    board.add(tile('a'));
    board.add(tile('b'));
    board.setFit(true);
    assert.ok(host.classList.contains('dc-bands-fit'));
    assert.deepEqual([box('a').h, box('b').y + box('b').h], [296, 600]);
    const edges = host.querySelectorAll<HTMLElement>('.dc-band-edge');
    assert.equal(edges.length, 1, 'between the bands only');
    pointer(edges[0]!, 'pointerdown', 500, 300);
    pointer(edges[0]!, 'pointerup', 500, 360);
    assert.ok(box('a').h > 350);
    assert.equal(box('a').h + 8 + box('b').h, 600);
    wellFormed(board);
  });

  it('moves a focused tile with the arrow keys and sizes it with Shift+arrows, each step announced', () => {
    const { board, changes } = sideBySide(['a', 'b']);
    key(root('a'), 'ArrowRight');
    assert.deepEqual(tiles(board.layout), ['b', 'a']);
    assert.equal(live(), 'A swapped with B.');
    key(root('a'), 'ArrowRight');
    assert.equal(live(), 'A is already at the right.');
    assert.equal(changes.length, 1);
    key(root('a'), 'ArrowRight', true);
    assert.ok(box('a').w > 496, 'wider: its left divider moved left');
    assert.equal(live(), 'A wider.');
    key(root('a'), 'ArrowDown', true);
    assert.equal(box('a').h, 330, 'taller: as tall as its band, the band grew');
    assert.equal(live(), 'A taller.');
    key(root('a'), 'ArrowUp');
    assert.equal(live(), 'A is already at the top.');
    assert.equal(changes.length, 3);
    // a key inside the tile's content is the content's
    key(root('a').querySelector<HTMLElement>('.dc-tile-body > div')!, 'ArrowLeft');
    assert.deepEqual(tiles(board.layout), ['b', 'a']);
    wellFormed(board);
  });

  it('says when a tile cannot grow or shrink that way', () => {
    const board = new BandBoard(host);
    board.add(tile('a'));
    key(root('a'), 'ArrowRight', true);
    assert.equal(live(), 'A cannot grow or shrink that way.');
  });

  it('stacks a narrow page, one tile under another, and leaves the saved layout alone', () => {
    sized(500, 600);
    const { board, changes } = sideBySide(['a', 'b']);
    assert.ok(host.classList.contains('dc-bands-narrow'));
    assert.equal(board.arrangeable, false);
    assert.equal(board.layout.bands.length, 1, 'still side by side, as saved');
    assert.deepEqual(box('a'), { x: 0, y: 0, w: 500, h: 300 });
    assert.deepEqual(box('b'), { x: 0, y: 308, w: 500, h: 300 });
    assert.equal(host.querySelectorAll('.dc-band-divider, .dc-band-edge').length, 0);
    key(root('a'), 'ArrowDown');
    pointer(root('a').querySelector('.dc-tile-head')!, 'pointerdown', 50, 10);
    pointer(root('a').querySelector('.dc-tile-head')!, 'pointerup', 50, 400);
    assert.equal(changes.length, 0, 'nothing moves');
  });

  it('in view mode shows no handles and moves nothing', () => {
    const { board, changes } = sideBySide(['a', 'b']);
    board.setEditing(false);
    assert.ok(host.classList.contains('dc-bands-view'));
    assert.equal(host.querySelectorAll('.dc-band-divider, .dc-band-edge').length, 0);
    pointer(root('a').querySelector('.dc-tile-head')!, 'pointerdown', 50, 10);
    pointer(root('a').querySelector('.dc-tile-head')!, 'pointerup', 980, 150);
    key(root('a'), 'ArrowRight');
    assert.equal(changes.length, 0);
    board.setEditing(true);
    assert.equal(host.querySelectorAll('.dc-band-divider').length, 1);
  });

  it('maximises a tile to fill the board, the rest hidden, and goes back (its button, or Escape)', () => {
    const { board } = sideBySide(['a', 'b']);
    root('b').querySelector<HTMLButtonElement>('.dc-tile-maximise')!.click();
    assert.equal(board.maximised, 'b');
    assert.equal(root('a').hidden, true);
    assert.deepEqual(box('b'), { x: 0, y: 0, w: 1000, h: 600 });
    assert.equal(host.querySelectorAll('.dc-band-divider').length, 0);
    root('b').querySelector<HTMLButtonElement>('.dc-tile-maximise')!.click();
    assert.equal(board.maximised, null);
    assert.equal(root('a').hidden, false);
    assert.deepEqual(box('b'), { x: 504, y: 0, w: 496, h: 300 });
    board.maximise('a');
    key(root('a'), 'Escape');
    assert.equal(board.maximised, null);
  });

  it('previews a preset without changing the layout, then arranges it: one big on the left, two stacked on the right', () => {
    const { board, changes } = sideBySide(['a', 'b', 'c']);
    const before = board.layout;
    board.preview('left-and-column', 'c');
    assert.equal(board.layout, before, 'a preview is not the layout');
    assert.deepEqual(box('c'), { x: 0, y: 0, w: 595, h: 600 });
    assert.equal(host.querySelectorAll('.dc-band-divider').length, 0, 'no handles on a preview');
    board.preview(null);
    assert.deepEqual(box('a'), { x: 0, y: 0, w: 328, h: 300 });
    board.arrange('left-and-column');
    assert.deepEqual(box('a'), { x: 0, y: 0, w: 595, h: 600 });
    assert.deepEqual(box('b'), { x: 603, y: 0, w: 397, h: 296 });
    assert.deepEqual(box('c'), { x: 603, y: 304, w: 397, h: 296 });
    assert.equal(changes.length, 1);
    assert.equal(host.querySelectorAll('.dc-band-divider').length, 2, 'one between the halves, one in the column');
    wellFormed(board);
  });

  it('puts a saved layout: tiles not on the board left out, tiles on the board but not in it below', () => {
    const board = new BandBoard(host);
    board.add(tile('a'));
    board.add(tile('b'));
    board.setLayout({ fit: false, bands: [{ height: 1, node: { split: 'row', parts: [
      { node: { tile: 'gone' }, size: 0.5 }, { node: { tile: 'a' }, size: 0.5 },
    ] } }] });
    assert.deepEqual(tiles(board.layout), ['a', 'b']);
    assert.deepEqual(box('a'), { x: 0, y: 0, w: 1000, h: 600 });
    assert.equal(box('b').y, 608);
    wellFormed(board);
  });

  it('renames a tile on a double click of its title', () => {
    const renamed: [string, string][] = [];
    const board = new BandBoard(host, { onRename: (id, title) => renamed.push([id, title]) });
    board.add(tile('a'));
    root('a').querySelector('.dc-tile-head')!.dispatchEvent(new dom.window.MouseEvent('dblclick', { bubbles: true }));
    const input = root('a').querySelector<HTMLInputElement>('.dc-tile-rename')!;
    input.value = 'Sales';
    key(input, 'Enter');
    assert.deepEqual(renamed, [['a', 'Sales']]);
    assert.equal(board.title('a'), 'Sales');
    assert.equal(root('a').querySelector('.dc-tile-title')!.textContent, 'Sales');
  });

  it('ends a gesture when its tile goes: the board never left mid-drag', () => {
    const { board, changes } = sideBySide(['a', 'b']);
    const divider = host.querySelector<HTMLElement>('.dc-band-divider-row')!;
    pointer(divider, 'pointerdown', 500, 100);
    pointer(divider, 'pointermove', 600, 100);
    board.remove('b');
    assert.equal(divider.isConnected, false);
    assert.deepEqual(box('a'), { x: 0, y: 0, w: 1000, h: 300 });
    assert.equal(changes.length, 0);
    // a new tile's divider drags at once: nothing left over from the old gesture
    board.add(tile('c'), 'a');
    const next = host.querySelector<HTMLElement>('.dc-band-divider-row')!;
    pointer(next, 'pointerdown', 500, 100);
    pointer(next, 'pointerup', 600, 100);
    assert.equal(changes.length, 1);
    wellFormed(board);
  });

  it('offers the layouts button only when the caller shows the layouts', () => {
    const asked: string[] = [];
    new BandBoard(host).add(tile('a'));
    assert.equal(root('a').querySelector<HTMLButtonElement>('.dc-tile-layout')!.hidden, true);
    host = dom.window.document.createElement('div');
    sized(1000, 600);
    new BandBoard(host, { onLayout: (id) => asked.push(id) }).add(tile('b'));
    root('b').querySelector<HTMLButtonElement>('.dc-tile-layout')!.click();
    assert.deepEqual(asked, ['b']);
  });

  it('disposes: the host emptied, its classes gone', () => {
    const { board } = sideBySide(['a', 'b']);
    board.setEditing(false);
    board.dispose();
    assert.equal(host.children.length, 0);
    assert.equal(host.className, '');
  });
});
