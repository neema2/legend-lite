// A NEWS FEED, DRAWN (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K15; ui/chart-feed.ts): a result's rows as cards, the
// newest first -- each slot as the grid writes it, its time of day on the newest card's day and its date before, its
// weight and tone as classes -- and drawn again at each tick with the cards it had kept: a card the last draw had is the
// same element, a new one marked, a gone one gone; a card or a ticker chip on it picked.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { JSDOM } from 'jsdom';
import { FeedView, feedCards, type FeedPick } from '../src/ui/chart-feed.ts';
import type { ChartFeed } from '../src/chart-spec.ts';
import type { ResultTable, Scalar } from '../../engine-client/src/result.ts';

const FEED: ChartFeed = {
  headline: 'headline', time: 'at', body: 'body', source: 'source', tags: ['category'], weight: 'weight',
  tickers: 'tickers', tone: 'tone', key: 'id',
};
const COLUMNS = ['id', 'at', 'headline', 'body', 'source', 'category', 'weight', 'tickers', 'tone', 'ticker'] as const;
const TYPES: Record<string, string> = { at: 'DateTime', tone: 'Float' };
type Story = readonly [string, string, string, string | null, string, string, string, string | null, number | null, string | null];
const STORIES: Story[] = [
  ['NS-3', '2026-10-25T14:31:05.250Z', 'Apex Systems raises its full-year outlook', 'Apex Systems (APX) shares rose 3.1% to $41.20.',
    'Market Wire', 'Guidance', 'FLASH', 'APX', 0.8, 'APX'],
  ['NS-2', '2026-10-25T14:30:59.000Z', 'Banks slide as yields fall', null, 'Market Wire', 'Industry', 'STORY', 'BNK CAP TRU', -0.35, null],
  ['NS-1', '2026-10-24T20:01:00.000Z', 'Fed official says cuts could come sooner', null, 'Macro Desk', 'Macro', 'BRIEF', null, null, null],
];
const table = (stories: readonly Story[]): ResultTable => ({
  columns: COLUMNS.map((name, c) => ({ name, type: TYPES[name] ?? 'String', values: stories.map((s) => s[c] as Scalar) })),
  rowCount: stories.length, epoch: 1, elapsedMs: 0,
});
const label = (v: Scalar, column: string): string => (column === 'at' ? `[${String(v)}]` : String(v));

describe('a news feed\'s cards', () => {
  it('each slot as the grid writes it; the newest day\'s by its time of day, an older one\'s by its date', () => {
    const cards = feedCards(FEED, table(STORIES), label);
    assert.deepEqual(cards.map((c) => c.time), ['14:31:05', '14:30:59', 'Oct 24 20:01']);
    assert.deepEqual(cards.map((c) => c.stamp), STORIES.map((s) => `[${s[1]}]`));
    assert.deepEqual(cards.map((c) => c.key), ['NS-3', 'NS-2', 'NS-1']);
    assert.deepEqual(cards[1]?.tickers, ['BNK', 'CAP', 'TRU']);
    assert.deepEqual(cards[2]?.tickers, []);
    assert.deepEqual(cards.map((c) => c.weight), ['flash', 'story', 'brief']);
    assert.deepEqual(cards.map((c) => c.tone), [0.8, -0.35, undefined]);
    assert.deepEqual(cards.map((c) => c.tags), [['Guidance'], ['Industry'], ['Macro']]);
    assert.equal(cards[1]?.body, undefined);
    assert.equal(cards[0]?.row.get('ticker'), 'APX');
  });

  it('drawn again at a tick: a kept card the same element, a new one marked new, a gone one gone; never marked at first', () => {
    const doc = new JSDOM('<!doctype html><body></body>').window.document;
    const view = new FeedView(doc, () => {});
    doc.body.append(view.element);
    view.draw(feedCards(FEED, table(STORIES.slice(1)), label), 'two');
    const first = [...view.element.children];
    assert.deepEqual(first.map((li) => li.getAttribute('data-key')), ['NS-2', 'NS-1']);
    assert.ok(first.every((li) => !li.classList.contains('dc-feed-new')));
    assert.ok(first[1]?.classList.contains('dc-feed-brief'));
    assert.ok(first[0]?.classList.contains('dc-feed-down'));
    assert.equal((first[0] as HTMLElement).style.getPropertyValue('--dc-feed-tone'), '0.35');
    // a story crosses, the oldest goes
    view.draw(feedCards(FEED, table(STORIES.slice(0, 2)), label), 'two again');
    const now = [...view.element.children];
    assert.deepEqual(now.map((li) => li.getAttribute('data-key')), ['NS-3', 'NS-2']);
    assert.equal(now[1], first[0]);
    assert.ok(now[0]?.classList.contains('dc-feed-new'));
    assert.ok(now[0]?.classList.contains('dc-feed-flash'));
    assert.equal(now[0]?.querySelector('.dc-feed-weight')?.textContent, 'FLASH');
    assert.equal(now[0]?.querySelector('.dc-feed-headline')?.textContent, 'Apex Systems raises its full-year outlook');
    assert.equal(now[1]?.querySelector('.dc-feed-time')?.getAttribute('title'), '[2026-10-25T14:30:59.000Z]');
    assert.equal(view.element.getAttribute('aria-label'), 'two again');
    assert.deepEqual(now.map((li) => li.getAttribute('aria-posinset')), ['1', '2']);
  });

  it('a card picked says its row; a ticker chip on it says that ticker too', () => {
    const doc = new JSDOM('<!doctype html><body></body>').window.document;
    const picks: FeedPick[] = [];
    const view = new FeedView(doc, (p) => picks.push(p));
    doc.body.append(view.element);
    view.draw(feedCards(FEED, table(STORIES), label), 'three');
    (view.element.querySelector('[data-key="NS-3"] .dc-feed-headline') as HTMLElement).click();
    (view.element.querySelector('[data-key="NS-2"] .dc-feed-ticker[data-ticker="CAP"]') as HTMLElement).click();
    assert.equal(picks[0]?.row.get('id'), 'NS-3');
    assert.equal(picks[0]?.ticker, undefined);
    assert.equal(picks[1]?.row.get('id'), 'NS-2');
    assert.equal(picks[1]?.ticker, 'CAP');
  });
});
