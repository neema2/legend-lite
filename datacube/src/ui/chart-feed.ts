// A NEWS FEED, DRAWN (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K15): the cube's rows as cards, the newest first, as a
// news terminal's pane shows its stories -- each its time and headline, its source and tags, its tickers as chips, its
// weight (a FLASH in the bad colour and bold, a BRIEF quiet) and its tone on its edge. A card new since the last draw
// comes in at the top, marked a moment; a card the last draw had is the same element, moved, never drawn again. The rows
// are the panel's query's (chart-spec.ts feedSnapshot), every value as the grid writes it; fetched with the first feed,
// never with a grid alone.

import type { ChartFeed } from '../chart-spec.ts';
import type { LabelOf } from '../chart-option.ts';
import type { ResultTable, Scalar } from '../../../engine-client/src/result.ts';

/** A card picked: its row's values by column, and the ticker whose chip was the pick (none: the card itself). */
export interface FeedPick {
  readonly row: ReadonlyMap<string, Scalar>;
  readonly ticker?: string;
}

/** A card as drawn: each slot's text as the grid writes it. */
export interface FeedCard {
  /** what keeps it itself across ticks: its key's value, else its time and headline */
  readonly key: string;
  /** its time as a terminal says it: the time of day on the newest card's day (UTC, as stored), else its date too */
  readonly time: string;
  /** its time as the grid writes it */
  readonly stamp: string;
  readonly headline: string;
  readonly body?: string;
  readonly source?: string;
  readonly tags: readonly string[];
  /** its weight's word, lowered: flash, story, brief (a class of its own each) */
  readonly weight?: string;
  readonly tickers: readonly string[];
  /** its tone, -1 to 1: the bad colour below zero, the good above, as strong as it is far from zero */
  readonly tone?: number;
  readonly row: ReadonlyMap<string, Scalar>;
}

const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
/** A date-time's text as stored (the database's, UTC): its date and its time of day. */
const STAMP = /^(\d{4})-(\d\d)-(\d\d)(?:[T ](\d\d):(\d\d)(?::(\d\d))?)?/;

/** The cards of `rows`, in their order (the query's: the newest first). */
export function feedCards(feed: ChartFeed, rows: ResultTable, label: LabelOf): FeedCard[] {
  const at = new Map(rows.columns.map((c) => [c.name, c] as const));
  const text = (column: string | undefined, i: number): string | undefined => {
    const c = column === undefined ? undefined : at.get(column);
    const v = c?.values[i];
    return c === undefined || v === null || v === undefined || v === '' ? undefined : label(v, c.name, c.type);
  };
  const day = (v: Scalar | undefined): string | undefined => (typeof v === 'string' ? STAMP.exec(v)?.slice(1, 4).join('-') : undefined);
  const times = at.get(feed.time);
  const newest = day(times?.values[0]);
  const out: FeedCard[] = [];
  for (let i = 0; i < rows.rowCount; i++) {
    const row = new Map(rows.columns.map((c) => [c.name, c.values[i] ?? null] as const));
    const raw = times?.values[i];
    const m = typeof raw === 'string' ? STAMP.exec(raw) : null;
    const clock = m?.[4] === undefined ? '' : `${m[4]}:${m[5]}${m[6] !== undefined ? `:${m[6]}` : ''}`;
    const time = m === null ? (text(feed.time, i) ?? '') : day(raw) === newest && clock !== ''
      ? clock : `${MONTHS[Number(m[2]) - 1]} ${Number(m[3])}${clock !== '' ? ` ${clock.slice(0, 5)}` : ''}`;
    const headline = text(feed.headline, i) ?? '';
    const key = text(feed.key, i) ?? `${String(raw)}|${headline}`;
    const tone = feed.tone === undefined ? undefined : row.get(feed.tone);
    const weight = text(feed.weight, i);
    const body = text(feed.body, i);
    const source = text(feed.source, i);
    out.push({
      key, time, stamp: text(feed.time, i) ?? '', headline,
      ...(body !== undefined ? { body } : {}),
      ...(source !== undefined ? { source } : {}),
      tags: (feed.tags ?? []).map((t) => text(t, i)).filter((t): t is string => t !== undefined),
      ...(weight !== undefined ? { weight: weight.toLowerCase() } : {}),
      tickers: (text(feed.tickers, i) ?? '').split(/\s+/).filter((t) => t !== ''),
      ...(typeof tone === 'number' && Number.isFinite(tone) ? { tone: Math.max(-1, Math.min(1, tone)) } : {}),
      row,
    });
  }
  return out;
}

/** How long a new card is marked as new. */
const NEW_MS = 4000;

/**
 * The feed's list, drawn into `host` and drawn again on each tick: its cards keyed, so that a card the last draw had is
 * kept (its element moved where it now is) and a new one comes in marked. A pick -- a card, or a ticker chip on it -- is
 * said to `onPick`; a card is reached by the keyboard too (Enter picks it).
 */
export class FeedView {
  readonly element: HTMLOListElement;
  readonly #doc: Document;
  readonly #onPick: (pick: FeedPick) => void;
  readonly #cards = new Map<string, { readonly element: HTMLLIElement; signature: string; card: FeedCard }>();
  #drawn = false;

  constructor(doc: Document, onPick: (pick: FeedPick) => void) {
    this.#doc = doc;
    this.#onPick = onPick;
    this.element = doc.createElement('ol');
    this.element.className = 'dc-feed';
    this.element.setAttribute('role', 'feed');
    this.element.addEventListener('click', (e) => this.#clicked(e));
    this.element.addEventListener('keydown', (e) => {
      if (e.key !== 'Enter') return;
      const card = (e.target as Element | null)?.closest?.('.dc-feed-card');
      const key = card?.getAttribute('data-key');
      const kept = key == null ? undefined : this.#cards.get(key);
      if (kept) this.#onPick({ row: kept.card.row });
    });
  }

  /** The cards now, in their order: kept ones moved, new ones made (marked new but on the first draw), gone ones gone. */
  draw(cards: readonly FeedCard[], description: string): void {
    this.element.setAttribute('aria-label', description);
    const seen = new Set<string>();
    let before: ChildNode | null = this.element.firstChild;
    for (const card of cards) {
      if (seen.has(card.key)) continue;
      seen.add(card.key);
      const signature = JSON.stringify([card.time, card.stamp, card.headline, card.body, card.source, card.tags, card.weight,
        card.tickers, card.tone]);
      let kept = this.#cards.get(card.key);
      if (kept === undefined) {
        kept = { element: this.#doc.createElement('li'), signature: '', card };
        if (this.#drawn) {
          kept.element.classList.add('dc-feed-new');
          const element = kept.element;
          setTimeout(() => element.classList.remove('dc-feed-new'), NEW_MS);
        }
        this.#cards.set(card.key, kept);
      }
      kept.card = card;
      if (kept.signature !== signature) {
        this.#paint(kept.element, card);
        kept.signature = signature;
      }
      if (kept.element !== before) this.element.insertBefore(kept.element, before);
      before = kept.element.nextSibling;
    }
    for (const [key, kept] of this.#cards) {
      if (seen.has(key)) continue;
      kept.element.remove();
      this.#cards.delete(key);
    }
    this.element.querySelectorAll('.dc-feed-card').forEach((li, i) => {
      li.setAttribute('aria-posinset', String(i + 1));
      li.setAttribute('aria-setsize', String(seen.size));
    });
    this.#drawn = true;
  }

  #paint(li: HTMLLIElement, card: FeedCard): void {
    const doc = this.#doc;
    const span = (className: string, words: string): HTMLSpanElement => {
      const el = doc.createElement('span');
      el.className = className;
      el.textContent = words;
      return el;
    };
    const keepNew = li.classList.contains('dc-feed-new');
    li.className = `dc-feed-card${card.weight !== undefined ? ` dc-feed-${card.weight.replace(/[^a-z]/g, '')}` : ''}`
      + `${card.tone === undefined || card.tone === 0 ? '' : card.tone > 0 ? ' dc-feed-up' : ' dc-feed-down'}${keepNew ? ' dc-feed-new' : ''}`;
    li.setAttribute('data-key', card.key);
    li.tabIndex = 0;
    if (card.tone !== undefined) li.style.setProperty('--dc-feed-tone', Math.abs(card.tone).toFixed(2));
    else li.style.removeProperty('--dc-feed-tone');
    const meta = doc.createElement('div');
    meta.className = 'dc-feed-meta';
    const time = doc.createElement('time');
    time.className = 'dc-feed-time';
    time.textContent = card.time;
    time.title = card.stamp;
    meta.append(time);
    if (card.weight !== undefined && card.weight !== 'story') meta.append(span('dc-feed-weight', card.weight.toUpperCase()));
    if (card.source !== undefined) meta.append(span('dc-feed-source', card.source));
    for (const tag of card.tags) meta.append(span('dc-feed-tag', tag));
    const headline = doc.createElement('div');
    headline.className = 'dc-feed-headline';
    headline.textContent = card.headline;
    li.replaceChildren(meta, headline);
    if (card.body !== undefined) {
      const body = doc.createElement('div');
      body.className = 'dc-feed-body';
      body.textContent = card.body;
      li.append(body);
    }
    if (card.tickers.length > 0) {
      const chips = doc.createElement('div');
      chips.className = 'dc-feed-tickers';
      for (const t of card.tickers) {
        const b = doc.createElement('button');
        b.type = 'button';
        b.className = 'dc-feed-ticker';
        b.textContent = t;
        b.setAttribute('data-ticker', t);
        chips.append(b);
      }
      li.append(chips);
    }
  }

  #clicked(e: MouseEvent): void {
    const target = e.target as Element | null;
    const card = target?.closest?.('.dc-feed-card');
    const key = card?.getAttribute('data-key');
    const kept = key == null ? undefined : this.#cards.get(key);
    if (!kept) return;
    const ticker = target?.closest?.('.dc-feed-ticker')?.getAttribute('data-ticker');
    this.#onPick(ticker == null ? { row: kept.card.row } : { row: kept.card.row, ticker });
  }
}
