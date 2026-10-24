// A TEXT TILE'S RULE (page-text.ts reads and draws its words, a chunk of its own): what a text tile's text may be -- the
// page document's reader and the page commands judge it at startup, the tile's editor before it tells it.

/** The longest text a tile holds: a page's note, not a document. */
export const MAX_TEXT = 10_000;

/** Why a tile's text is not one, in words; null when it is. */
export function textProblem(text: unknown): string | null {
  if (typeof text !== 'string') return `a text tile's text is words, not ${JSON.stringify(text) ?? 'nothing'}`;
  if (text.trim() === '') return 'a text tile has words: its text is blank';
  if (text.length > MAX_TEXT) return `a text tile holds at most ${MAX_TEXT.toLocaleString('en-US')} characters, not ${text.length.toLocaleString('en-US')}`;
  return null;
}
