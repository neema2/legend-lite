// A sheet's layout read as a board's bands (page-document.ts PageLayout: a board's, or a flow's boxes): a test that
// walks a board's bands names that it is one, and a flow there fails it in words.

import type { BandsLayout, PageLayout } from '../src/page-document.ts';

export function bandsOf(layout: PageLayout): BandsLayout {
  if (layout.kind !== 'bands') throw new Error(`a flow's boxes, not a board's bands: ${JSON.stringify(layout)}`);
  return layout;
}
