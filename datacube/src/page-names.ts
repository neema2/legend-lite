// THE NAME A PERSON GIVES a page, a tile or a sheet: DataCube's one rule, asked by the screen's three renames (page-app.ts,
// layout/band-board.ts, page/cube-page.ts), by the page reader (page-document.ts readPage), and by Python
// (headless/page.ts nameGiven). A module of its own, so a layout board asks it without the page reader.

/** What a person names on a page. */
export type NamedPart = 'page' | 'tile' | 'sheet';

/**
 * A NAME A PERSON GIVES a page, a tile or a sheet, by DataCube's one rule (the user, 2026-10-15: "one rule please"):
 * its text trimmed; a page and a tile must have one -- a blank is refused (the screen keeps the name it had; Python's
 * call raises) -- and a sheet's blank is none of its own (the sheet is then named after its first grid). The reader
 * holds a saved page to the same rule (readPage). What is not text is refused, as the commands' readers refuse it (the S7
 * confirmation audit's Y2-4: Python refused it in words of its own).
 */
export function givenName(part: NamedPart, text: unknown):
  { readonly name: string | undefined } | { readonly refused: string } {
  if (typeof text !== 'string') return { refused: `a ${part}'s name is text, not ${JSON.stringify(text) ?? String(text)}` };
  const name = text.trim();
  if (name !== '') return { name };
  return part === 'sheet' ? { name: undefined } : { refused: `a ${part}'s name is not blank` };
}
