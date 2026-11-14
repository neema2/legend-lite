// THE HASH TWO EDITORS CHECK A PAGE BY (docs/DATACUBE_LIVE_EDITING_2026_10_24.md §6, W17): SHA-256 of the page's
// CANONICAL JSON -- compact, each object's keys in code point order, every number its own digits -- which Python's
// keeper computes the same way (_json.canonical, keeper.py page_hash) of the page it keeps. The tab compares its page's
// hash with the keeper's on every write's answer and every follow: a mismatch is the one message that must never show
// (the page is then opened again as the keeper has it, said). The canonical form is content, not text: a page a command
// made and the same page the screen wrote, whose keys come in another order, hash alike. The digest is computed here,
// synchronously (FIPS 180-4, SHA-256), so the channel's rules run with no turn of the event loop between a write's
// answer and its check (test/page-hash.test.ts holds it to the platform's digest).

import { ExactNumber, fromJson } from '../../pure-protocol/src/index.ts';
import { pageToJson, type PageDocument } from './page-document.ts';
import { sha256Hex } from '../../engine-client/src/sha256.ts';

/** The canonical text of JSON `text`: keys sorted, compact, numbers as their digits. */
export function canonicalJson(text: string): string {
  return write(fromJson(text));
}

function write(v: unknown): string {
  if (v === null || v === undefined) return 'null';
  if (v instanceof ExactNumber) return v.text;
  if (typeof v === 'boolean' || typeof v === 'number') return JSON.stringify(v);
  if (typeof v === 'string') return JSON.stringify(v);
  if (Array.isArray(v)) return `[${v.map(write).join(',')}]`;
  const o = v as Record<string, unknown>;
  const keys = Object.keys(o).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  return `{${keys.map((k) => `${JSON.stringify(k)}:${write(o[k])}`).join(',')}}`;
}

// SHA-256 itself is engine-client's (sha256.ts): one implementation, which a host's plan reader checks a payload with
// too (plan-params.ts, the tenets audit's M3)
export { sha256Hex } from '../../engine-client/src/sha256.ts';

/** The page's hash, as its keeper computes it of the page it keeps. */
export function pageHash(page: PageDocument): string {
  return sha256Hex(canonicalJson(pageToJson(page)));
}
