// A SHARE LINK: the saved page, in the URL (docs/DATACUBE_SAVE_SHARE_2026_09_28.md, milestone 1b).
//
// The link carries the page's SETTINGS -- the cube's query, its configuration, its open rows, its
// charts and layout, and its source by identity -- never a row of data. It is the page exactly as
// Save writes it (`pageToJson`), deflated against a PRESET DICTIONARY the app ships and never sends
// (link-p1.ts: the format's own words, so they cost almost nothing), then base64url, after the `#`:
// a browser never sends the fragment to any server, so the page is stored nowhere but the link.
//
//   #p1.<base64url of deflate(page JSON, dictionary p1)>
//
// `p1` names the dictionary: a version's dictionary is frozen, so every link ever made opens
// forever; a new vocabulary is a new version, and a link of a version this build does not know is
// refused BY NAME. A link that cannot be read fails LOUDLY, never opening something half-read.
//
// Encoded, not encrypted: anyone holding the link can read the page, filter values included.

import { deflateSync, inflateSync, strFromU8, strToU8 } from 'fflate';

import { pageToJson, readPage, type PageDocument } from '../page-document.ts';
import { LINK_DICTIONARY_P1 } from './link-p1.ts';

/** The version new links are made with. */
export const LINK_VERSION = 'p1';

/**
 * Links longer than this are cut by some mail and chat tools (Outlook's and Teams' limits sit
 * near 2,000): a longer one still works, but the person is told, and offered the file instead.
 */
export const LINK_BUDGET = 2000;

const DICTIONARIES: Readonly<Record<string, Uint8Array>> = {
  p1: strToU8(LINK_DICTIONARY_P1),
};

export class ShareLinkError extends Error {
  constructor(detail: string) {
    super(`cannot open this share link: ${detail}`);
    this.name = 'ShareLinkError';
  }
}

/** The fragment for a page, without the `#`: `p1.<data>`. A link carries no saved record of the person's store, as a page
 *  file carries none (B13; the S7 confirmation audit's H2-3: it leaked the store's id, and opening it dropped it). */
export function pageFragment(page: PageDocument): string {
  const dictionary = DICTIONARIES[LINK_VERSION] as Uint8Array;
  const { record: _saved, ...shared } = page;
  const packed = deflateSync(strToU8(pageToJson(shared)), { level: 9, dictionary });
  return `${LINK_VERSION}.${toBase64Url(packed)}`;
}

/** Whether a fragment (with or without its `#`) looks like a share link at all. */
export function isPageFragment(fragment: string): boolean {
  return /^#?p\d+\./.test(fragment);
}

/** The page a fragment (with or without its `#`) carries. Throws `ShareLinkError`, saying why. */
export function readPageFragment(fragment: string): PageDocument {
  const f = fragment.startsWith('#') ? fragment.slice(1) : fragment;
  const dot = f.indexOf('.');
  const version = dot > 0 ? f.slice(0, dot) : '';
  if (!/^p\d+$/.test(version)) throw new ShareLinkError('it is not a DataCube share link');
  const dictionary = DICTIONARIES[version];
  if (!dictionary) {
    throw new ShareLinkError(`it was made by a newer DataCube (link version ${version}); update this page to open it`);
  }
  let text: string;
  try {
    text = strFromU8(inflateSync(fromBase64Url(f.slice(dot + 1)), { dictionary }));
  } catch {
    throw new ShareLinkError('it is damaged or incomplete (was it cut short when it was pasted?)');
  }
  try {
    return readPage(text);
  } catch (e) {
    throw new ShareLinkError(e instanceof Error ? e.message : String(e));
  }
}

/** A page's share link: the address it opens at, how long it is, and whether that is long. */
export interface ShareLink {
  readonly url: string;
  readonly length: number;
  /** Longer than `LINK_BUDGET`: some mail and chat tools would cut it. */
  readonly long: boolean;
}

/** The link for a page, at `base` (the page's address; any fragment it has is replaced). */
export function shareLink(base: string, page: PageDocument): ShareLink {
  const hash = base.indexOf('#');
  const url = `${hash >= 0 ? base.slice(0, hash) : base}#${pageFragment(page)}`;
  return { url, length: url.length, long: url.length > LINK_BUDGET };
}

// ---------------------------------------------------------------- base64url, no padding

function toBase64Url(bytes: Uint8Array): string {
  let binary = '';
  for (let i = 0; i < bytes.length; i += 0x8000) {
    binary += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  }
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

function fromBase64Url(text: string): Uint8Array {
  if (!/^[A-Za-z0-9_-]*$/.test(text)) throw new Error('not base64url');
  const b64 = text.replace(/-/g, '+').replace(/_/g, '/') + '='.repeat((4 - (text.length % 4)) % 4);
  const binary = atob(b64);
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) out[i] = binary.charCodeAt(i);
  return out;
}
