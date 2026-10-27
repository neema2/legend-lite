// FILES IN A TEXT BOX (docs/DATACUBE_UI_REVIEW_2026_10_24.md §8, step 1; the user, 2026-10-25): open or drop files on a
// text box and preview them in it. A file is POINTED TO, never copied into the page -- its name, size and fingerprint
// (its SHA-256) are kept, its bytes stay where it was picked: a handle kept in this browser (file-handles.ts), else the
// file asked for again, the fingerprint saying whether it is the one added. What a file is drawn as follows from its
// name -- the one rule here, read by the page's command, its reader and Python (through headless) alike.

/** A file a text box shows, as its page keeps it. */
export interface TextFile {
  readonly name: string;
  /** Its bytes, when it was added. */
  readonly size: number;
  /** Lower-case hex SHA-256 of its bytes: whether a file given later is the one added. */
  readonly sha256: string;
}

/** How a file is drawn: Markdown as a text box draws its own words, text as it is, CSV as a table, JSON indented. */
export type TextFileFormat = 'markdown' | 'text' | 'csv' | 'json';

const BY_EXTENSION: Readonly<Record<string, TextFileFormat>> = {
  md: 'markdown',
  markdown: 'markdown',
  txt: 'text',
  text: 'text',
  log: 'text',
  csv: 'csv',
  tsv: 'csv',
  json: 'json',
};

/** The most files one text box shows. */
export const MAX_TEXT_FILES = 12;

/** How much of a file a text box draws: the rest is said, not read into the page. */
export const TEXT_FILE_SHOWN = 2 * 1024 * 1024;

/** The largest file a text box takes: each is read whole once, to fingerprint it (the 2026-10-25 audit: a log of a
 *  gigabyte was read and hashed before its kind was looked at). */
export const MAX_TEXT_FILE_SIZE = 64 * 1024 * 1024;

/** A file's size as a person reads it: 812 bytes, 14 KB, 3.2 MB, 64 MB. */
export function bytesWords(bytes: number): string {
  if (bytes < 1024) return `${bytes} byte${bytes === 1 ? '' : 's'}`;
  if (bytes < 1024 * 1024) return `${Math.round(bytes / 1024)} KB`;
  const mb = bytes / (1024 * 1024);
  return `${Number.isInteger(mb) ? mb : mb.toFixed(1)} MB`;
}

/** The extensions a text box opens, as a file picker takes them. */
export const TEXT_FILE_EXTENSIONS: readonly string[] = Object.keys(BY_EXTENSION).map((e) => `.${e}`);

/** What a file is drawn as, by its name's extension; undefined: a kind a text box does not show (yet). */
export function textFileFormat(name: string): TextFileFormat | undefined {
  const dot = name.lastIndexOf('.');
  return dot < 0 ? undefined : BY_EXTENSION[name.slice(dot + 1).toLowerCase()];
}

/** A tab-separated file's separator, else a comma. */
export function csvSeparator(name: string): ',' | '\t' {
  return name.toLowerCase().endsWith('.tsv') ? '\t' : ',';
}

const FIELDS = ['name', 'size', 'sha256'] as const;

/** Why a file of this name and size is no text box's, in words; undefined: a kind it shows, of a size it takes. */
function fileRefusal(name: string, size: number): string | undefined {
  if (textFileFormat(name) === undefined) {
    return `${name} is not a kind of file a text box shows: Markdown, text, CSV or JSON (${TEXT_FILE_EXTENSIONS.join(' ')})`;
  }
  if (size > MAX_TEXT_FILE_SIZE) return `${name} is ${bytesWords(size)}, larger than the ${bytesWords(MAX_TEXT_FILE_SIZE)} a text box takes`;
  return undefined;
}

/**
 * Why files picked (their names and sizes) would be refused, in the words `readTextFiles` says it -- too many, a kind a
 * text box does not show, too big -- asked before they are read (the screen fingerprints each by reading it whole);
 * undefined: they may be read.
 */
export function pickedRefusal(files: readonly { readonly name: string; readonly size: number }[]): string | undefined {
  if (files.length > MAX_TEXT_FILES) return `a text box shows ${MAX_TEXT_FILES} files at most`;
  for (const f of files) {
    const why = fileRefusal(f.name, f.size);
    if (why !== undefined) return why;
  }
  return undefined;
}

/**
 * `files` read as a text box's files: each its name, size and fingerprint, in order -- or why not, in words: not a list,
 * too many, a file missing one of the three or carrying another, a kind a text box does not show, one too big, the same
 * file twice.
 */
export function readTextFiles(files: unknown): readonly TextFile[] | string {
  if (!Array.isArray(files)) return "a text box's files are a list";
  if (files.length > MAX_TEXT_FILES) return `a text box shows ${MAX_TEXT_FILES} files at most`;
  const out: TextFile[] = [];
  const seen = new Set<string>();
  for (const f of files as unknown[]) {
    if (typeof f !== 'object' || f === null || Array.isArray(f)) return 'a file is its name, size and sha256';
    const o = f as Record<string, unknown>;
    const extra = Object.keys(o).find((k) => !(FIELDS as readonly string[]).includes(k));
    if (extra !== undefined) return `a file is its name, size and sha256, not its ${extra}`;
    const { name, size, sha256 } = o;
    if (typeof name !== 'string' || name.trim() === '') return 'a file has a name';
    if (typeof size !== 'number' || !Number.isInteger(size) || size < 0) return `${name}'s size is a whole number of bytes`;
    if (typeof sha256 !== 'string' || !/^[0-9a-f]{64}$/.test(sha256)) return `${name}'s sha256 is 64 lower-case hex digits`;
    const why = fileRefusal(name, size);
    if (why !== undefined) return why;
    if (seen.has(sha256)) return `${name} is in the text box twice`;
    seen.add(sha256);
    out.push({ name, size, sha256 });
  }
  return out;
}
