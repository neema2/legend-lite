// Receipts: what ran a query, where, and as whom -- from the thing that ran it.
//
// The plane button and the host's status text are the tab describing itself;
// a person cannot tell from them whether a query really went to a server.
// A receipt is issued by the ENGINE that ran the query, per query, and carries
// what only that engine knows: the warehouse's own statement id and row count,
// the SQL legend-engine reports having run. Where the server keeps a record
// (the warehouse's history, per user), the receipt can ask it, separately,
// whether the statement is there -- evidence the tab does not produce.
//
// Every plane issues one, the tab's DuckDB included, so "nothing remote ran"
// is stated rather than left to be inferred from a missing badge.

import { UI_LOCALE } from './locale.ts';

export type ReceiptPlane = 'tab' | 'warehouse' | 'engine';

export interface Receipt {
  readonly plane: ReceiptPlane;
  /** Where it ran, as a person reads it: "this tab's DuckDB", "the warehouse at 127.0.0.1:9090". */
  readonly where: string;
  /** Who the server ran it as (the warehouse's principal). */
  readonly as?: string;
  /** The server's own id for the statement. */
  readonly statementId?: string;
  /** Rows as the server counted them. */
  readonly serverRows?: number;
  /** The SQL the server reports having run (legend-engine's activities). */
  readonly serverSql?: string;
  /** What the server wrote beside it, verbatim (legend-engine's activity comment: its trace id). */
  readonly serverNote?: string;
  /** Remote files the tab's DuckDB read over HTTP for it (a mounted source). */
  readonly reading?: readonly string[];
  /** Set while snapped: the copy this read, and the pull that made it. */
  readonly copy?: { readonly takenAt: Date; readonly from?: Receipt };
  /** Ask the server, apart from this query, whether it has the statement on record. */
  readonly check?: () => Promise<string>;
}

/** A host:port for a URL, which is what a person recognises in a receipt. */
export function hostOf(url: string): string {
  try {
    return new URL(url).host || url;
  } catch {
    return url;
  }
}

/** One short line: the status bar's chip. */
export function receiptLabel(r: Receipt): string {
  if (r.copy) return `this tab's copy (${r.copy.takenAt.toLocaleTimeString(UI_LOCALE)})`;
  switch (r.plane) {
    case 'warehouse':
      return [r.where, r.as, r.statementId ? `#${r.statementId.slice(0, 8)}` : undefined]
        .filter((x) => x !== undefined).join(' · ');
    case 'engine':
      return r.where;
    case 'tab':
      return r.reading && r.reading.length > 0
        ? `this tab, reading ${r.reading.map(hostOf).join(', ')}`
        : 'this tab';
  }
}

/** The whole receipt, a line per fact: the chip's tooltip and the Receipts window. */
export function receiptLines(r: Receipt): string[] {
  const lines = [`Ran on ${r.where}${r.as ? ` as ${r.as}` : ''}.`];
  if (r.statementId) lines.push(`Server statement id: ${r.statementId}`);
  if (r.serverRows !== undefined) lines.push(`Rows, as the server counted them: ${r.serverRows.toLocaleString(UI_LOCALE)}`);
  if (r.reading && r.reading.length > 0) lines.push(`Read over HTTP: ${r.reading.join(', ')}`);
  if (r.serverSql) lines.push(`SQL the server reports running: ${r.serverSql}`);
  if (r.serverNote) lines.push(`The server's note: ${r.serverNote}`);
  if (r.copy) {
    const from = r.copy.from;
    lines.push(`Read from the snap copied into this tab at ${r.copy.takenAt.toLocaleTimeString(UI_LOCALE)}`
      + (from ? `, pulled from ${from.where}${from.as ? ` as ${from.as}` : ''}`
        + (from.statementId ? ` (statement ${from.statementId})` : '') : '')
      + '. Nothing was sent to a server for this query.');
  } else if (r.plane === 'tab') {
    lines.push(r.reading && r.reading.length > 0
      ? 'Nothing was sent to a query server; the file itself was fetched.'
      : 'Nothing left this tab.');
  }
  return lines;
}
