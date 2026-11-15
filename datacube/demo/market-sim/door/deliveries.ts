// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

// THE FILES THE WORLD'S OWNERS DROP (docs/DEPLOYMENT_DESIGN_2026_11_14.md §11): position keeping's start-of-day
// snapshot, whole as of one moment -- each dataset a deployment's plan reads as a delivered file, its rows as the world
// wrote them (the feed keeps the rows of every table no topic is made of), written as CSV at the file's URL, its header
// the plan's columns, each value as the server's CSV reader takes it (an instant ISO 8601, a day its ISO date, a truth
// true or false, a null an empty field). Node alone (node:fs).

import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

import type { KeptRow } from './door-feed.ts';

/** A plan's table, as much of it as a delivery needs: its name, source, format and columns. */
export interface DeliveredTable {
  readonly name: string;
  readonly source: { readonly kind: string; readonly url?: string };
  readonly format: string;
  readonly columns: readonly { readonly name: string; readonly type: string }[];
}

/** A value as a CSV field: quoted where it holds a separator, a quote or a line break. */
function field(value: string | number | boolean | null, type: string): string {
  if (value === null) return '';
  const text = type === 'java.time.Instant' && typeof value === 'number' ? new Date(value).toISOString() : String(value);
  return /[",\r\n]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

/** Each delivered file of `tables` written from `kept`, the world's rows by table: the files written, by table. */
export function writeDeliveries(tables: readonly DeliveredTable[], kept: ReadonlyMap<string, readonly KeptRow[]>)
  : Record<string, number> {
  const written: Record<string, number> = {};
  for (const t of tables) {
    if (t.source.kind !== 'file') continue;
    if (t.format !== 'CSV') throw new Error(`the file of ${t.name} is ${t.format}: the world drops CSV files`);
    const rows = kept.get(t.name);
    if (rows === undefined) throw new Error(`the world wrote no ${t.name}: its file has no rows to drop`);
    const lines = [t.columns.map((c) => c.name).join(',')];
    for (const r of rows) lines.push(t.columns.map((c) => field(r[c.name] ?? null, c.type)).join(','));
    const path = fileURLToPath(t.source.url!);
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, lines.join('\n') + '\n');
    written[t.name] = rows.length;
  }
  return written;
}
