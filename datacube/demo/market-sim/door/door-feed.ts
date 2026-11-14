// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

// THE WORLD THROUGH THE DOOR (docs/DESK_DATA_ROADMAP_2026_10_29.md B1): the market world's rows as the records of the
// topics its tables' datasets read -- the model's topics (the planner's `deephaven` request, op `topics`), each record
// one row as JSON with the dataset's columns, keyed by the dataset's key where the topic keeps each key's latest. A
// world's table no stream reads (a master's, a position file's: B3) is no topic, and the feed writes none of it. Each
// value as Deephaven's JSON reader takes it (measured 2026-10-30 on the pinned 42.7, B1's measurement): a number or
// text as it is, an instant its epoch milliseconds, a truth a JSON boolean, a day its ISO date (a LocalDate column is
// refused by that reader, so a day travels as text), a value the row has not a null.

import type { MadeColumn } from '../../../../engine-client/src/arrow-ipc.ts';
import type { MarketSink } from '../market.ts';
import type { DoorTopic, KafkaDoor } from './kafka-door.ts';

/** A topic as the planner answers it (Boundary.topics): its landing table, its name, its records' columns (each with
 *  the Java type a Deephaven column holds it as), the key a compacted topic keeps each latest of, its retention. */
export interface StreamTopic {
  readonly table: string;
  readonly topic: string;
  readonly columns: readonly { readonly name: string; readonly type: string }[];
  readonly key: readonly string[];
  readonly keepRows: number | null;
  readonly keepFor: string | null;
}

/** The topics the planner answered (its `deephaven` request, op `topics`: `OK` and the JSON, or `ERR` and why). */
export function topicsOf(answer: string): StreamTopic[] {
  if (!answer.startsWith('OK\n')) {
    throw new Error(`the planner refused the topics: ${answer.split('\n').slice(2).join('\n') || answer}`);
  }
  return (JSON.parse(answer.slice(3)) as { readonly topics: StreamTopic[] }).topics;
}

/** An ISO 8601 duration of days, hours, minutes and seconds (`PT8H`, `P1DT30M`) in ms; another is refused by name. */
export function durationMs(iso: string): number {
  const m = /^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?$/.exec(iso);
  if (m === null || iso === 'P' || iso.endsWith('T')) {
    throw new Error(`the retention '${iso}' is no duration of days, hours, minutes and seconds (PT8H, P1D)`);
  }
  const [, d = '0', h = '0', min = '0', s = '0'] = m;
  return ((Number(d) * 24 + Number(h)) * 60 + Number(min)) * 60_000 + Number(s) * 1000;
}

/** The door's topics, as the model's topics say each is kept. */
export function doorTopics(topics: readonly StreamTopic[]): DoorTopic[] {
  return topics.map((t) => ({
    topic: t.topic,
    compacted: t.key.length > 0,
    keepRows: t.keepRows,
    keepForMs: t.keepFor === null ? null : durationMs(t.keepFor),
  }));
}

/** What the feed has written: each topic's records, and a compacted one's keys. */
export interface FeedCounts {
  readonly records: Record<string, number>;
  readonly keys: Record<string, number>;
}

/**
 * The world's sink writing into `door`: each table a topic is made of, its rows that topic's records; any other table
 * written by no one here (its rows are B3's). `clock` the records' time (ms). The records the sink has written are
 * counted by topic, and a compacted topic's keys.
 */
export function doorSink(door: KafkaDoor, topics: readonly StreamTopic[], clock: () => number = Date.now)
  : { readonly sink: MarketSink; readonly counts: FeedCounts } {
  const byTable = new Map(topics.map((t) => [t.table, t]));
  const records: Record<string, number> = {};
  const keys = new Map<string, Set<string>>();
  const sink: MarketSink = (table, count, rows) => {
    const t = byTable.get(table.name);
    if (t === undefined) return Promise.resolve(0);
    const ts = clock();
    const seen = t.key.length > 0 ? (keys.get(t.topic) ?? keys.set(t.topic, new Set()).get(t.topic)!) : undefined;
    for (let i = 0; i < count; i++) {
      const record: Record<string, unknown> = {};
      for (const c of t.columns) record[c.name] = valueOf(rows[c.name], i);
      const key = seen === undefined ? null : JSON.stringify(t.key.map((k) => record[k] ?? null));
      if (key !== null) seen!.add(key);
      door.append(t.topic, ts, key, JSON.stringify(record));
    }
    records[t.topic] = (records[t.topic] ?? 0) + count;
    door.arrived();
    return Promise.resolve(count);
  };
  return {
    sink,
    counts: {
      records,
      get keys(): Record<string, number> {
        return Object.fromEntries([...keys].map(([k, v]) => [k, v.size]));
      },
    },
  };
}

/** A row's value of a column as its record holds it: null where the row has none. */
function valueOf(column: MadeColumn | undefined, row: number): string | number | boolean | null {
  if (column === undefined) return null;
  if ('doubles' in column) {
    const v = column.doubles(row);
    return v === null || !Number.isFinite(v) ? null : v;
  }
  if ('integers' in column) return column.integers(row);
  if ('text' in column) {
    const k = column.pick(row);
    return k === null ? null : column.text[k] ?? null;
  }
  if ('instants' in column) return column.instants(row);
  if ('truths' in column) return column.truths(row);
  const d = column.days(row);
  return d === null ? null : new Date(d * 86_400_000).toISOString().slice(0, 10);
}
