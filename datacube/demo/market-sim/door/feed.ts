// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

// THE FEED (docs/DESK_DATA_ROADMAP_2026_10_29.md B1): the market world behind the door -- the desk model's topics as the
// planner answers them (the `deephaven` request, op `topics`, for each database whose streams the desk reads), the door
// holding each as its dataset lands, and the world writing every table a stream reads through it. Deephaven reads the
// topics with its own Kafka reader; the rows never pass through anything else. Node alone (the door's node:net).

import type { MarketSettings } from '../market.ts';
import { MarketWorld } from '../market.ts';
import { doorSink, doorTopics, topicsOf, type FeedCounts, type StreamTopic } from './door-feed.ts';
import { KafkaDoor } from './kafka-door.ts';

/** The databases whose streams the desk reads: the corpus's store and the world's own status. */
export const FEED_DATABASES: readonly string[] = ['store::DB', 'market::World'];

export interface FeedOptions {
  /** The desk's whole model (model.ts DESK_MODEL): the ingests and their topics' stores. */
  readonly model: string;
  /** The planner over it: its `deephaven` request answers the topics. */
  readonly planner: { deephaven(op: string, args: Readonly<Record<string, unknown>>): Promise<string> };
  /** The door's port on the loopback (0: any free one). */
  readonly port?: number;
  /** The world's size and pace. */
  readonly settings?: MarketSettings;
  /** Where a refusal of the door's is said. */
  readonly say?: (message: string) => void;
}

export interface Feed {
  readonly port: number;
  readonly topics: readonly StreamTopic[];
  readonly door: KafkaDoor;
  readonly world: MarketWorld;
  /** The records written, by topic, and a compacted topic's keys. */
  readonly counts: FeedCounts;
  /** The world stopped, its last rows written: the door stays open, for its readers to finish. */
  stopWorld(): Promise<void>;
  /** The world stopped and the door closed. */
  close(): Promise<void>;
}

export async function startFeed(o: FeedOptions): Promise<Feed> {
  const topics: StreamTopic[] = [];
  for (const database of FEED_DATABASES) {
    topics.push(...topicsOf(await o.planner.deephaven('topics', { model: o.model, database })));
  }
  const door = new KafkaDoor(doorTopics(topics), o.say);
  const port = await door.listen(o.port ?? 0);
  const { sink, counts } = doorSink(door, topics);
  const world = await MarketWorld.open(o.settings ?? {}, sink);
  let stopped: Promise<void> | null = null;
  const stopWorld = (): Promise<void> => (stopped ??= (async () => {
    world.close();
    await world.flush();
  })());
  return {
    port, topics, door, world, counts, stopWorld,
    close: async () => {
      await stopWorld();
      await door.close();
    },
  };
}
