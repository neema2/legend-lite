# A Kafka door Deephaven reads (spike, 2026-10-29)

Scratch evidence for docs/DEEPHAVEN_TABLES_AND_SOURCES_2026_10_29.md §A.5, not product code. The question (the user,
2026-10-29): instead of running someone else's broker (Redpanda, Tansu) for the demo's streams, could we put a Kafka
front door on our own simulated world?

`kafka-door.mjs` is about 200 lines of Node with no libraries. It is a simulated world (100 names, quotes as a random walk,
`RATE` a second, in 10 ms ticks) and a Kafka endpoint that serves the world's quotes as one topic, `quotes`, one JSON
record per quote. It answers only the four requests Deephaven's Kafka reader sends, one version of each:

| Request | Version | What for |
|---|---|---|
| ApiVersions | 3-4 | the client asks what the server speaks |
| Metadata | 8 | the topic's partitions and who leads them |
| ListOffsets | 5 | "the first record" and "the last record", for seek to beginning or end |
| Fetch | 11 | the records from an offset on, as one record batch (Kafka's format v2, CRC-32C), waiting up to the client's wait when there are none |

Deephaven's reader needs no more because it reads by assigning partitions and seeking, never through a consumer group:
`io.deephaven.kafka.ingest.KafkaIngester` calls `assign`, `partitionsFor`, `seek`, `seekToBeginning`, `seekToEnd`, `poll`
(read from its bytecode in the pinned 42.7). The client is Deephaven's own `kafka-clients-8.3.2-ccs`; its lowest versions
(read from the jar) are Fetch 4, ListOffsets 1, Metadata 0.

`probe.groovy` runs inside the pinned Deephaven (the market fixture with this application instead of the market's): it
consumes the topic with `KafkaTools.consumeToTable` (JSON, append, from the beginning), waits 15 s, notes the last
sequence number, waits 20 s more, then writes whether every quote arrived exactly once and how late the ones after the
mark arrived (`now()` when the row lands, less the record's timestamp).

## Run it

```sh
bazel build //datacube:deephaven_market_fixture
cd experiments/kafka-door-spike
SECONDS=75 RATE=2000 node kafka-door.mjs &
sleep 85 | PROBE_OUT=/tmp/probe.out TEST_TMPDIR=/tmp/dh \
  ../../bazel-bin/datacube/deephaven_market_fixture \
  --jvm_flag=-Dembedded.deephaven.application=$PWD/probe.app
cat /tmp/probe.out
```

`PORT` (the door's, default 19092; the probe reads `SHIM_PORT`), `RATE`, `SECONDS` and `KEEP` (records held, default
2,000,000) are environment variables. `--jvm_flag=-Dembedded.deephaven.cycleMs=10` sets Deephaven's update cycle (the
fixture's default is 100 ms).

## Measured (2026-10-29, a cloud session's container)

| Quotes a second | Deephaven's cycle | Every quote, once, in order? | Late by, p50 / p99 / max | The door's CPU, memory |
|---|---|---|---|---|
| 2,000 | 100 ms | yes, 143,660 of 143,660 | 53 / 103 / 143 ms | 6% of a core, 134 MB |
| 20,000 | 100 ms | yes, 879,000 of 879,000 | 55 / 104 / 151 ms | 19% of a core, 461 MB (1.4 M records held) |
| 2,000 | 10 ms | yes, 89,620 of 89,620 | 7 / 14 / 61 ms | 7% of a core, 130 MB |

How late a quote is comes from Deephaven's cycle, not the door: about half a cycle on average, a cycle at worst. 2,000
quotes a second is the desk world's rate (docs/DUCKDB_LIVE_2026_10_25.md §2); 20,000 is ten times it. Deephaven took 3 to
5 s to start reading after its script asked (the Kafka client's start-up, not measured against a real broker).

## What it is not

- Not a broker: nothing can write into it from outside (no Produce), no consumer groups, one partition, one topic.
- Not tested against any client but Deephaven's.
- Not checked against a real broker side by side: running Tansu or Redpanda here needs the user's go-ahead (a downloaded
  binary).
