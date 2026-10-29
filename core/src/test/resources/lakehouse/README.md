# Legend's ingest JSON, as test input

Copied from legend-studio (Apache License 2.0, Copyright Goldman Sachs) at `18a8f8f` (2026-10-29), the ingest
elements' `content` only, sourced from
`packages/legend-graph/src/graph-manager/__tests__/roundtripTestData/`:

- `studio-ingests.json`: `TEST_DATA__IngestIncludeTransitiveRoundtrip.ts`, its two `ingestDefinition`s (engine-shaped:
  every key sorted, every list present);
- `studio-ingest-minimal.json`: `TEST_DATA__IngestRoundtrip.ts`, `TEST_DATA__INGEST_DEFINITION` (written by hand in
  Studio's test: its lists left out).

Read by `com.legend.protocol.LakehouseIngestTest`: Legend's closed grammar has no public parser, so its JSON is the
spec lite's reader and writer are held to (docs/DEEPHAVEN_TABLES_AND_SOURCES_2026_10_29.md §A.4).
