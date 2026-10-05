// //datacube:verify_remote_test (Bazel workplan P4-03): the two remote-data harnesses as one test in two shards --
// shard 0 a Parquet over HTTP with Range, in the remote harness page and in the app (verify-remote.mjs); shard 1 a
// real Parquet read by DuckDB-WASM's httpfs, totals against DuckDB's (verify-real-data.mjs). Each builds its own
// fixture in its temp directory and serves on port 0; under a test neither reads DATA, FORMAT or EXPECT.
import { writeFile } from 'node:fs/promises';

const shard = Number(process.env.TEST_SHARD_INDEX ?? 0);
if (process.env.TEST_SHARD_STATUS_FILE) await writeFile(process.env.TEST_SHARD_STATUS_FILE, '');
for (const knob of ['DATA', 'PARQUET', 'FORMAT', 'EXPECT']) delete process.env[knob];
await import(shard === 0 ? './verify-remote.mjs' : './verify-real-data.mjs');
