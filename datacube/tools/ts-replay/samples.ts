// THE GALLERY'S SAMPLES FOR THE REPLAY (docs/TS_DECISIONS_2026_11_07.md item 9): the three samples the gallery and the
// markets example read -- the prices, the rates and the macro series, as the pages make them (samples.ts) -- written to
// stdout as one JSON object of their CSV texts, which //core:ts_replay_test loads into each database it replays every
// series' Show #TS statement on.

import { macroCsv, marketsCsv, ratesCsv } from '../../src/samples.ts';

process.stdout.write(JSON.stringify({ prices: marketsCsv(), rates: ratesCsv(), macro: macroCsv() }));
