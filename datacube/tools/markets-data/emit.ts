// THE MARKETS PROJECT'S DATA (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §11, Phase 6): the markets sample's days in
// the trading model's own tables, as a `###Data` element (stress-markets.ts stressMarketsData) -- written to stdout, the
// project's second file beside the trading model (BUILD.bazel :markets_project), which a page fills its project's
// database from (C5).

import { stressMarketsData } from '../../src/stress-markets.ts';

process.stdout.write(stressMarketsData());
