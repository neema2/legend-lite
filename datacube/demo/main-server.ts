// The demo against a RUNNING legend-lite, over HTTP.
//
// The other entry point, main.ts, is the default: it plans in the tab
// and needs nothing running. This one exists because the server plane
// is the thing the browser plane has to keep agreeing with, and an
// agreement nobody exercises is a claim, not a fact.
//
//   npm run engine        # in one terminal
//   npm run build:demo    # then open demo/index-server.html

import {
  boot,
  LEGEND_LITE,
  loadModel,
  must,
  RUNTIME,
  SNAP_TARGET,
  SOURCE,
  type Engine,
} from './boot.ts';
import { LegendLitePlanner } from '../src/planner.ts';

/**
 * The planner. There is exactly one, and it is the real one.
 *
 * This used to CHOOSE between legend-lite and a demo-only shim
 * depending on whether the server answered a health check, and that
 * one line of convenience hid three real bugs for the whole life of
 * the project: snap building SQL by hand, the snapped plane never
 * redirecting, and the "Generated SQL" panel showing Pure. Every one
 * of them was invisible because the shim's source was a bare SQL
 * identifier and the broken path happened to work against it.
 *
 * So there is no fallback. A test may INJECT a stub planner -- that
 * choice is static, in code that never ships -- but the product
 * cannot select one at runtime. If legend-lite is not running, this
 * throws and the page says so; it does not quietly show fake numbers
 * that look exactly like real ones.
 */
export async function requireEngine(_status: HTMLElement): Promise<Engine> {
  // The model is fetched rather than inlined so the SAME text is what
  // the planner compiles and what a reader opens -- one copy, in
  // demo/trades.pure.
  const model = await loadModel();

  let reachable = false;
  try {
    const health = await fetch(`${LEGEND_LITE}/health`, {
      signal: AbortSignal.timeout(1500),
    });
    reachable = health.ok;
  } catch {
    reachable = false;
  }
  if (!reachable) {
    must('plannermissing').hidden = false;
    throw new Error(
      `legend-lite is not answering on ${LEGEND_LITE}. ` +
        'Start it with `npm run engine` and reload, or open' +
        ' index.html, the default, which plans in the browser' +
        ' with no server at all.',
    );
  }

  return {
    planner: new LegendLitePlanner({
      baseUrl: LEGEND_LITE,
      model,
      runtime: RUNTIME,
    }),
    source: SOURCE,
    snapTarget: SNAP_TARGET,
    label: 'planner: legend-lite (server)',
  };
}

void boot(requireEngine).catch((e) => {
  const s = document.getElementById('status');
  if (s) {
    s.textContent = `failed to start: ${e instanceof Error ? e.message : e}`;
    s.classList.add('bad');
  }
});
