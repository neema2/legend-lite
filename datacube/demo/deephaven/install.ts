// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

// INSTALLING A DEPLOYMENT ON A DEEPHAVEN SERVER (docs/DEPLOYMENT_DESIGN_2026_11_14.md §9, step 3): a server's start is an
// install on an empty server. The deployment's plan for its live target (the planner's `deephaven` request, op `plan`:
// data, never code) is written beside lite's one fixed loader (core/src/main/deephaven/legend-loader.groovy), and the
// server's application runs the loader, which makes each table the plan lists and checks it against the plan before
// publishing it -- a table not as declared fails the start, naming it. Node alone (node:fs).

import { copyFileSync, mkdirSync, writeFileSync } from 'node:fs';
import { basename, join } from 'node:path';

/** A deployment's plan for a Deephaven server, as the planner answers it (`DeephavenLoad.Plan.data`). */
export interface DeephavenPlan {
  readonly kind: 'legend.deephaven.load';
  readonly version: number;
  readonly deployment: string;
  readonly tables: readonly { readonly name: string; readonly topic: string; readonly keep: { readonly kind: string } }[];
}

/** The plan the planner answered (its `deephaven` request, op `plan`: `OK` and the JSON, or `ERR` and why). */
export function planOf(answer: string): DeephavenPlan {
  if (!answer.startsWith('OK\n')) {
    throw new Error(`the planner refused the deployment: ${answer.split('\n').slice(2).join('\n') || answer}`);
  }
  return (JSON.parse(answer.slice(3)) as { readonly plan: DeephavenPlan }).plan;
}

/** What an install wrote: the application file a server runs, and the environment it runs with. */
export interface Installed {
  readonly app: string;
  readonly env: Readonly<Record<string, string>>;
}

/**
 * The deployment `plan` installed in `dir` for a Deephaven server's application mode: the plan, lite's `loader` (its
 * path) and the application file naming it first -- then `after`, files the same application runs once the tables are
 * made (a test's check). The server is started with the returned environment.
 */
export function installDeephaven(dir: string, plan: DeephavenPlan, loader: string, after: readonly string[] = []): Installed {
  mkdirSync(dir, { recursive: true });
  const planFile = join(dir, 'plan.json');
  writeFileSync(planFile, JSON.stringify(plan));
  copyFileSync(loader, join(dir, 'legend-loader.groovy'));
  const files = ['legend-loader.groovy'];
  for (const f of after) {
    copyFileSync(f, join(dir, basename(f)));
    files.push(basename(f));
  }
  const app = join(dir, 'legend.app');
  writeFileSync(app, ['type=script', 'scriptType=groovy', 'enabled=true', 'id=legend.deployment',
    `name=${plan.deployment}, landed by legend-lite's loader`, ...files.map((f, i) => `file_${i}=${f}`), ''].join('\n'));
  return { app, env: { LEGEND_DEEPHAVEN_PLAN: planFile } };
}
