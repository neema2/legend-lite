// Projects opened by name (design Phase 3, plan A0½): each project a Depot knows, at its project line's snapshot --
// Depot's `master-SNAPSHOT`, upstream Query's HEAD: what Studio's reviews have committed, released or not -- its
// model the version's files and its dependencies' (Depot's nearest-wins closure, the one Studio compiles against),
// loaded as the demo's files are. A project with no line (nothing committed yet) is not offered.

import type { DepotClient } from '../../../depot-client/src/client.ts';
import { modelText } from '../../../depot-client/src/model-text.ts';
import { SNAPSHOT } from '../../../depot-client/src/wire.ts';
import type { Grammar } from '../backend/engine.ts';
import { ModelGraph } from '../model/graph.ts';
import { gavOf, type LoadedProject, type ProjectConfig } from './context.ts';

/** One version of a project, loaded: its model text and graph, as the demo's files are. */
export async function loadByName(depot: DepotClient, grammar: Grammar, groupId: string, artifactId: string, versionId: string): Promise<LoadedProject> {
  const code = await modelText(depot, groupId, artifactId, versionId);
  const config: ProjectConfig = { groupId, artifactId, versionId, title: artifactId, models: [] };
  return { config, gav: gavOf(config), context: { _type: 'text', code }, graph: new ModelGraph(await grammar.modelJson(code)) };
}

/** Every project the Depot has with a snapshot, loaded at it (what the start page lists). */
export async function projectsByName(depot: DepotClient, grammar: Grammar): Promise<LoadedProject[]> {
  const out: LoadedProject[] = [];
  for (const p of await depot.projects()) {
    if (!(await depot.versions(p.groupId, p.artifactId, true)).includes(SNAPSHOT)) continue;
    out.push(await loadByName(depot, grammar, p.groupId, p.artifactId, SNAPSHOT));
  }
  return out;
}

/** A project's versions as the picker offers them: HEAD (the snapshot) first, then releases, newest first. */
export async function versionsOf(depot: DepotClient, groupId: string, artifactId: string): Promise<string[]> {
  const all = await depot.versions(groupId, artifactId, true);
  return [...all.filter((v) => v === SNAPSHOT), ...all.filter((v) => v !== SNAPSHOT).reverse()];
}
