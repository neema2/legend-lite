// Projects opened by name (design Phase 3, plan A0½): a version of a project a Depot knows -- by default its line's snapshot,
// Depot's `master-SNAPSHOT`, upstream Query's HEAD: what Studio's reviews have committed, released or not -- its
// model the version's files and its dependencies' (Depot's nearest-wins closure, the one Studio compiles against),
// loaded as the demo's files are, the first time it is opened (AppContext.load).

import type { DepotClient } from '../../../depot-client/src/client.ts';
import { modelText } from '../../../depot-client/src/model-text.ts';
import { dataTables } from '../../../engine-client/src/model-data.ts';
import { SNAPSHOT } from '../../../depot-client/src/wire.ts';
import type { Grammar } from '../backend/engine.ts';
import { ModelGraph } from '../model/graph.ts';
import { gavOf, type LoadedProject, type ProjectConfig } from './context.ts';

/**
 * One version of a project, loaded: its model text and graph, as the demo's files are -- and, where queries run in
 * this tab (`tabData`), the model's own test data (plan A2: its relational Data elements' tables, made as its Databases
 * declare), which AppContext.activate makes the tab's rows each time this version is opened.
 */
export async function loadByName(depot: DepotClient, grammar: Grammar, groupId: string, artifactId: string, versionId: string,
  tabData: boolean): Promise<LoadedProject> {
  const code = await modelText(depot, groupId, artifactId, versionId);
  const pmcd = await grammar.modelJson(code);
  const config: ProjectConfig = { groupId, artifactId, versionId, title: artifactId, models: [] };
  return {
    config, gav: gavOf(config), context: { _type: 'text', code }, graph: new ModelGraph(pmcd),
    ...(tabData ? { tables: dataTables(pmcd.elements) } : {}),
  };
}

/** A project's versions as the picker offers them: HEAD (the snapshot) first, then releases, newest first. */
export async function versionsOf(depot: DepotClient, groupId: string, artifactId: string): Promise<string[]> {
  const all = await depot.versions(groupId, artifactId, true);
  return [...all.filter((v) => v === SNAPSHOT), ...all.filter((v) => v !== SNAPSHOT).reverse()];
}
