// Projects opened by name (design Phase 3, plan A0½): each project a Depot knows, at its project line's snapshot --
// Depot's `master-SNAPSHOT`, upstream Query's HEAD: what Studio's reviews have committed, released or not -- its
// model the version's files and its dependencies' (Depot's nearest-wins closure, the one Studio compiles against),
// loaded as the demo's files are. A project with no line (nothing committed yet) is not offered.

import type { DepotClient } from '../../../depot-client/src/client.ts';
import { SNAPSHOT } from '../../../depot-client/src/wire.ts';
import type { Grammar } from '../backend/engine.ts';
import { ModelGraph } from '../model/graph.ts';
import { gavOf, type LoadedProject, type ProjectConfig } from './context.ts';

/** A version's model text: every file of it and its dependency closure, each from the Pure section (`###Pure`). */
export async function modelText(depot: DepotClient, groupId: string, artifactId: string, versionId: string): Promise<string> {
  const closure = await depot.dependencyFiles([{ groupId, artifactId, versionId }]);
  return closure.flatMap((v) => v.files.map((f) => `###Pure\n${f.pureCode}`)).join('\n');
}

/** Every project the Depot has with a snapshot, loaded at it. */
export async function projectsByName(depot: DepotClient, grammar: Grammar): Promise<LoadedProject[]> {
  const out: LoadedProject[] = [];
  for (const p of await depot.projects()) {
    if (!(await depot.versions(p.groupId, p.artifactId, true)).includes(SNAPSHOT)) continue;
    const code = await modelText(depot, p.groupId, p.artifactId, SNAPSHOT);
    const config: ProjectConfig = { groupId: p.groupId, artifactId: p.artifactId, versionId: SNAPSHOT, title: p.artifactId, models: [] };
    out.push({ config, gav: gavOf(config), context: { _type: 'text', code }, graph: new ModelGraph(await grammar.modelJson(code)) });
  }
  return out;
}
