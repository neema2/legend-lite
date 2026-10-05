// A version's model as text, for an app that opens a project by name (design Phase 3: Query, DataCube): every file of
// the version and of its dependency closure (Depot's nearest-wins, the one Studio compiles against), in one call, each
// entering the Pure section (`###Pure`) as Studio's compile joins them.

import type { DepotClient } from './client.ts';

export async function modelText(depot: DepotClient, groupId: string, artifactId: string, versionId: string): Promise<string> {
  const closure = await depot.dependencyFiles([{ groupId, artifactId, versionId }]);
  return closure.flatMap((v) => v.files.map((f) => `###Pure\n${f.pureCode}`)).join('\n');
}
