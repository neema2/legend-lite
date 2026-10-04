// The dogfood model (design S18): four small projects with real dependencies -- types; party and
// instruments on different versions of it; trading on both (a diamond: nearest wins) -- published the
// way a person publishes: a workspace, a save, a review, a commit, a version. Every release goes through
// the SDLC's compile gate, so loading them proves the model compiles with its dependencies.

import type { SdlcClient } from '../../../sdlc-client/src/client.ts';
import type { NewVersionType } from '../../../sdlc-client/src/wire.ts';
import type { Compiler } from '../backend/planner.ts';

export interface Manifest {
  readonly groupId: string;
  readonly steps: readonly {
    readonly artifactId: string;
    readonly name?: string;
    readonly message: string;
    readonly dependencies?: readonly (readonly [string, string])[];
    readonly files: readonly string[];
    readonly release: NewVersionType;
  }[];
}

/**
 * Publishes the manifest's steps in order. `read` answers a manifest-relative file's text. A step whose
 * version already exists is skipped, so loading twice changes nothing.
 */
export async function loadDemoProjects(client: SdlcClient, compiler: Compiler, manifest: Manifest,
  read: (file: string) => Promise<string>, progress: (message: string) => void = () => undefined): Promise<void> {
  const done = new Map<string, number>();
  for (const step of manifest.steps) {
    const p = `${manifest.groupId}:${step.artifactId}`;
    const n = (done.get(p) ?? 0) + 1;
    done.set(p, n);
    const exists = (await client.projects()).some((x) => x.projectId === p);
    if (!exists) await client.createProject({ name: step.name ?? step.artifactId, description: 'The Legend Studio demo (design S18)', groupId: manifest.groupId, artifactId: step.artifactId });
    if ((await client.versions(p)).length >= n) continue;
    progress(`${p}: ${step.message}`);
    const workspace = `demo-${n}`;
    await client.createWorkspace(p, workspace);
    if (step.dependencies?.length) {
      await client.updateConfiguration(p, workspace, {
        message: 'dependencies',
        projectDependenciesToAdd: step.dependencies.map(([artifactId, versionId]) => ({ projectId: `${manifest.groupId}:${artifactId}`, versionId })),
      });
    }
    const existing = new Set((await client.pure({ project: p, workspace })).map((f) => f.path));
    const changes = [];
    for (const file of step.files) {
      const pureCode = await read(file);
      const [element] = await compiler.elements(pureCode);
      if (!element) throw new Error(`${file} declares no element`);
      changes.push({ type: existing.has(element.path) ? 'MODIFY' as const : 'CREATE' as const, path: element.path, pureCode });
    }
    await client.performPureChanges(p, workspace, { message: step.message, changes });
    const review = await client.createReview(p, { workspaceId: workspace, workspaceType: 'USER', title: step.message, description: 'demo' });
    await client.commitReview(p, review.id, `${step.message} [review]`);
    await client.createVersion(p, { versionType: step.release, notes: step.message });
  }
}
