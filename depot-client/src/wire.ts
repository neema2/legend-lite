// Upstream legend-depot's read API, as it writes its records (studio/docs/DEPOT_CONTRACT.md), plus
// lite's text twin for dependency files. The contract every Depot here meets -- legend-depot defines it.

import type { Entity } from '../../sdlc-client/src/wire.ts';

/** `StoreProjectData`: a project Depot knows, in its wire order. */
export interface StoreProjectData {
  readonly groupId: string;
  readonly artifactId: string;
  readonly defaultBranch: string | null;
  readonly projectId: string;
  readonly latestVersion: string | null;
}

/** `ArtifactDependency`: a version asked for (`versionId` wins over `version`). */
export interface ArtifactDependency {
  readonly groupId: string;
  readonly artifactId: string;
  readonly versionId: string;
  readonly exclusions?: readonly { readonly groupId: string; readonly artifactId: string }[];
}

/** `ProjectVersionEntities`: one version's entities in a dependency answer. */
export interface ProjectVersionEntities {
  readonly groupId: string;
  readonly artifactId: string;
  readonly versionId: string;
  readonly versionedEntity: boolean;
  readonly entities: readonly Entity[];
}

/** lite's text twin: one version's files (`POST /projects/dependencies/pure`). */
export interface ProjectVersionFiles {
  readonly groupId: string;
  readonly artifactId: string;
  readonly versionId: string;
  readonly files: readonly { readonly path: string; readonly pureCode: string }[];
}

/** `ProjectVersionDTO` (`GET /versions/{g}/{a}/{v}`). */
export interface ProjectVersion {
  readonly groupId: string;
  readonly artifactId: string;
  readonly versionId: string;
  readonly versionData: {
    readonly dependencies: readonly { readonly groupId: string; readonly artifactId: string; readonly versionId: string }[];
  };
}

/** The project line's snapshot, as Depot names it (upstream's `head`). */
export const SNAPSHOT = 'master-SNAPSHOT';
