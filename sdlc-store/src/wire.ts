// Upstream's SDLC records, as legend-sdlc's REST API writes them (legend-sdlc-model; census
// studio/docs/UPSTREAM_STUDIO_CENSUS.md Part C §1), plus lite's text routes (design S15). The
// contract every SDLC store here meets -- legend-sdlc defines the upstream half, nothing here owns it.

/** `Entity`: one element, its protocol JSON as `content` (which carries `package` and `name`). */
export interface Entity {
  readonly path: string;
  readonly classifierPath: string;
  readonly content: Readonly<Record<string, unknown>>;
}

export type EntityChangeType = 'CREATE' | 'DELETE' | 'MODIFY' | 'RENAME';

/** `EntityChange`: one change in an `entityChanges` save. */
export interface EntityChange {
  readonly type: EntityChangeType;
  readonly entityPath: string;
  readonly classifierPath?: string | null;
  readonly content?: Readonly<Record<string, unknown>> | null;
  readonly newEntityPath?: string | null;
}

/** `PerformChangesCommand`: `revisionId` is the optimistic lock (null: no check). */
export interface PerformChangesCommand {
  readonly message: string;
  readonly entityChanges: readonly EntityChange[];
  readonly revisionId?: string | null;
}

export type ProjectType = 'PRODUCTION' | 'PROTOTYPE' | 'MANAGED' | 'EMBEDDED';

export interface Project {
  readonly projectId: string;
  readonly name: string;
  readonly description: string | null;
  readonly tags: readonly string[] | null;
  readonly projectType: ProjectType | null;
  readonly webUrl: string | null;
}

export interface CreateProjectCommand {
  readonly name: string;
  readonly description: string;
  readonly type?: ProjectType | null;
  readonly groupId: string;
  readonly artifactId: string;
  readonly tags?: readonly string[] | null;
}

/** `Workspace`: `userId` is null exactly for a group workspace (Studio derives the type from it). */
export interface Workspace {
  readonly projectId: string;
  readonly userId: string | null;
  readonly workspaceId: string;
}

/** `Revision`: instants are ISO-8601 strings. */
export interface Revision {
  readonly id: string;
  readonly authorName: string;
  readonly authoredTimestamp: string;
  readonly committerName: string;
  readonly committedTimestamp: string;
  readonly message: string;
}

/** The aliases a revision id may be (case-insensitive), else a literal id. */
export type RevisionAlias = 'BASE' | 'HEAD' | 'CURRENT' | 'LATEST';

export interface User {
  readonly userId: string;
  readonly name: string;
}

export interface ProjectStructureVersion {
  readonly version: number;
  readonly extensionVersion?: number | null;
}

export interface ProjectDependency {
  /** `groupId:artifactId`. */
  readonly projectId: string;
  /** `M.m.p`. */
  readonly versionId: string;
  readonly exclusions?: readonly { readonly projectId: string }[] | null;
}

/** `ProjectConfiguration`, the `/project.json` file. */
export interface ProjectConfiguration {
  readonly projectId: string;
  readonly projectType?: 'MANAGED' | 'EMBEDDED' | null;
  readonly projectStructureVersion: ProjectStructureVersion;
  readonly platformConfigurations?: readonly { readonly name: string; readonly version: string }[] | null;
  readonly groupId: string;
  readonly artifactId: string;
  readonly projectDependencies: readonly ProjectDependency[];
  readonly metamodelDependencies?: readonly { readonly metamodel: string; readonly version: number }[];
  readonly runDependencyTests?: boolean | null;
  readonly produceShadedServiceJar?: boolean | null;
}

/** `ExtendedErrorMessage`: what every refusal answers. */
export interface ErrorMessage {
  readonly code: number;
  readonly message: string;
  readonly details?: string | null;
  readonly timestamp?: string;
}

// ---- lite's text routes (design S15): beside upstream's, which stay untouched ----

/** One element's file: its path and its text (one element, its imports and comments). */
export interface PureFile {
  readonly path: string;
  readonly pureCode: string;
}

export type PureChangeType = 'CREATE' | 'MODIFY' | 'DELETE';

/** One change in a `pureChanges` save: a rename is DELETE + CREATE. */
export interface PureChange {
  readonly type: PureChangeType;
  readonly path: string;
  /** CREATE and MODIFY: the whole file. */
  readonly pureCode?: string | null;
}

/** `POST …/pureChanges`: the same lock and answer as `entityChanges`. */
export interface PerformPureChangesCommand {
  readonly message: string;
  readonly changes: readonly PureChange[];
  readonly revisionId?: string | null;
}
