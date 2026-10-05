// THE ONE CLIENT of an SDLC: upstream legend-sdlc's REST API and lite's text routes (design S15),
// typed. Studio talks to its projects through this and nothing else; WHERE the SDLC is -- sdlc-server,
// a real legend-sdlc, or this page (wasm-server.ts) -- is only the `fetch` and root
// it is handed, never a branch in an app (design S21).

import type {
  CreateProjectCommand, CreateReviewCommand, CreateVersionCommand, Entity, ErrorMessage, PerformChangesCommand,
  PerformPureChangesCommand, Project, ProjectConfiguration, PureChange, PureFile, Review, ReviewState, Revision,
  UpdateProjectConfigurationCommand, User, Version, Workspace, WorkspaceUpdateReport,
} from './wire.ts';

/** An SDLC's refusal, as it said it: its status and its own words. */
export class SdlcError extends Error {
  readonly status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = 'SdlcError';
    this.status = status;
  }
}

/**
 * Where a read is: a project's line, or one of its workspaces; at their current revision, or at
 * `revision` (an id or an alias, `BASE`/`HEAD`/`CURRENT`/`LATEST`).
 */
export interface Where {
  readonly project: string;
  readonly workspace?: string;
  readonly revision?: string;
}

/** The entity filters every `entities` route takes (all optional). */
export interface EntityFilter {
  readonly classifierPath?: readonly string[];
  readonly package?: readonly string[];
  readonly includeSubPackages?: boolean;
  readonly name?: string;
  readonly stereotype?: readonly string[];
  readonly taggedValue?: readonly string[];
  readonly excludeInvalid?: boolean;
}

export interface ProjectSearch {
  readonly search?: string;
  readonly user?: boolean;
  readonly tag?: readonly string[];
  readonly excludeTag?: readonly string[];
  readonly limit?: number;
}

const enc = encodeURIComponent;

/** `/projects/{p}[/workspaces/{w}][/revisions/{r}]`. */
function base(w: Where): string {
  let path = `/projects/${enc(w.project)}`;
  if (w.workspace !== undefined) path += `/workspaces/${enc(w.workspace)}`;
  if (w.revision !== undefined) path += `/revisions/${enc(w.revision)}`;
  return path;
}

function query(params: Readonly<Record<string, string | number | boolean | readonly string[] | undefined>>): string {
  const parts: string[] = [];
  for (const [k, v] of Object.entries(params)) {
    if (v === undefined) continue;
    for (const one of Array.isArray(v) ? v : [v]) parts.push(`${enc(k)}=${enc(String(one))}`);
  }
  return parts.length === 0 ? '' : `?${parts.join('&')}`;
}

/**
 * The SDLC at `api` -- its API root, `http://host:port/sdlc/api` -- through `fetcher`: the network,
 * or the page's own SDLC (`(await wasmSdlcServer(...)).fetch`).
 */
export class SdlcClient {
  readonly #api: string;
  readonly #fetch: typeof fetch;

  constructor(api: string, fetcher: typeof fetch = globalThis.fetch.bind(globalThis)) {
    this.#api = api.replace(/\/+$/, '');
    this.#fetch = fetcher;
  }

  // ---- who and what ----

  currentUser(): Promise<User> {
    return this.#json('GET', '/currentUser');
  }

  authorized(): Promise<boolean> {
    return this.#json('GET', '/auth/authorized');
  }

  // ---- projects ----

  projects(search: ProjectSearch = {}): Promise<Project[]> {
    return this.#json('GET', `/projects${query({ ...search })}`);
  }

  project(id: string): Promise<Project> {
    return this.#json('GET', `/projects/${enc(id)}`);
  }

  createProject(command: CreateProjectCommand): Promise<Project> {
    return this.#json('POST', '/projects', command);
  }

  configuration(where: Where): Promise<ProjectConfiguration> {
    return this.#json('GET', `${base(where)}/configuration`);
  }

  // ---- workspaces ----

  workspaces(project: string): Promise<Workspace[]> {
    return this.#json('GET', `/projects/${enc(project)}/workspaces`);
  }

  workspace(project: string, workspace: string): Promise<Workspace> {
    return this.#json('GET', base({ project, workspace }));
  }

  createWorkspace(project: string, workspace: string): Promise<Workspace> {
    return this.#json('POST', base({ project, workspace }));
  }

  async deleteWorkspace(project: string, workspace: string): Promise<void> {
    await this.#call('DELETE', base({ project, workspace }));
  }

  /** Whether the project line has moved since the workspace was made from it. */
  outdated(project: string, workspace: string): Promise<boolean> {
    return this.#json('GET', `${base({ project, workspace })}/outdated`);
  }

  /** `POST …/update`: the workspace rebased onto the project line's head (upstream's WorkspaceUpdateReport). */
  updateWorkspace(project: string, workspace: string): Promise<WorkspaceUpdateReport> {
    return this.#json('POST', `${base({ project, workspace })}/update`);
  }

  inConflictResolutionMode(project: string, workspace: string): Promise<boolean> {
    return this.#json('GET', `${base({ project, workspace })}/inConflictResolutionMode`);
  }

  // ---- conflict resolution (upstream's, made by a CONFLICT update) ----

  /** The resolution's files: the line's head with the workspace's changes over it, as resolved so far. */
  conflictResolutionPure(project: string, workspace: string): Promise<PureFile[]> {
    return this.#json('GET', `${base({ project, workspace })}/conflictResolution/pure`);
  }

  /** Has the project line moved on since the resolution was made from it? */
  conflictResolutionOutdated(project: string, workspace: string): Promise<boolean> {
    return this.#json('GET', `${base({ project, workspace })}/conflictResolution/outdated`);
  }

  /** `POST …/conflictResolution/accept`: the resolution, with these text changes, becomes the workspace (lite's text form). */
  async acceptConflictResolution(project: string, workspace: string, command: { readonly message: string; readonly changes: readonly PureChange[] }): Promise<void> {
    await this.#call('POST', `${base({ project, workspace })}/conflictResolution/accept`, command);
  }

  /** `DELETE …/conflictResolution`: the resolution dropped; the workspace stays as it was. */
  async discardConflictResolution(project: string, workspace: string): Promise<void> {
    await this.#call('DELETE', `${base({ project, workspace })}/conflictResolution`);
  }

  /** `POST …/conflictResolution/discardChanges`: the workspace's changes dropped; it becomes the line's head. */
  async discardConflictResolutionChanges(project: string, workspace: string): Promise<void> {
    await this.#call('POST', `${base({ project, workspace })}/conflictResolution/discardChanges`);
  }

  // ---- revisions ----

  /** The revision at `where.revision` (default `CURRENT`). */
  revision(where: Where): Promise<Revision> {
    return this.#json('GET', base({ ...where, revision: where.revision ?? 'CURRENT' }));
  }

  /** A history, newest first (`since`/`until` ISO instants). */
  revisions(where: Where, params: { since?: string; until?: string; limit?: number } = {}): Promise<Revision[]> {
    return this.#json('GET', `${base(where)}/revisions${query({ ...params })}`);
  }

  /** `POST …/{w}/configuration`: dependencies added and removed, as one revision. */
  updateConfiguration(project: string, workspace: string, command: UpdateProjectConfigurationCommand): Promise<Revision> {
    return this.#json('POST', `${base({ project, workspace })}/configuration`, command);
  }

  // ---- reviews ----

  reviews(project: string, params: { state?: ReviewState; revisionIds?: readonly string[]; workspaceIdRegex?: string; since?: string; until?: string; limit?: number } = {}): Promise<Review[]> {
    return this.#json('GET', `/projects/${enc(project)}/reviews${query({ ...params })}`);
  }

  review(project: string, id: string): Promise<Review> {
    return this.#json('GET', `/projects/${enc(project)}/reviews/${enc(id)}`);
  }

  createReview(project: string, command: CreateReviewCommand): Promise<Review> {
    return this.#json('POST', `/projects/${enc(project)}/reviews`, command);
  }

  /** `close`, `reject`, `reopen`, `approve`, `revokeApproval`: the review after it. */
  reviewAction(project: string, id: string, action: 'close' | 'reject' | 'reopen' | 'approve' | 'revokeApproval'): Promise<Review> {
    return this.#json('POST', `/projects/${enc(project)}/reviews/${enc(id)}/${action}`);
  }

  /** `GET …/approval`: who has approved the review. */
  approval(project: string, id: string): Promise<{ readonly approvedBy: readonly User[] }> {
    return this.#json('GET', `/projects/${enc(project)}/reviews/${enc(id)}/approval`);
  }

  /** Lands the review on the project line (and deletes its workspace). */
  commitReview(project: string, id: string, message: string): Promise<Review> {
    return this.#json('POST', `/projects/${enc(project)}/reviews/${enc(id)}/commit`, { message });
  }

  // ---- versions ----

  versions(project: string): Promise<Version[]> {
    return this.#json('GET', `/projects/${enc(project)}/versions`);
  }

  /** `GET …/versions/{v}/pure`: a released version's files (lite's text route). */
  versionPure(project: string, version: string): Promise<PureFile[]> {
    return this.#json('GET', `/projects/${enc(project)}/versions/${enc(version)}/pure`);
  }

  /** `GET …/versions/{v}/configuration`: a released version's project configuration. */
  versionConfiguration(project: string, version: string): Promise<ProjectConfiguration> {
    return this.#json('GET', `/projects/${enc(project)}/versions/${enc(version)}/configuration`);
  }

  /** The latest version, or undefined (upstream's 204). */
  async latestVersion(project: string): Promise<Version | undefined> {
    const res = await this.#call('GET', `/projects/${enc(project)}/versions/latest`);
    return res.status === 204 ? undefined : res.json() as Promise<Version>;
  }

  createVersion(project: string, command: CreateVersionCommand): Promise<Version> {
    return this.#json('POST', `/projects/${enc(project)}/versions`, command);
  }

  // ---- entities (upstream's JSON, derived from the stored text) ----

  entities(where: Where, filter: EntityFilter = {}): Promise<Entity[]> {
    return this.#json('GET', `${base(where)}/entities${query({ ...filter })}`);
  }

  entity(where: Where, path: string): Promise<Entity> {
    return this.#json('GET', `${base(where)}/entities/${enc(path)}`);
  }

  /** `POST …/entityChanges`: a save from an upstream client; the new revision. */
  performChanges(project: string, workspace: string, command: PerformChangesCommand): Promise<Revision> {
    return this.#json('POST', `${base({ project, workspace })}/entityChanges`, command);
  }

  // ---- lite's text routes (S15) ----

  /** `GET …/pure`: every element's file. */
  pure(where: Where): Promise<PureFile[]> {
    return this.#json('GET', `${base(where)}/pure`);
  }

  /** `GET …/pure/{path}`: one element's file. */
  pureFile(where: Where, path: string): Promise<PureFile> {
    return this.#json('GET', `${base(where)}/pure/${enc(path)}`);
  }

  /** `POST …/pureChanges`: a save of text; the new revision. */
  performPureChanges(project: string, workspace: string, command: PerformPureChangesCommand): Promise<Revision> {
    return this.#json('POST', `${base({ project, workspace })}/pureChanges`, command);
  }

  async #call(method: string, path: string, body?: unknown): Promise<Response> {
    const res = await this.#fetch(`${this.#api}${path}`, {
      method,
      ...(body === undefined ? {} : { headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }),
    });
    if (!res.ok) {
      const text = await res.text();
      let message = text || `${res.status} ${res.statusText}`;
      try {
        const e = JSON.parse(text) as Partial<ErrorMessage>;
        if (typeof e.message === 'string') message = e.message;
      } catch {
        // not JSON: the text is the message
      }
      throw new SdlcError(message, res.status);
    }
    return res;
  }

  async #json<T>(method: string, path: string, body?: unknown): Promise<T> {
    return (await this.#call(method, path, body)).json() as Promise<T>;
  }
}
