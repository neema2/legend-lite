// THE SDLC IN THIS PAGE (design S21, level 0): upstream legend-sdlc's REST API and lite's text routes
// (S15) answered without a server, from the page's own records (records.ts). The rules are legend-sdlc's
// GitLab backend's, route for route, status for status, message for message
// (studio/docs/SDLC_CONTRACT_SLICE1.md); where this departs, the comment says so and README.md lists it.
// It is handed to the one client (client.ts) in place of the network, and the one suite
// (test/conformance.ts) runs against it -- and, when it lands, against the model home's server.
//
// What it stores is text (design S5): one `.pure` file per element, imports refused at save as upstream
// SDLC refuses them (S20, v0). Entities are DERIVED on read by the page's own grammar (the WASM planner's
// `modelJsonOrError`, the engine's `grammarToJson`), never stored.
//
// What it cannot be is a team's SDLC: there are no reviews, versions or patches here (they answer
// upstream's 501 "does not support"), its revisions live in this browser, and its user is the one the
// page was configured with.

import { classifierPathOf } from './classifiers.ts';
import type { Records } from './records.ts';
import type {
  CreateProjectCommand, Entity, ProjectConfiguration, PureChange, PureFile, Revision, User, Workspace,
} from './wire.ts';

/** The API root the page's SDLC answers at: never on the network (`fetch` below answers it). */
export const LOCAL_API = 'http://this-browser.invalid/sdlc/api';

/** The backend type a 501 names. */
const BACKEND = 'page';

/** The project structure every project here declares (upstream's latest; the layout itself is lite's, S5). */
const STRUCTURE_VERSION = 13;

/** What the page needs of a grammar: Pure text to its protocol elements, or a refusal (an Error). */
export interface ModelReader {
  modelJson(text: string): Promise<{ readonly elements: readonly Readonly<Record<string, unknown>>[] }>;
}

export interface LocalSdlcOptions {
  readonly records: Records;
  /** Who this page's caller is: the page's configured user (there is no sign-in here). */
  readonly user: User;
  readonly grammar: ModelReader;
  readonly clock?: () => number;
}

export interface LocalSdlc {
  /** One request to `LOCAL_API/...`, answered as the server answers it. */
  handle(request: Request): Promise<Response>;
  /** The same, shaped as `fetch`: what the client is handed. */
  readonly fetch: typeof fetch;
}

/** A refusal: its status and legend-sdlc's words. */
class Refusal extends Error {
  readonly status: number;
  readonly details: string | undefined;
  constructor(message: string, status: number, details?: string) {
    super(message);
    this.status = status;
    this.details = details;
  }
}

/** upstream's 501 for a capability this backend does not have (`UnsupportedCapabilityExceptionMapper`). */
class Unsupported extends Error {
  readonly capability: string;
  constructor(capability: string) {
    super(`The backend "${BACKEND}" does not support ${capability}`);
    this.capability = capability;
  }
}

// ---- the records (layout 1) ----
//
//   project/<p>                    ProjectRecord
//   line/<p>                       Ref: the project line
//   ws/<p>/<user>/<w>              Ref: a user workspace
//   rev/<id>                       RevisionRecord (immutable; id = SHA-1 of its canonical JSON)
//   blob/<id>                      a file's text (immutable; id = SHA-1 of the text)

interface ProjectRecord {
  readonly projectId: string;
  readonly name: string;
  readonly description: string;
  readonly tags: readonly string[];
}

interface Ref {
  /** The revision the ref points at. */
  readonly head: string;
  /** Where it was made from: a workspace's merge base, the project line's first revision. */
  readonly base: string;
}

interface WorkspaceRef extends Ref {
  readonly workspaceId: string;
}

interface RevisionRecord {
  readonly id: string;
  readonly parent: string | null;
  readonly config: ProjectConfiguration;
  /** entity path → blob id. */
  readonly files: Readonly<Record<string, string>>;
  readonly authorName: string;
  readonly authoredTimestamp: string;
  readonly committerName: string;
  readonly committedTimestamp: string;
  readonly message: string;
}

type Json = Record<string, unknown>;

const isObject = (v: unknown): v is Json => typeof v === 'object' && v !== null && !Array.isArray(v);

/** `Instant.toString()` (ISO_INSTANT): milliseconds only when not zero. */
const instant = (ms: number): string => new Date(ms).toISOString().replace('.000Z', 'Z');

async function sha1(text: string): Promise<string> {
  const digest = await globalThis.crypto.subtle.digest('SHA-1', new TextEncoder().encode(text));
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

/** An `ExtendedErrorMessage` body: code, message, [details], timestamp; nulls omitted. */
function error(status: number, message: string, details: string | undefined, now: number): Response {
  const body: Json = { code: status, message };
  if (details !== undefined) body['details'] = details;
  body['timestamp'] = instant(now);
  return new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } });
}

function ok(body: unknown): Response {
  return new Response(JSON.stringify(body), { status: 200, headers: { 'Content-Type': 'application/json' } });
}

const NO_CONTENT = (): Response => new Response(null, { status: 204 });

// ---- path rules (legend-sdlc-model EntityPaths.java:28-123) ----

const ENTITY_PATH = /^(?!meta::)[A-Za-z0-9_]+(::[A-Za-z0-9_]+)*::[A-Za-z0-9_$]+$/;
const WORKSPACE_ID = /^[A-Za-z0-9_]([A-Za-z0-9_-]|\.(?!\.))*[A-Za-z0-9_]$|^[A-Za-z0-9_]$/;
// ProjectStructure.java:93 and SourceVersion.isName (dotted Java identifiers; keywords checked below)
const ARTIFACT_ID = /^[a-z][a-z\d_]*(-[a-z][a-z\d_]*)*$/;
const JAVA_IDENTIFIER = /^[A-Za-z_$][A-Za-z\d_$]*$/;
const JAVA_KEYWORDS = new Set(['abstract', 'assert', 'boolean', 'break', 'byte', 'case', 'catch', 'char', 'class', 'const',
  'continue', 'default', 'do', 'double', 'else', 'enum', 'extends', 'final', 'finally', 'float', 'for', 'goto', 'if',
  'implements', 'import', 'instanceof', 'int', 'interface', 'long', 'native', 'new', 'package', 'private', 'protected',
  'public', 'return', 'short', 'static', 'strictfp', 'super', 'switch', 'synchronized', 'this', 'throw', 'throws',
  'transient', 'try', 'void', 'volatile', 'while', 'true', 'false', 'null', '_']);

const isJavaName = (s: string): boolean =>
  s.split('.').every((part) => JAVA_IDENTIFIER.test(part) && !JAVA_KEYWORDS.has(part));

/** Where an element's file sits in a revision's tree: `<package path>/<Name>.pure` (design S5). */
const filePathOf = (entityPath: string): string => `${entityPath.split('::').join('/')}.pure`;

// ---- reference descriptions (legend-sdlc's two phrasings, contract §0.5) ----

const wsOf = (p: string, w: string): string => `user workspace ${w} of project ${p}`;
const wsIn = (p: string, w: string): string => `user workspace ${w} in project ${p}`;

export function localSdlcServer(options: LocalSdlcOptions): LocalSdlc {
  const { records, user, grammar } = options;
  const clock = options.clock ?? Date.now;
  /** Entities by the blob they were read from: a blob is immutable, so is its entity. */
  const derived = new Map<string, Promise<Entity>>();
  /** Writes to one project, one at a time (a browser's tabs aside: BrowserRecords is per tab). */
  let writes: Promise<unknown> = Promise.resolve();
  const serially = <T>(f: () => Promise<T>): Promise<T> => {
    const next = writes.then(f, f);
    writes = next.catch(() => undefined);
    return next;
  };

  // ---- reading ----

  async function project(p: string): Promise<ProjectRecord> {
    const record = await records.get<ProjectRecord>(`project/${p}`);
    if (!record) throw new Refusal(`Unknown project: ${p}`, 404);
    return record;
  }

  async function revisionRecord(id: string): Promise<RevisionRecord> {
    const r = await records.get<RevisionRecord>(`rev/${id}`);
    if (!r) throw new Error(`the page's SDLC lost revision ${id}`);
    return r;
  }

  async function blob(id: string): Promise<string> {
    const text = await records.get<string>(`blob/${id}`);
    if (text === undefined) throw new Error(`the page's SDLC lost file ${id}`);
    return text;
  }

  /** A workspace's ref, or the 404 in the phrasing the caller's context uses. */
  async function workspaceRef(p: string, w: string, phrase: (p: string, w: string) => string): Promise<Ref> {
    await project(p);
    const ref = await records.get<Ref>(`ws/${p}/${user.userId}/${w}`);
    if (!ref) throw new Refusal(`Unknown: ${phrase(p, w)}`, 404);
    return ref;
  }

  async function lineRef(p: string): Promise<Ref> {
    await project(p);
    const ref = await records.get<Ref>(`line/${p}`);
    if (!ref) throw new Error(`the page's SDLC lost the line of project ${p}`);
    return ref;
  }

  async function isAncestor(ancestor: string, of: string): Promise<boolean> {
    for (let at: string | null = of; at !== null; at = (await revisionRecord(at)).parent) {
      if (at === ancestor) return true;
    }
    return false;
  }

  /**
   * A revision named in a URL: an alias (case-insensitive: BASE; HEAD, CURRENT, LATEST) or an id on
   * the scope's history (contract §4). DEPARTURE: entity and text reads check a literal id too
   * (upstream reads any commit of the repository through any workspace URL, quirk 6).
   */
  async function resolve(p: string, w: string | undefined, r: string): Promise<RevisionRecord> {
    const ref = w === undefined ? await lineRef(p) : await workspaceRef(p, w, wsIn);
    const alias = r.toLowerCase();
    if (alias === 'base') return revisionRecord(ref.base);
    if (alias === 'head' || alias === 'current' || alias === 'latest') return revisionRecord(ref.head);
    const desc = w === undefined ? `project ${p}` : wsIn(p, w);
    if (!(await records.get<RevisionRecord>(`rev/${r}`)) || !(await isAncestor(r, ref.head))) {
      throw new Refusal(`Revision ${r} is unknown for ${desc}`, 404);
    }
    return revisionRecord(r);
  }

  const revisionView = (r: RevisionRecord): Revision => ({
    id: r.id,
    authorName: r.authorName,
    authoredTimestamp: r.authoredTimestamp,
    committerName: r.committerName,
    committedTimestamp: r.committedTimestamp,
    message: r.message,
  });

  const projectView = (p: ProjectRecord): Json => ({
    projectId: p.projectId, name: p.name, description: p.description, tags: p.tags, webUrl: null,
  });

  /** `SimpleProjectConfiguration` as served: creator order, nulls written (contract §7). */
  const configView = (c: ProjectConfiguration): Json => ({
    projectId: c.projectId,
    projectType: c.projectType ?? null,
    projectStructureVersion: { version: c.projectStructureVersion.version, extensionVersion: c.projectStructureVersion.extensionVersion ?? null },
    platformConfigurations: c.platformConfigurations ?? null,
    groupId: c.groupId,
    artifactId: c.artifactId,
    projectDependencies: c.projectDependencies,
    metamodelDependencies: c.metamodelDependencies ?? [],
    artifactGenerations: [],
    runDependencyTests: c.runDependencyTests ?? null,
    produceShadedServiceJar: c.produceShadedServiceJar ?? null,
  });

  // ---- one file's text: what a save accepts, and the entity a read derives ----

  /**
   * A file's text read by the grammar: exactly one element (and at most one section index, with no
   * imports), at `path`, of a type an SDLC stores. legend-sdlc's own rules for a `.pure` file
   * (PureEntitySerializer.java:165-260), then lite's: the element must be the one the file is named for.
   * The refusals, or the entity.
   */
  async function read(path: string, text: string): Promise<{ entity?: Entity; errors: string[] }> {
    let elements: readonly Readonly<Json>[];
    try {
      elements = (await grammar.modelJson(text)).elements;
    } catch (e) {
      return { errors: [e instanceof Error ? e.message : String(e)] };
    }
    const sections = elements.filter((e) => e['_type'] === 'sectionIndex');
    const others = elements.filter((e) => e['_type'] !== 'sectionIndex');
    if (sections.length > 1) return { errors: [`Expected at most one SectionIndex, found ${sections.length}`] };
    if (others.length === 0) return { errors: ['No element found'] };
    if (others.length > 1) return { errors: [`Expected one element, found ${others.length}`] };
    const hasImports = sections.some((s) => Array.isArray(s['sections'])
      && (s['sections'] as Json[]).some((sec) => Array.isArray(sec['imports']) && (sec['imports'] as unknown[]).length > 0));
    if (hasImports) return { errors: ['Imports in Pure files are not currently supported'] };
    const element = others[0]!;
    const found = `${String(element['package'])}::${String(element['name'])}`;
    const errors: string[] = [];
    if (found !== path) errors.push(`Mismatch between entity path ("${path}") and the element's path ("${found}")`);
    const classifierPath = classifierPathOf(String(element['_type']));
    if (classifierPath === undefined) errors.push(`Unsupported element type: ${String(element['_type'])}`);
    if (errors.length > 0) return { errors };
    return { entity: { path, classifierPath: classifierPath!, content: element }, errors };
  }

  function entityOf(path: string, blobId: string): Promise<Entity> {
    let e = derived.get(blobId);
    if (!e) {
      e = (async () => {
        const r = await read(path, await blob(blobId));
        if (!r.entity) {
          throw new Refusal(`Error deserializing entity "${path}" from file "${filePathOf(path)}": ${r.errors.join('; ')}`, 500);
        }
        return r.entity;
      })();
      derived.set(blobId, e);
      e.catch(() => derived.delete(blobId));
    }
    return e;
  }

  // ---- entity filters (contract §5, EntityAccessResource.java:40-246) ----

  function regex(source: string): RegExp {
    try {
      return new RegExp(source, 'i');
    } catch {
      return new RegExp(source.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
    }
  }

  function filtered(entities: readonly Entity[], q: URLSearchParams): Entity[] {
    const classifiers = q.getAll('classifierPath');
    const packages = q.getAll('package');
    const subPackages = (q.get('includeSubPackages') ?? 'true') !== 'false';
    const name = q.get('name');
    const nameRegex = name === null ? null : regex(name);
    const stereotypes = new Set(q.getAll('stereotype'));
    const tagged = new Map<string, RegExp[]>();
    for (const tv of q.getAll('taggedValue')) {
      const slash = tv.indexOf('/');
      const tag = (slash < 0 ? tv : tv.slice(0, slash)).trim();
      const rx = slash < 0 ? '' : tv.slice(slash + 1).trim();
      tagged.set(tag, [...(tagged.get(tag) ?? []), regex(rx)]);
    }
    return entities.filter((e) => {
      const cut = e.path.lastIndexOf('::');
      const pkg = e.path.slice(0, cut);
      const simple = e.path.slice(cut + 2);
      if (packages.length > 0 && !packages.some((p) => pkg === p || (subPackages && pkg.startsWith(`${p}::`)))) return false;
      if (nameRegex && !nameRegex.test(simple)) return false;
      if (classifiers.length > 0 && !classifiers.includes(e.classifierPath)) return false;
      if (stereotypes.size > 0) {
        const own = (Array.isArray(e.content['stereotypes']) ? e.content['stereotypes'] as Json[] : [])
          .map((s) => `${String(s['profile'])}.${String(s['value'])}`);
        if (!own.some((s) => stereotypes.has(s))) return false;
      }
      if (tagged.size > 0) {
        const own = Array.isArray(e.content['taggedValues']) ? e.content['taggedValues'] as Json[] : [];
        const hit = own.some((t) => {
          const tag = isObject(t['tag']) ? `${String(t['tag']['profile'])}.${String(t['tag']['value'])}` : '';
          return typeof t['value'] === 'string' && (tagged.get(tag) ?? []).some((rx) => rx.test(t['value'] as string));
        });
        if (!hit) return false;
      }
      return true;
    });
  }

  /** Every entity of a revision, by path (DEPARTURE: a stable order; upstream's is hash order, quirk 14). */
  async function entities(r: RevisionRecord, excludeInvalid: boolean): Promise<Entity[]> {
    const out: Entity[] = [];
    for (const path of Object.keys(r.files).sort()) {
      try {
        out.push(await entityOf(path, r.files[path]!));
      } catch (e) {
        if (!excludeInvalid) throw e;
      }
    }
    return out;
  }

  // ---- writing ----

  async function commit(parent: RevisionRecord | null, config: ProjectConfiguration, files: Readonly<Record<string, string>>,
    message: string): Promise<RevisionRecord> {
    const at = instant(clock());
    const body = {
      parent: parent?.id ?? null,
      config,
      files,
      authorName: user.userId,
      authoredTimestamp: at,
      committerName: user.userId,
      committedTimestamp: at,
      message,
    };
    const id = await sha1(JSON.stringify(body));
    const record: RevisionRecord = { id, ...body };
    await records.put(`rev/${id}`, record);
    return record;
  }

  async function createProject(body: unknown): Promise<Json> {
    if (!isObject(body)) throw new Refusal('Input required to create project', 400);
    const c = body as Partial<CreateProjectCommand>;
    const nonEmpty = typeof c.name === 'string' && c.name !== '';
    if (!nonEmpty) throw new Refusal('name may not be null or empty', 400);
    if (typeof c.description !== 'string') throw new Refusal('description may not be null', 400);
    if (typeof c.groupId !== 'string' || c.groupId === '' || !isJavaName(c.groupId)) {
      throw new Refusal(`Invalid groupId: ${String(c.groupId ?? null)}`, 400);
    }
    if (typeof c.artifactId !== 'string' || !ARTIFACT_ID.test(c.artifactId)) {
      throw new Refusal(`Invalid artifactId: ${String(c.artifactId ?? null)}. ArtifactId must follow pattern that starts with a lowercase letter and can include lowercase letters, digits, underscores, and hyphens between segments.`, 400);
    }
    const type = c.type == null ? 'MANAGED' : String(c.type).toUpperCase();
    if (type !== 'MANAGED' && type !== 'EMBEDDED') throw new Refusal(`Invalid type: ${type}`, 400);
    // DEPARTURE (lite): a project is named by its coordinates, `groupId:artifactId` -- the id upstream's
    // own project.json uses for a dependency -- not a GitLab number; one project per coordinates.
    const projectId = `${c.groupId}:${c.artifactId}`;
    return serially(async () => {
      if (await records.get(`project/${projectId}`)) {
        throw new Refusal(`Failed to create project: ${c.name}: a project with coordinates ${projectId} already exists`, 409);
      }
      const record: ProjectRecord = {
        projectId, name: c.name!, description: c.description!, tags: [...(c.tags ?? [])],
      };
      const first = await commit(null, {
        projectId,
        projectType: type as 'MANAGED' | 'EMBEDDED',
        projectStructureVersion: { version: STRUCTURE_VERSION, extensionVersion: null },
        groupId: c.groupId!,
        artifactId: c.artifactId!,
        projectDependencies: [],
        metamodelDependencies: [],
      }, {}, 'Build project structure');
      await records.put(`project/${projectId}`, record);
      await records.put(`line/${projectId}`, { head: first.id, base: first.id } satisfies Ref);
      return projectView(record);
    });
  }

  async function createWorkspace(p: string, w: string): Promise<Workspace> {
    if (!WORKSPACE_ID.test(w)) {
      throw new Refusal(`Invalid workspace id: "${w}". A workspace id must be a non-empty string consisting of characters from the following set: {a-z, A-Z, 0-9, _, ., -}. The id may not contain ".." and may not start or end with '.' or '-'.`, 400);
    }
    return serially(async () => {
      const line = await lineRef(p);
      const key = `ws/${p}/${user.userId}/${w}`;
      const existing = await records.get<Ref>(key);
      // upstream: already there AT the line's head is a no-op; elsewhere, GitLab's "Branch already exists" as a 500
      if (existing && existing.head !== line.head) {
        throw new Refusal(`Error creating ${wsOf(p, w)}: Branch already exists`, 500);
      }
      if (!existing) await records.put(key, { workspaceId: w, head: line.head, base: line.head } satisfies WorkspaceRef);
      return { projectId: p, userId: user.userId, workspaceId: w };
    });
  }

  const changeString = (c: unknown): string => {
    if (!isObject(c)) return '(null)';
    return `<PureChange type=${String(c['type'] ?? null)} path=${String(c['path'] ?? null)}>`;
  };

  /**
   * `POST …/pureChanges` (S15): legend-sdlc's `entityChanges` rules (contract §6) over text. Every
   * change is checked -- shape, then its text read by the grammar -- and all errors answered as one 400
   * in upstream's layout; none is a 204; then the lock; then the operations, in upstream's words.
   * DEPARTURES: the stale-revision 409 comes before the operations (upstream checks the operations
   * against the stale state first, so a stale save may answer 500, quirk 8); two changes to one path
   * are refused (quirk 10).
   */
  async function pureChanges(p: string, w: string, body: unknown): Promise<Response> {
    if (!isObject(body)) throw new Refusal('Input required to perform entity changes', 400);
    const changes = body['changes'] ?? [];
    if (!Array.isArray(changes)) throw new Refusal('Unable to process JSON', 400, 'changes must be an array');
    if (typeof body['message'] !== 'string') throw new Refusal('message may not be null', 400);
    const message = body['message'];

    const problems: string[] = [];
    const seen = new Set<string>();
    const parsed: { change: PureChange; text: string | undefined }[] = [];
    for (const [i, raw] of changes.entries()) {
      const errors: string[] = [];
      if (!isObject(raw)) {
        errors.push(`Invalid entity change: ${String(raw)}`);
      } else {
        const type = raw['type'];
        const path = raw['path'];
        const code = raw['pureCode'];
        if (type == null) errors.push('Missing entity change type');
        else if (type !== 'CREATE' && type !== 'MODIFY' && type !== 'DELETE') errors.push(`Invalid entity change type: ${String(type)}`);
        if (path == null) errors.push('Missing entity path');
        else if (typeof path !== 'string' || !ENTITY_PATH.test(path)) errors.push(`Invalid entity path: ${String(path)}`);
        else if (seen.has(path)) errors.push(`Duplicate entity path: ${path}`);
        else seen.add(path);
        if (type === 'DELETE' && code != null) errors.push('Unexpected Pure code');
        if ((type === 'CREATE' || type === 'MODIFY') && typeof code !== 'string') errors.push('Missing Pure code');
        if (errors.length === 0 && typeof code === 'string' && type !== 'DELETE') errors.push(...(await read(path as string, code)).errors);
        if (errors.length === 0) parsed.push({ change: raw as unknown as PureChange, text: typeof code === 'string' ? code : undefined });
      }
      if (errors.length > 0) problems.push(`\tEntity change #${i + 1} (${changeString(raw)}):\n${errors.map((e) => `\t\t${e}`).join('\n')}`);
    }
    if (problems.length > 0) throw new Refusal(`There are entity change errors:\n${problems.join('\n')}`, 400);
    if (parsed.length === 0) return NO_CONTENT();

    return serially(async () => {
      const key = `ws/${p}/${user.userId}/${w}`;
      const ref = await workspaceRef(p, w, wsIn);
      const revisionId = body['revisionId'];
      if (revisionId != null && revisionId !== ref.head) {
        throw new Refusal(`Expected revision ${String(revisionId)} of ${wsOf(p, w)} to be at revision ${String(revisionId)}; instead it was at revision ${ref.head}`, 409);
      }
      const head = await revisionRecord(ref.head);
      const files: Record<string, string> = { ...head.files };
      let changed = false;
      for (const { change, text } of parsed) {
        const operation = `<PureChange type=${change.type} path=${change.path}>`;
        const exists = change.path in files;
        if (change.type === 'CREATE' && exists) {
          throw new Refusal(`Unable to handle operation ${operation}: entity "${change.path}" already exists`, 500);
        }
        if (change.type !== 'CREATE' && !exists) {
          throw new Refusal(`Unable to handle operation ${operation}: could not find entity "${change.path}"`, 500);
        }
        if (change.type === 'DELETE') {
          delete files[change.path];
          changed = true;
          continue;
        }
        const id = await sha1(text!);
        if (files[change.path] === id) continue; // the same text: a no-op, as upstream's same bytes
        await records.put(`blob/${id}`, text!);
        files[change.path] = id;
        changed = true;
      }
      if (!changed) return NO_CONTENT();
      const next = await commit(head, head.config, files, message);
      await records.put(key, { workspaceId: w, head: next.id, base: ref.base } satisfies WorkspaceRef);
      return ok(revisionView(next));
    });
  }

  // ---- the routes ----

  async function route(method: string, parts: readonly string[], q: URLSearchParams, body: () => Promise<unknown>): Promise<Response> {
    const [a, b] = parts;
    if (parts.length === 1 && a === 'currentUser' && method === 'GET') return ok({ userId: user.userId, name: user.name });
    if (a === 'auth' && parts.length === 2 && method === 'GET') {
      if (b === 'authorized') return ok(true);
      if (b === 'termsOfServiceAcceptance') return ok([]);
    }
    if (a === 'server' && b === 'features' && parts.length === 2 && method === 'GET') {
      return ok({ canCreateProject: true, canCreateVersion: false });
    }
    if (a === 'configuration' && b === 'latestProjectStructureVersion' && parts.length === 2 && method === 'GET') {
      return ok({ version: STRUCTURE_VERSION, extensionVersion: null });
    }
    if (a !== 'projects') throw new Refusal('HTTP 404 Not Found', 404);

    if (parts.length === 1) {
      if (method === 'POST') return ok(await createProject(await body()));
      if (method !== 'GET') throw new Refusal('HTTP 405 Method Not Allowed', 405);
      const limitText = q.get('limit');
      const limit = limitText === null ? undefined : Number(limitText);
      if (limit !== undefined && !Number.isInteger(limit)) throw new Refusal('HTTP 404 Not Found', 404);
      if (limit !== undefined && limit < 0) throw new Refusal(`Invalid limit: ${limit}`, 400);
      if (limit === 0) return ok([]);
      const search = q.get('search')?.toLowerCase();
      const tags = q.getAll('tag');
      const excluded = q.getAll('excludeTag');
      const all = (await records.list<ProjectRecord>('project/')).filter((p) =>
        (search === undefined || p.name.toLowerCase().includes(search) || p.description.toLowerCase().includes(search))
        && (tags.length === 0 || tags.some((t) => p.tags.includes(t)))
        && !excluded.some((t) => p.tags.includes(t)));
      return ok(all.slice(0, limit).map(projectView));
    }

    const p = b!;
    const rest = parts.slice(2);
    if (rest.length === 0) {
      if (method !== 'GET') throw new Refusal('HTTP 405 Method Not Allowed', 405);
      return ok(projectView(await project(p)));
    }
    const [c, d] = rest;
    if (c === 'reviews') throw new Unsupported('REVIEWS');
    if (c === 'versions') throw new Unsupported('VERSIONS');
    if (c === 'patches') throw new Unsupported('PATCHES');
    if (c === 'conflictResolution' && rest.length === 1 && method === 'GET') {
      await project(p);
      return ok([]);
    }
    if (c === 'workspaces') {
      if (rest.length === 1) {
        if (method !== 'GET') throw new Refusal('HTTP 405 Method Not Allowed', 405);
        await project(p);
        // the page has one user: `owned` changes nothing
        return ok((await workspaceIds(p)).map((w) => ({ projectId: p, userId: user.userId, workspaceId: w })));
      }
      const w = d!;
      const sub = rest.slice(2);
      if (sub.length === 0) {
        if (method === 'GET') {
          await workspaceRef(p, w, wsOf);
          return ok({ projectId: p, userId: user.userId, workspaceId: w });
        }
        if (method === 'POST') return ok(await createWorkspace(p, w));
        if (method === 'DELETE') {
          // upstream: deleting what is not there succeeds (quirk 3)
          await serially(() => records.delete(`ws/${p}/${user.userId}/${w}`));
          return NO_CONTENT();
        }
        throw new Refusal('HTTP 405 Method Not Allowed', 405);
      }
      if (sub.length === 1 && method === 'GET' && sub[0] === 'outdated') {
        const ref = await workspaceRef(p, w, wsOf);
        const line = await lineRef(p);
        return ok(ref.head !== line.head && !(await isAncestor(line.head, ref.head)));
      }
      if (sub.length === 1 && method === 'GET' && sub[0] === 'inConflictResolutionMode') {
        await workspaceRef(p, w, wsOf);
        return ok(false);
      }
      if (sub.length === 1 && method === 'POST' && sub[0] === 'pureChanges') return pureChanges(p, w, await body());
      if (sub.length === 1 && method === 'POST' && sub[0] === 'entityChanges') {
        // DEPARTURE (S20): a JSON save is printed to text first (S5), and the model printer is not built yet
        await workspaceRef(p, w, wsOf);
        throw new Unsupported('ENTITY_CHANGES');
      }
      return reads(p, w, sub, method, q);
    }
    return reads(p, undefined, rest, method, q);
  }

  async function workspaceIds(p: string): Promise<string[]> {
    return (await records.list<WorkspaceRef>(`ws/${p}/${user.userId}/`)).map((r) => r.workspaceId).sort();
  }

  /** The read routes under a project or a workspace: configuration, revisions, entities, text. */
  async function reads(p: string, w: string | undefined, sub: readonly string[], method: string, q: URLSearchParams): Promise<Response> {
    if (method !== 'GET') throw new Refusal('HTTP 405 Method Not Allowed', 405);
    let revision = 'HEAD';
    let at = sub;
    let named = false;
    if (at[0] === 'revisions') {
      if (at.length === 1) throw new Refusal('HTTP 404 Not Found', 404); // revision history: not in this slice
      revision = at[1]!;
      named = true;
      at = at.slice(2);
      if (at.length === 0) return ok(revisionView(await resolve(p, w, revision)));
    }
    const r = await resolve(p, w, revision);
    const desc = (named ? `revision ${revision} of ` : '') + (w === undefined ? `project ${p}` : wsOf(p, w));
    const [what, path] = at;
    if (what === 'configuration' && at.length === 1) return ok(configView(r.config));
    if (what === 'entities' && at.length === 1) {
      return ok(filtered(await entities(r, q.get('excludeInvalid') === 'true'), q));
    }
    if (what === 'entities' && at.length === 2) {
      const blobId = r.files[path!];
      if (blobId === undefined) throw new Refusal(`Unknown entity ${path} for ${desc}`, 404);
      return ok(await entityOf(path!, blobId));
    }
    if (what === 'pure' && at.length === 1) {
      const files: PureFile[] = [];
      for (const fp of Object.keys(r.files).sort()) files.push({ path: fp, pureCode: await blob(r.files[fp]!) });
      return ok(files);
    }
    if (what === 'pure' && at.length === 2) {
      const blobId = r.files[path!];
      if (blobId === undefined) throw new Refusal(`Unknown entity ${path} for ${desc}`, 404);
      return ok({ path: path!, pureCode: await blob(blobId) } satisfies PureFile);
    }
    throw new Refusal('HTTP 404 Not Found', 404);
  }

  async function handle(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const root = new URL(LOCAL_API).pathname;
    if (url.origin !== new URL(LOCAL_API).origin || !url.pathname.startsWith(`${root}/`)) {
      return error(404, 'HTTP 404 Not Found', undefined, clock());
    }
    const parts = url.pathname.slice(root.length + 1).split('/').filter((s) => s !== '').map(decodeURIComponent);
    const body = async (): Promise<unknown> => {
      const text = await request.text();
      if (text === '') return null;
      try {
        return JSON.parse(text);
      } catch (e) {
        throw new Refusal('Unable to process JSON', 400, e instanceof Error ? e.message : String(e));
      }
    };
    try {
      return await route(request.method, parts, url.searchParams, body);
    } catch (e) {
      if (e instanceof Unsupported) {
        return new Response(JSON.stringify({ capability: e.capability, backendType: BACKEND, message: e.message }),
          { status: 501, headers: { 'Content-Type': 'application/json' } });
      }
      if (e instanceof Refusal) return error(e.status, e.message, e.details, clock());
      return error(500, e instanceof Error ? e.message : String(e), undefined, clock());
    }
  }

  return {
    handle,
    fetch: (input, init) => handle(input instanceof Request ? input : new Request(input, init)),
  };
}
