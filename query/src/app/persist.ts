// A session as a saved `Query`, and a saved `Query` opened as a session -- upstream's record and
// rules (census §8): content is the query's Pure text (no `->from()`: the execution context holds
// the mapping and runtime, or the data space), tagged with its data space and class, parameter
// values kept as Pure text.

import { findAll, isFunction, lambda as makeLambda, type Lambda } from '../../../pure-protocol/src/index.ts';
import { buildLambda, valueSpec } from '../builder/build.ts';
import { loadLambda, parametersOf, valueOf } from '../builder/load.ts';
import { emptyQuery, type ClassSource, type Value } from '../builder/state.ts';
import { QUERY_PROFILE, type Query, type QueryExecutionContext, type QueryTaggedValue } from '../backend/wire.ts';
import type { AppContext, LoadedProject } from './context.ts';
import { executionLambda } from './run.ts';
import { Session } from './session.ts';
import { literalFromText } from '../ui/values.ts';

/** A value as Pure text (`'EMEA'`, `%2024-01-02`, `today()`), printed by the grammar. */
async function valueText(app: AppContext, v: Value): Promise<string> {
  const text = await app.engine.lambdaText(makeLambda([], valueSpec(v)), 'STANDARD');
  return text.replace(/^\|/, '');
}

/**
 * The saved query's content: the lambda without `->from()`, as Pure text -- or, `withFrom` (a function's body, which
 * names its own mapping and runtime), with it.
 */
export async function contentOf(app: AppContext, session: Session, withFrom = false): Promise<string> {
  const l: Lambda = withFrom ? executionLambda(session, undefined) : session.text?.lambda ?? buildLambda(session.project.graph, session.query, { withFrom: false });
  return app.engine.lambdaText(l, 'PRETTY');
}

export async function toQuery(app: AppContext, session: Session, identity: { id: string; name: string; description?: string }): Promise<Query> {
  const src = session.query.source;
  const p = session.project.config;
  const executionContext: QueryExecutionContext = src.dataSpace
    ? { _type: 'dataSpaceExecutionContext', dataSpacePath: src.dataSpace.path, executionKey: src.dataSpace.context }
    : { _type: 'explicitExecutionContext', mapping: src.mapping, runtime: src.runtime };
  const taggedValues: QueryTaggedValue[] = [];
  if (src.dataSpace) taggedValues.push({ tag: { profile: QUERY_PROFILE, value: 'dataSpace' }, value: src.dataSpace.path });
  taggedValues.push({ tag: { profile: QUERY_PROFILE, value: 'class' }, value: src.class });
  const defaultParameterValues = await Promise.all([...session.paramValues]
    .filter(([name]) => session.query.parameters.some((x) => x.name === name))
    .map(async ([name, v]) => ({ name, content: await valueText(app, v) })));
  const prior = session.saved;
  return {
    id: identity.id,
    name: identity.name,
    description: identity.description ?? prior?.description ?? null,
    groupId: p.groupId,
    artifactId: p.artifactId,
    versionId: p.versionId,
    originalVersionId: prior?.originalVersionId ?? p.versionId,
    executionContext,
    content: await contentOf(app, session),
    taggedValues,
    stereotypes: prior?.stereotypes ?? [],
    defaultParameterValues,
    gridConfig: prior?.gridConfig ?? null,
  };
}

/** The source a saved query's execution context names, with its class read from the query itself. */
export function contextOf(project: LoadedProject, q: Query): { mapping: string; runtime: string; dataSpace?: ClassSource['dataSpace'] } {
  const ctx = q.executionContext;
  if (ctx?._type === 'explicitExecutionContext') return { mapping: ctx.mapping, runtime: ctx.runtime };
  if (ctx?._type === 'dataSpaceExecutionContext') {
    const ds = project.graph.dataSpaces.get(ctx.dataSpacePath);
    if (!ds) throw new Error(`the query's data space ${ctx.dataSpacePath} is not in ${project.gav}`);
    const key = ctx.executionKey ?? ds.defaultExecutionContext;
    const ec = ds.executionContexts.find((c) => c.name === key);
    if (!ec?.mapping || !ec.defaultRuntime) throw new Error(`the data space has no execution context '${key}' with a mapping and runtime`);
    return { mapping: ec.mapping.path, runtime: ec.defaultRuntime.path, dataSpace: { path: ctx.dataSpacePath, context: key } };
  }
  throw new Error('the query has no execution context legend-lite serves (explicit or data space)');
}

/** A saved query opened: in the form when it can be, else as text; parameter values restored. */
/**
 * A saved query as a session to edit. `asSaved: false` (a share link): the record is the query,
 * not a copy in this store -- the session is unsaved, and Save makes the person's own.
 */
export async function openQuery(app: AppContext, q: Query, urlParams: ReadonlyMap<string, string>, asSaved = true): Promise<Session> {
  // the version it was saved on, exactly (upstream pins it too): a snapshot follows the line, a release does not
  const gav = `${q.groupId}:${q.artifactId}:${q.versionId}`;
  const project = await app.ensure(gav).catch(() => undefined);
  if (!project) throw new Error(`the query belongs to ${gav}, which is not configured here and not in Depot`);
  const ctx = contextOf(project, q);
  const lambda = await app.engine.lambdaJson(q.content);
  const loaded = loadLambda(project.graph, lambda, ctx);
  let session: Session;
  if (loaded.ok) {
    session = new Session(project, loaded.query, asSaved ? q : undefined);
  } else {
    // the form cannot show it: a text-only session, its source what the context says
    const cls = q.taggedValues?.find((t) => t.tag.profile === QUERY_PROFILE && t.tag.value === 'class')?.value ?? '';
    const source: ClassSource = { kind: 'class', class: cls, mapping: ctx.mapping, runtime: ctx.runtime, ...(ctx.dataSpace ? { dataSpace: ctx.dataSpace } : {}) };
    session = new Session(project, { ...emptyQuery(source), parameters: parametersOf(lambda) }, asSaved ? q : undefined, { lambda, reason: loaded.reason });
  }
  if (!asSaved) session.sharedAs = q.name;
  for (const pv of q.defaultParameterValues ?? []) {
    try {
      const parsed = await app.engine.lambdaJson(`|${pv.content}`);
      session.paramValues.set(pv.name, valueOf(parsed.body[0] as never));
    } catch { /* a value the form cannot read is left for the person to set */ }
  }
  for (const [name, text] of urlParams) {
    const p = session.query.parameters.find((x) => x.name === name) ?? lambda.parameters.find((x) => x.name === name);
    const type = p && 'type' in p ? p.type : p?.genericType?.rawType._type === 'packageableType' ? p.genericType.rawType.fullPath : undefined;
    const v = type ? literalFromText(project.graph, type, text) : undefined;
    if (v) session.paramValues.set(name, v);
  }
  return session;
}

/** A curated, service or embedding host's query opened in the form, or as text when the form cannot show it. */
export function sessionFrom(p: LoadedProject, lambda: Lambda, ctx: { mapping: string; runtime: string; dataSpace?: ClassSource['dataSpace'] }): Session {
  const loaded = loadLambda(p.graph, lambda, ctx);
  if (loaded.ok) return new Session(p, loaded.query);
  const getAll = findAll(lambda, isFunction).find((f) => f.function === 'getAll' || f.function.endsWith('::getAll'));
  const target = getAll?.parameters[0];
  const cls = target?._type === 'packageableElementPtr' ? target.fullPath : '';
  const source: ClassSource = { kind: 'class', class: cls, mapping: ctx.mapping, runtime: ctx.runtime, ...(ctx.dataSpace ? { dataSpace: ctx.dataSpace } : {}) };
  return new Session(p, { ...emptyQuery(source), parameters: parametersOf(lambda) }, undefined, { lambda, reason: loaded.reason });
}

export function newQueryId(): string {
  return globalThis.crypto?.randomUUID?.() ?? `q-${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
}
