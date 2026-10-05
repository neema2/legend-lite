// A source's columns, as the COMPILER types them
// (docs/DATACUBE_TYPES_TO_SERVER_2026_09_27.md, T1, option B).
//
// A host names a source and, where it knows better than the default, a column's
// kind (a year is a dimension). It never writes a type: opening a source asks the
// compiler for the relation type of the source expression, through whatever answers
// upstream's `lambdaRelationType` on this plane -- the tab's planner module, a
// legend-lite or legend-engine server.

import { lambda, type Lambda, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import type { PlanColumn } from '../../engine-client/src/relation-type.ts';
import type { ColumnKind, ColumnSpec } from './snapshot.ts';

/** Anything that answers `lambdaRelationType`: every planner, runner and executor. */
export interface RelationTyper {
  relationType(query: Lambda, signal?: AbortSignal): Promise<PlanColumn[]>;
}

/** What a host may say about a column: its kind. Never its type. */
export interface DeclaredColumn {
  readonly name: string;
  readonly kind?: ColumnKind;
}

/**
 * The source's columns, in the compiler's order and with the compiler's types, each
 * with the kind the host declared. A declared column the source does not have is
 * refused: the host and the source disagree, which a guess would hide.
 */
export async function sourceColumns(
  typer: RelationTyper,
  source: ValueSpecification,
  declared: readonly DeclaredColumn[] = [],
  signal?: AbortSignal,
): Promise<ColumnSpec[]> {
  const typed = await typer.relationType(lambda([], source), signal);
  const names = new Set(typed.map((c) => c.name));
  const missing = declared.filter((d) => !names.has(d.name)).map((d) => d.name);
  if (missing.length > 0) {
    throw new Error(`the source has no column ${missing.map((m) => `'${m}'`).join(', ')}`);
  }
  const kinds = new Map(declared.flatMap((d) => (d.kind === undefined ? [] : [[d.name, d.kind] as const])));
  return typed.map((c) => {
    const kind = kinds.get(c.name);
    return { name: c.name, type: c.type, ...(kind === undefined ? {} : { kind }) };
  });
}
