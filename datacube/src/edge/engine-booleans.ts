// legend-engine's TinyInt for a BIT column (ENGINE DEFECT S23), read by the legend-engine client (planner.ts,
// UpstreamPlanner) alone -- the edge audit's H10, 2026-11-17: until then the product's relation-type.ts, fed by the
// product's table model's `bitColumns`. A chunk of its own, fetched by the legend-engine plane alone.

/**
 * ENGINE DEFECT (docs/SEMANTICS_REGISTER.md S23) -- delete when legend-engine types BIT Boolean. The legend-engine
 * client's own reading, here alone (the edge audit's H10, 2026-11-17: until then in the product's relation-type.ts,
 * fed by the product's table model's `bitColumns`).
 *
 * legend-engine's compiler types a Database `BIT` column `precisePrimitives::TinyInt` (one case in
 * `RelationalCompilerExtension.convertTypes`, :1085), while legend-pure, the engine's own result typing and its DuckDB
 * DDL read BIT as Boolean -- and the rows ARE booleans (probed 2026-10-01: the plan says TinyInt, DuckDB returns
 * true/false). No grammar declares a BOOLEAN column, so a boolean is always declared BIT. A column is therefore read as
 * Boolean when engine's PRECISE type is TinyInt and the model's Database declares that column BIT: a real TINYINT column
 * is declared TINYINT, and an aggregate over the column (count, sum) is typed Integer, not TinyInt, so neither is
 * touched. legend-lite types BIT Boolean itself and never answers TinyInt.
 */
const ENGINE_TINYINT = 'meta::pure::precisePrimitives::TinyInt';

/** A column of legend-engine's relation type, and of its plan's tdsColumns, as read here. */
interface RelationColumn { readonly name?: unknown; readonly genericType?: { readonly rawType?: { readonly fullPath?: unknown } } }
interface TdsColumn { readonly name?: unknown; readonly type?: unknown }

/** The columns of an answer, each with the type path legend-engine wrote: a relation type's or a plan's. */
function answerColumns(answer: unknown): readonly { readonly name?: unknown; readonly path?: unknown }[] {
  const relation = (answer as { columns?: unknown }).columns;
  if (Array.isArray(relation)) {
    return (relation as readonly RelationColumn[]).map((c) => ({ name: c.name, path: c.genericType?.rawType?.fullPath }));
  }
  const tds = (answer as { rootExecutionNode?: { resultType?: { tdsColumns?: unknown } } }).rootExecutionNode?.resultType?.tdsColumns;
  return Array.isArray(tds) ? (tds as readonly TdsColumn[]).map((c) => ({ name: c.name, path: c.type })) : [];
}

/** Whether legend-engine typed any column of `answer` TinyInt. */
export function carriesTinyInt(answer: unknown): boolean {
  return answerColumns(answer).some((c) => c.path === ENGINE_TINYINT);
}

/** The columns a model's protocol elements declare BIT: every Database's tables' columns of type Bit, by name. */
export function bitColumnsOf(elements: readonly unknown[]): ReadonlySet<string> {
  const bits = new Set<string>();
  for (const e of elements) {
    const db = e as { _type?: unknown; schemas?: readonly { tables?: readonly { columns?: readonly { name?: unknown; type?: { _type?: unknown } }[] }[] }[] };
    if (db._type !== 'relational') continue;
    for (const schema of db.schemas ?? []) {
      for (const table of schema.tables ?? []) {
        for (const column of table.columns ?? []) {
          if (column.type?._type === 'Bit' && typeof column.name === 'string') bits.add(column.name);
        }
      }
    }
  }
  return bits;
}

/** `answer` (a relation type or a plan) with each TinyInt column named in `bits` typed Boolean; the rest as written. */
export function engineBooleans(answer: unknown, bits: ReadonlySet<string>): unknown {
  const asBoolean = (name: unknown, path: unknown): boolean => path === ENGINE_TINYINT && typeof name === 'string' && bits.has(name);
  const relation = (answer as { columns?: unknown }).columns;
  if (Array.isArray(relation)) {
    return { ...(answer as object), columns: (relation as readonly RelationColumn[]).map((c) =>
      (asBoolean(c.name, c.genericType?.rawType?.fullPath)
        ? { ...c, genericType: { ...c.genericType, rawType: { ...c.genericType?.rawType, fullPath: 'Boolean' } } } : c)) };
  }
  const root = (answer as { rootExecutionNode?: { resultType?: { tdsColumns?: unknown } } }).rootExecutionNode;
  const tds = root?.resultType?.tdsColumns;
  if (!Array.isArray(tds)) return answer;
  return { ...(answer as object), rootExecutionNode: { ...root, resultType: { ...root?.resultType,
    tdsColumns: (tds as readonly TdsColumn[]).map((c) => (asBoolean(c.name, c.type) ? { ...c, type: 'Boolean' } : c)) } } };
}
