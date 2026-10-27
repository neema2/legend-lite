// A FLOW'S OUTPUT (docs/DATACUBE_FLOW_MODE_2026_10_24.md M6): a box's rows written as a CSV file -- every row, in the order
// its sorts give them (query.ts handedOnLambda), the columns it hands on under the names it hands them on by
// (box-columns.ts handedOn, handedOnRows: what the box after it reads), each value as the database gave it
// (export.ts toCsv, none defused: a data file is read by a program, not a spreadsheet). One writer for the tab's Write
// now and Run (app.ts writeOutput) and Python's flow.run (headless gridOutput): the same file from both (the review of
// 2026-10-26's B2, C9, C10). Streamed a chunk at a time, each chunk written and let go.
//
// PURE: no DOM, no planner.

import type { ResultTable } from '../../engine-client/src/result.ts';
import type { Lambda } from '../../pure-protocol/src/index.ts';
import { handedOn, handedOnRows } from './box-columns.ts';
import { toCsv } from './export.ts';
import { isMachinery } from './grid/columns.ts';
import { columnsLambda, handedOnLambda } from './query.ts';
import type { RunOutcome } from './runner.ts';
import { CubeRefusal, type CubeSnapshot } from './snapshot.ts';

/** The database as an Output asks it: a query's rows typed (none of them read), and a query's rows a chunk at a time. */
export interface OutputQueries {
  runQuery(query: Lambda, snapshot: CubeSnapshot): Promise<RunOutcome>;
  streamQuery(query: Lambda, snapshot: CubeSnapshot, onChunk: (chunk: ResultTable) => void): Promise<void>;
}

/** The query an Output's rows are read by: the box's rows as it hands them on (handedOnRows), and its columns, each
 *  under the name it hands it on by. */
export async function outputQuery(snapshot: CubeSnapshot, configuration: Readonly<Record<string, unknown>>, queries: OutputQueries):
  Promise<{ readonly query: Lambda; readonly columns: readonly { readonly name: string; readonly type: string }[] }> {
  if (snapshot.pivotOn.length > 0 && (snapshot.pivotValues ?? []).length === 0) {
    throw new CubeRefusal('a pivoted grid\'s columns are its values: Load its values to fix them, or take its pivot off, to write it');
  }
  const rows = handedOnLambda(snapshot).body[0]!;
  const typed = (await queries.runQuery(columnsLambda(rows), snapshot)).rows.columns;
  const names = typed.map((c) => c.name);
  // the product's own columns (grid/columns.ts isMachinery: a Top N's rank) are none of its rows'
  const on = handedOn(names.filter((n) => !isMachinery(n)), snapshot, configuration);
  const query: Lambda = { _type: 'lambda', parameters: [], body: [handedOnRows(rows, names, on)] };
  return { query, columns: on.map((c) => ({ name: c.as, type: typed.find((t) => t.name === c.name)!.type })) };
}

/** A box's Output as CSV text (`bom`: a UTF-8 BOM first, as a file a spreadsheet may open has; on by default). */
export async function outputCsv(snapshot: CubeSnapshot, configuration: Readonly<Record<string, unknown>>, queries: OutputQueries,
  bom = true): Promise<string> {
  const { query, columns } = await outputQuery(snapshot, configuration, queries);
  const parts: string[] = [];
  await queries.streamQuery(query, snapshot, (chunk) => {
    const text = toCsv(chunk, { bom: bom && parts.length === 0, defuse: false });
    // its head row written once, with the file's first chunk
    parts.push(parts.length === 0 ? text : text.slice(text.indexOf('\r\n') + 2));
  });
  // no rows: its head row alone
  if (parts.length === 0) {
    const none = { columns: columns.map((c) => ({ ...c, values: [] })), rowCount: 0, epoch: snapshot.epoch, elapsedMs: 0 };
    parts.push(toCsv(none, { bom, defuse: false }));
  }
  return parts.join('');
}
