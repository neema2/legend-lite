// THE DOOR TO A PERSON'S FILE PUT INTO A TABLE (C5-T, 2026-11-15: the TypeScript split of plan and run, as Java's doors
// only compose): the planner asked for the put's plan (Boundary.fileLoad) outside any run, then the plan run inside a
// run frame (run-frame.ts: a run plans nothing) by model-data.ts putFile -- registered, loaded in one transaction, let
// go of. Until then model-data.ts loadFileTable asked the planner itself.

import { fileLoadRequest, filePut, putFile, type FilePut, type FileSink } from '../model-data.ts';
import type { PlanText } from '../planned-sql.ts';
import type { FileLoadSource } from '../planner-sources.ts';
import { inRun } from '../run-frame.ts';

/** A declared table: the Database that declares it, its schema and name. */
type Target = { readonly database: string; readonly schema: string; readonly table: string };

/** `file` ready to be put into `target` as `as`, and the planner's plan for it: asked before any run that puts it. */
export async function planFilePut(planner: FileLoadSource, databaseType: string, model: string, target: Target,
  file: { readonly name: string; readonly bytes: Uint8Array }, as: string): Promise<{ readonly put: FilePut; readonly plan: PlanText }> {
  const put = await filePut(file, as);
  return { put, plan: await planner.fileLoad(fileLoadRequest(databaseType, model, target, put)) };
}

/** `file` put into `target` (a table `model`'s Database declares) as `as`: planned, then run. */
export async function loadFileTable(sink: FileSink, planner: FileLoadSource, databaseType: string, model: string,
  target: Target, file: { readonly name: string; readonly bytes: Uint8Array }, as: string): Promise<void> {
  const { put, plan } = await planFilePut(planner, databaseType, model, target, file, as);
  await inRun("a person's file put", () => putFile(sink, plan, put));
}
