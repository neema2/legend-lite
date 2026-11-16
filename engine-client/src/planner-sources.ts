// THE PLANNER'S QUESTIONS A HOST ASKS (C5-T, 2026-11-15: the TypeScript split of plan and run,
// docs/TOTAL_WALL_START_HERE_2026_11_11.md section 6.1): what legend-lite's planner answers a host -- WasmGrammar in
// the tab, DataCube's LitePlanner -- each request a record of requests.ts and each answer a plan or the plan's
// statements. A planner module: only the planner implements these, and only a door or an app asks them. The run side
// never names one (//engine-client:run's deps hold no planner module, tools/deps/ts-layers.txt): a run function takes
// the planner's ANSWER, which its door asked for first. Until C5 they were records beside the requests.

import type { PlannedSql, PlanText } from './planned-sql.ts';
import type {
  FileLoadRequest, HostOperation, IngestPlans, IngestPlansRequest, ProjectDataStatements, RemoteMountRequest,
  SnapCopyRequest, SnapStreamRequest, SynthesizeRequest,
} from './requests.ts';

/** Where a remote file's mount's plan comes from: legend-lite's planner (WasmGrammar, DataCube's LitePlanner). */
export interface RemoteMountSource {
  /** The plan, PlanJson's text: one HOST_SESSION load the hosts' runner runs (runLoad), binding what it names. */
  remoteMount(request: RemoteMountRequest): Promise<PlanText>;
}

/** Where a snapshot's stream's and copy's plans come from: legend-lite's planner (WasmGrammar, DataCube's LitePlanner). */
export interface SnapStreamSource {
  /** The plan, PlanJson's text: one HOST_SESSION load whose stream step the hosts' runner runs (runLoad). */
  snapStream(request: SnapStreamRequest): Promise<PlanText>;
  /** The plan, PlanJson's text: one write -- its schema, the table there before dropped, the table made from the pull
   *  (its data: payloads bound by digest) -- in its target's transaction, which the hosts' runner runs (runPlan). */
  snapCopy?(request: SnapCopyRequest): Promise<PlanText>;
}

/** Where a synthesized table's plan comes from: legend-lite's planner (WasmGrammar, DataCube's LitePlanner). */
export interface SynthesizeSource {
  /** The plan, PlanJson's text: one HOST_SESSION load (the table made as declared, then one statement filling it). */
  synthesize(request: SynthesizeRequest): Promise<PlanText>;
}

/** What answers a host's operations: legend-lite's planner (WasmGrammar, DataCube's LitePlanner). */
export interface HostOperationsSource {
  /** Each operation's statements, in order, for a database of `databaseType`. */
  hostOperations(databaseType: string, operations: readonly HostOperation[]): Promise<PlannedSql[][]>;
}

/** Where a host's file's plan comes from: legend-lite's planner (WasmGrammar.fileLoad, DataCube's LitePlanner). */
export interface FileLoadSource {
  /** The plan, PlanJson's text: one HOST_SESSION load the hosts' runner runs (runLoad). */
  fileLoad(request: FileLoadRequest): Promise<PlanText>;
}

/** Where a project's own database's statements come from: legend-lite's planner (WasmGrammar, DataCube's LitePlanner). */
export interface ProjectDataSource {
  /** Named by what it holds (the model's Databases and Data rows): an edited row is a fresh database. */
  projectData(model: string, databaseType: string): Promise<ProjectDataStatements>;
}

/** What the tab's DuckDB is set up by: legend-lite's planner (WasmGrammar, DataCube's LitePlanner) -- its host
 *  operations (the extension folder) and its session setup (doors/tab-duckdb.ts asks both). */
export interface TabDuckDbPlanner extends HostOperationsSource {
  /** The planner's session setup for a database's type (Boundary.sessionSetup: a DuckDB session in UTC). */
  sessionSetup(databaseType: string): Promise<readonly PlannedSql[]>;
}

/** Where a dataset's landing plans come from: legend-lite's planner (WasmGrammar, DataCube's LitePlanner). */
export interface IngestPlansSource {
  ingestPlans(request: IngestPlansRequest): Promise<IngestPlans>;
}
