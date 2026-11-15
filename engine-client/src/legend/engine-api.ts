// THE ENGINE'S API AS TYPES (C5-T, 2026-11-15): legend-engine's refusal and the two interfaces a query app reaches an
// engine through -- Grammar (what the tab's planner can answer as well as a server) and Engine (everything else, the
// server's execute among it). Records: they plan nothing and send nothing, so the planner (wasm-grammar.ts), the server's
// planning client (planning-client.ts), the doors (engine.ts's HttpEngine and RoutedEngine) and the apps read them alike
// (//engine-client:records, tools/deps/ts-layers.txt). Until C5 they lived in engine.ts beside HttpEngine's execute.

import type { Lambda } from '../../../pure-protocol/src/index.ts';
import type { PureModelContextData } from './pmcd.ts';
import type {
  CompileResult, ExecuteInput, ExecutionResult, PureModelContext, RelationTypeAnswer,
} from './wire.ts';

/** A refusal from the engine, in its error shape (`{code, errorType?, message, status}`). */
export class EngineError extends Error {
  readonly status: number;
  readonly errorType: string | undefined;

  constructor(message: string, status: number, errorType?: string) {
    super(message);
    this.name = 'EngineError';
    this.status = status;
    this.errorType = errorType;
  }
}

/** Grammar and typing: what the tab's WASM planner can answer as well as a server. */
export interface Grammar {
  /** `grammar/grammarToJson/model`, without source information. */
  modelJson(text: string): Promise<PureModelContextData>;
  /** `grammar/grammarToJson/lambda`, without source information. */
  lambdaJson(text: string): Promise<Lambda>;
  /** `grammar/jsonToGrammar/lambda`. */
  lambdaText(lambda: Lambda, style: 'PRETTY' | 'STANDARD'): Promise<string>;
  /** `grammar/jsonToGrammar/model`: a model's protocol JSON as Pure text. */
  modelText(model: PureModelContextData, style: 'PRETTY' | 'STANDARD'): Promise<string>;
  /** `compilation/lambdaRelationType`. */
  relationType(model: PureModelContext, lambda: Lambda): Promise<RelationTypeAnswer>;
}

/** Everything else, which needs a server. */
export interface Engine extends Grammar {
  /** `compilation/compile`. */
  compile(model: PureModelContext): Promise<CompileResult>;
  /** `compilation/lambdaReturnType`: the result type's path. */
  returnType(model: PureModelContext, lambda: Lambda): Promise<string>;
  /** `execution/execute`. */
  execute(input: ExecuteInput, signal?: AbortSignal): Promise<ExecutionResult>;
  /** `execution/generatePlan`. */
  generatePlan(input: ExecuteInput): Promise<unknown>;
  /** `server/v1/currentUser`. */
  currentUser(): Promise<string>;
}
