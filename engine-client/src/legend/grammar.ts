// GRAMMAR AND TYPING, the planner's interface (C5-T audit, item 3, 2026-11-15: out of the records, engine-api.ts): what
// the tab's WASM planner answers (wasm-grammar.ts) as well as a server (planning-client.ts) -- text to protocol JSON and
// back, and a query's relation type. legend-engine's grammar routes, the edge's (H11, 2026-11-17: //engine-client:edge,
// out of the planner; lite's own planner client is wasm-grammar.ts WasmGrammar): no product module reaches it.

import type { Lambda } from '../../../pure-protocol/src/index.ts';
import type { PureModelContextData } from './pmcd.ts';
import type { RelationTypeAnswer } from './engine-wire.ts';
import type { PureModelContext } from './wire.ts';

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
