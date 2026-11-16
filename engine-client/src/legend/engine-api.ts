// LEGEND-ENGINE'S REFUSAL (C5-T, 2026-11-15): the error shape both sides read -- the planner (wasm-grammar.ts), the
// server's planning client (planning-client.ts), the server's engines (engine.ts) and the apps. A record: it plans
// nothing and sends nothing (//engine-client:records). The interfaces a query app reaches an engine through left the
// records with the C5-T audit's item 3 (2026-11-15), so the records hand the run side no typed way into the planner:
// Grammar, the planner's (legend/grammar.ts, //engine-client:planner), and Engine, the server's (legend/engine.ts).

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
