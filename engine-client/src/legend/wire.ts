// LITE'S MODEL INPUT, as its text (lite's API v1 spec §1, 2026-11-17: a model travels as its text): the shape the apps
// hand a model in, which legend-engine's PureModelContextText shares. Types only. legend-engine's execution and typing
// shapes (ExecuteInput, its results, lambdaRelationType's answer, compilation/compile's) are the edge's
// (engine-wire.ts, //engine-client:edge; H11, 2026-11-17).

/** `PureModelContextText`: the model as its grammar (what legend-lite reads; the engine too). */
export interface PureModelContextText {
  readonly _type: 'text';
  readonly code: string;
}

export type PureModelContext = PureModelContextText;
