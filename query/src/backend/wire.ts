// The model's input (lite's, engine-client/src/legend/wire.ts) and the saved query's (query-store/src/wire.ts), as
// Query's screens read them: one place to import both from. legend-engine's wire shapes are the edge's (H11,
// 2026-11-17: engine-client/src/legend/engine-wire.ts), never read here.

export * from '../../../engine-client/src/legend/wire.ts';
export {
  QUERY_PROFILE,
  type Query, type QueryExecutionContext, type QuerySearchSortBy, type QuerySearchSpecification,
  type QueryStereotype, type QueryTaggedValue,
} from '../../../query-store/src/wire.ts';
