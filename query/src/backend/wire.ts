// The engine's wire shapes (engine-client's in-tab legend engine, engine-client/src/legend/wire.ts) and the saved
// query's (query-store/src/wire.ts), as Query's screens read them: one place to import both from.

export * from '../../../engine-client/src/legend/wire.ts';
export {
  QUERY_PROFILE,
  type Query, type QueryExecutionContext, type QuerySearchSortBy, type QuerySearchSpecification,
  type QueryStereotype, type QueryTaggedValue,
} from '../../../query-store/src/wire.ts';
