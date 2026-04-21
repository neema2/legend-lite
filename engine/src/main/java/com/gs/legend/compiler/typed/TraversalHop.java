package com.gs.legend.compiler.typed;

/**
 * One hop in a traverse-extend chain.
 *
 * <p>Captures a single join step: the target table name resolved from the
 * hop's target expression, plus a 2-parameter lambda {@code (prev, hop) ->
 * Boolean} expressing the join condition. The parameter names are encoded
 * inside {@link TypedLambda#params()} so PlanGenerator can re-bind them when
 * lowering to relational joins.
 *
 * @param tableName Fully-qualified target table name as resolved against
 *                  the store (matches the old {@code TypeInfo.TraversalHop}
 *                  tableName).
 * @param condition Type-checked join predicate. Its two params correspond,
 *                  in order, to the previous-table row and the hop-table
 *                  row.
 */
public record TraversalHop(
        String tableName,
        TypedLambda condition
) {}
