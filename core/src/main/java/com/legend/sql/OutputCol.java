package com.legend.sql;

import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;

/**
 * One output column of a query node, WITH its Pure type &mdash; the typed-results
 * contract (PHASE_HIJ_LOWERING.md): type information rides the plan so result
 * consumers never inspect SQL/JDBC types. Stamped by the lowering from the
 * HIR's schemas.
 */
public record OutputCol(String name, Type type, Multiplicity multiplicity) {
}
