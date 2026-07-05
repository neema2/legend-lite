package com.legend.sql;

/**
 * One output column of a query node, in the SQL layer's own type vocabulary
 * (LEGEND_SQL_VISION.md). Frontends stamp these at the lowering boundary;
 * result layers that need frontend types (Pure) read them from the FRONTEND's
 * typed root, never from the plan.
 */
public record OutputCol(String name, SqlType type, boolean nullable) {
}
