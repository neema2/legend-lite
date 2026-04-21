package com.gs.legend.compiler.typed;

/**
 * Join kind. Covers Pure's {@code meta::pure::functions::relation::JoinKind}
 * ({@code INNER, LEFT, RIGHT, FULL}) plus {@code CROSS} from mapping-layer joins.
 */
public enum JoinType { INNER, LEFT, RIGHT, FULL, CROSS }
