package com.gs.legend.compiler.typed;

/**
 * Join kind. Names match Pure's
 * {@code meta::pure::functions::relation::JoinKind} verbatim
 * ({@code INNER, LEFT_OUTER, RIGHT_OUTER, FULL_OUTER}); {@code CROSS} is
 * added for mapping-layer joins. Using Pure's spellings means
 * {@link Enum#valueOf} parses the EnumValue's name directly with no
 * translation step.
 */
public enum JoinType { INNER, LEFT_OUTER, RIGHT_OUTER, FULL_OUTER, CROSS }
