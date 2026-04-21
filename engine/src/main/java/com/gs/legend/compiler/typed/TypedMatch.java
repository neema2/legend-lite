package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;
import java.util.List;

/** Pattern match: {@code subject->match([a | ..., b | ...])}. */
public record TypedMatch(
        TypedSpec subject,
        List<TypedLambda> cases,
        ExpressionType info
) implements TypedSpec {}
