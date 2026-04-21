package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;
import java.util.List;

/**
 * Relational zip (outer/inner merge by shared keys). Placeholder — carries sources
 * and by-keys verbatim; refine shape when finalizing PlanGen zip rendering.
 */
public record TypedZip(
        List<TypedSpec> sources,
        List<String> byKeys,
        ExpressionType info
) implements TypedSpec {}
