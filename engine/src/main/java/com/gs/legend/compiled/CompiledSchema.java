package com.gs.legend.compiled;

import java.util.List;

/**
 * Compiled-state database schema. Placeholder shape for Phase 1a; the full
 * expansion (tables, views, joins, filters) lands alongside the relational
 * mapping work in later phases.
 *
 * <p>Field set is deliberately minimal until Phase 1b wires up
 * {@code compileDatabase} and discovers the exact downstream requirements.
 */
public record CompiledSchema(
        String name,
        List<String> tableFqns,
        List<String> viewFqns,
        List<String> joinFqns,
        List<String> filterFqns) {
}
