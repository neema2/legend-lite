package com.gs.legend.compiled;

import java.util.List;

/**
 * Compiled-state representation of a Pure database — the store target of
 * relational mappings. Contains included-database FQNs and one or more
 * compiled schemas. Full schema expansion (tables, views, joins, filters)
 * is deliberately minimal in Phase 1a and gets fleshed out alongside the
 * relational mapping work in later phases.
 */
public record CompiledDatabase(
        String qualifiedName,
        List<String> includedDatabaseFqns,
        List<CompiledSchema> schemas,
        SourceLocation sourceLocation) implements CompiledElement {
}
