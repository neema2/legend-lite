package org.finos.legend.pure.dsl.definition;

/**
 * Sealed interface representing top-level Pure definitions.
 * 
 * These are parsed from Pure source files containing:
 * - Class definitions
 * - Database (Store) definitions
 * - Mapping definitions
 */
public sealed interface PureDefinition 
        permits ClassDefinition, DatabaseDefinition, MappingDefinition {
}
