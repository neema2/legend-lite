package org.finos.legend.pure.dsl.definition;

/**
 * Sealed interface representing top-level Pure definitions.
 * 
 * These are parsed from Pure source files containing:
 * - Class definitions
 * - Association definitions
 * - Database (Store) definitions
 * - Mapping definitions
 */
public sealed interface PureDefinition
                permits ClassDefinition, AssociationDefinition, DatabaseDefinition, MappingDefinition,
                ServiceDefinition, EnumDefinition, ProfileDefinition, FunctionDefinition,
                ConnectionDefinition, RuntimeDefinition {
}
