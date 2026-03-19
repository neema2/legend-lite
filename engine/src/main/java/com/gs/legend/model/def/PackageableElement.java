package com.gs.legend.model.def;
import com.gs.legend.model.m3.*;
import com.gs.legend.model.store.*;
import com.gs.legend.model.mapping.*;
/**
 * Sealed interface representing top-level Pure definitions (packageable elements).
 *
 * Mirrors the legend-engine M3 metamodel where PackageableElement and
 * ValueSpecification are siblings under Any. Every packageable element
 * has a qualified name (e.g., "model::Person") and lives in a package.
 *
 * Parsed from Pure source files containing:
 * - Class definitions
 * - Association definitions
 * - Enum definitions
 * - Database (Store) definitions
 * - Mapping definitions
 * - Service definitions
 * - Connection/Runtime definitions
 * - Profile definitions
 * - Function definitions
 */
public sealed interface PackageableElement
                permits ClassDefinition, AssociationDefinition, DatabaseDefinition, MappingDefinition,
                ServiceDefinition, EnumDefinition, ProfileDefinition, FunctionDefinition,
                ConnectionDefinition, RuntimeDefinition {

    /** Fully qualified name, e.g. "model::Person" */
    String qualifiedName();

    /** Simple name without package, e.g. "Person" */
    default String simpleName() {
        String qn = qualifiedName();
        int idx = qn.lastIndexOf("::");
        return idx >= 0 ? qn.substring(idx + 2) : qn;
    }

    /** Package path without element name, e.g. "model" */
    default String packagePath() {
        String qn = qualifiedName();
        int idx = qn.lastIndexOf("::");
        return idx >= 0 ? qn.substring(0, idx) : "";
    }
}
