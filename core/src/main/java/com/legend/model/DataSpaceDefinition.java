package com.legend.model;

import java.util.List;
import java.util.Objects;

/**
 * A parsed {@code DataSpace} element &mdash; a curated view over mappings,
 * runtimes and executables that Studio surfaces as a product. Nothing in
 * legend-lite executes a data space; the record exists so {@code ###DataSpace}
 * sections parse to TYPED, indexed elements instead of failing or hiding
 * (the opaque carrier is locked to the overlay seam by design).
 *
 * @param qualifiedName            fully qualified name
 * @param executionContexts        the named mapping/runtime contexts
 * @param defaultExecutionContext  name of the default context
 * @param title                    display title, or null
 * @param description              display description, or null
 * @param executables              curated executables (services, functions,
 *                                 inline queries)
 * @param diagrams                 referenced diagrams
 * @param supportInfoSource        raw {@code supportInfo:} payload (kind +
 *                                 body), or null
 * @param elements                 element-scope paths as written, exclusions
 *                                 keeping their {@code -} prefix
 */
public record DataSpaceDefinition(
        String qualifiedName,
        List<ExecutionContext> executionContexts,
        @com.legend.Nullable String defaultExecutionContext,
        @com.legend.Nullable String title,
        @com.legend.Nullable String description,
        List<Executable> executables,
        List<Diagram> diagrams,
        @com.legend.Nullable String supportInfoSource,
        List<String> elements) implements PackageableElement {

    public DataSpaceDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        executionContexts = List.copyOf(executionContexts);
        executables = List.copyOf(executables);
        diagrams = List.copyOf(diagrams);
        elements = List.copyOf(elements);
    }

    /** One {@code executionContexts:} entry; {@code testDataSource} carries
     *  the raw {@code testData:} payload (kind + island), if any. */
    /** Since 4.145.0 a context names EITHER a mapping OR a mapping provider
     *  (the provider rides the protocol record only; here {@code mapping}
     *  is null for it), and the default runtime is optional. */
    public record ExecutionContext(String name,
            @com.legend.Nullable String title,
            @com.legend.Nullable String description,
            @com.legend.Nullable String mapping,
            @com.legend.Nullable String defaultRuntime,
            @com.legend.Nullable String testDataSource) {
        public ExecutionContext {
            Objects.requireNonNull(name, "Context name cannot be null");
        }
    }

    /** One {@code executables:} entry — a path-referencing executable OR an
     *  inline query ({@code querySource} raw), never both. */
    public record Executable(@com.legend.Nullable String id, String title,
            @com.legend.Nullable String description,
            @com.legend.Nullable String executable,
            @com.legend.Nullable String querySource,
            @com.legend.Nullable String executionContextKey) {
        public Executable {
            Objects.requireNonNull(title, "Executable title cannot be null");
        }
    }

    /** One {@code diagrams:} entry. */
    public record Diagram(String title,
            @com.legend.Nullable String description, String diagram) {
        public Diagram {
            Objects.requireNonNull(title, "Diagram title cannot be null");
            Objects.requireNonNull(diagram, "Diagram path cannot be null");
        }
    }
}
