package org.finos.legend.pure.dsl.ast;

import org.finos.legend.engine.plan.RelationType;
import org.finos.legend.engine.store.RelationalMapping;

/**
 * Per-node type information stored in the compilation side table.
 *
 * <p>
 * During the compile pass, {@link CleanCompiler} computes a {@code TypeInfo}
 * for every {@link ValueSpecification} node in the AST. The SQL generation
 * pass ({@link SqlCompiler}) looks up this info to understand column types,
 * resolve property→column mappings, and validate references.
 *
 * <p>
 * Designed for Rust portability: side table pattern
 * ({@code HashMap<NodeId, TypeInfo>}) works cleanly in both Java and Rust.
 *
 * @param relationType The output columns of this expression (null for scalars)
 * @param mapping      Resolved class→table mapping (null when no class context)
 */
public record TypeInfo(
        RelationType relationType,
        RelationalMapping mapping) {

    /** Creates a TypeInfo for a scalar (non-relational) expression. */
    public static TypeInfo scalar() {
        return new TypeInfo(null, null);
    }

    /** Creates a TypeInfo with type info but no mapping. */
    public static TypeInfo of(RelationType relationType) {
        return new TypeInfo(relationType, null);
    }

    /** Full constructor with both type and mapping. */
    public static TypeInfo of(RelationType relationType, RelationalMapping mapping) {
        return new TypeInfo(relationType, mapping);
    }

    /** Returns true if this represents a relational (table-like) result. */
    public boolean isRelational() {
        return relationType != null && !relationType.columns().isEmpty();
    }
}
