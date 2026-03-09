package org.finos.legend.pure.dsl.ast;

import org.finos.legend.engine.plan.RelationType;
import org.finos.legend.engine.store.RelationalMapping;

import java.util.IdentityHashMap;

/**
 * Output of the {@link CleanCompiler} compilation pass.
 *
 * <p>
 * Contains the unchanged AST, a side table of per-node type information,
 * and the resolved top-level mapping. The AST is NOT modified — type info
 * is stored separately in the side table, following the pattern used by
 * mature compilers (rustc, rust-analyzer) for clean separation.
 *
 * <p>
 * Consumed by {@link PlanGenerator} which walks the AST and looks up type
 * info as needed to generate SQL.
 *
 * @param ast     The unchanged parse tree (same object references)
 * @param types   Per-node type info: {@code IdentityHashMap<node, TypeInfo>}
 * @param mapping Top-level resolved class→table mapping (may be null)
 */
public record CompilationResult(
        ValueSpecification ast,
        IdentityHashMap<ValueSpecification, TypeInfo> types,
        RelationalMapping mapping) {

    /** Looks up the type info for a node, throwing if not found. */
    public TypeInfo requireType(ValueSpecification node) {
        TypeInfo info = types.get(node);
        if (info == null) {
            throw new IllegalStateException(
                    "No type info for node: " + node.getClass().getSimpleName());
        }
        return info;
    }

    /** Looks up the type info for a node, returning null if not found. */
    public TypeInfo getType(ValueSpecification node) {
        return types.get(node);
    }

    /** Gets the result RelationType of the top-level expression. */
    public RelationType resultType() {
        TypeInfo info = types.get(ast);
        return info != null ? info.relationType() : RelationType.empty();
    }
}
