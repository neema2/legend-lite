package com.gs.legend.ast;

import com.gs.legend.model.ModelContext;
import com.gs.legend.model.m3.Type;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Purely syntactic representation of the Pure {@code @Something} type annotation
 * as it appears in user source code — the AST node produced by the parser when
 * a type is passed as a value (e.g., {@code $x->cast(@Integer)}).
 *
 * <p>Following the separation-of-phases discipline every real compiler uses, a
 * {@code TypeAnnotation} carries <strong>only syntactic structure</strong> —
 * strings and nested annotations, never resolved {@link Type} values. The
 * parser has no symbol table / {@code ModelContext} and must not speculate
 * about whether a name is a primitive, class, or enum. Classification happens
 * at type-check time via {@link #resolve(ModelContext)}.
 *
 * <p>Three grammar productions map to three record variants:
 * <ul>
 *   <li>{@link Named} — scalar references like {@code @Integer}, {@code @Firm},
 *       {@code @my::pkg::Thing}.</li>
 *   <li>{@link RelationShape} — structural
 *       {@code @Relation<(col:Type, ...)>} with column entries whose
 *       types are themselves nested {@code TypeAnnotation} values (recursive).</li>
 *   <li>{@link Wildcard} — the {@code ?} column-type wildcard inside a
 *       {@code RelationShape}. Never appears at the top level.</li>
 * </ul>
 */
public sealed interface TypeAnnotation extends ValueSpecification {

    /** Scalar type reference: {@code @Integer}, {@code @Firm}, {@code @my::pkg::Thing}. */
    record Named(String fullPath) implements TypeAnnotation {
        public Named {
            Objects.requireNonNull(fullPath, "Named.fullPath must not be null");
        }
    }

    /** Structural relation type: {@code @Relation<(col:Type, ...)>}. */
    record RelationShape(List<Column> columns) implements TypeAnnotation {
        public RelationShape {
            Objects.requireNonNull(columns, "RelationShape.columns must not be null");
            columns = List.copyOf(columns);
        }

        /**
         * One column of a {@link RelationShape}. {@code name} is the source-level
         * column identifier (may be {@code "?"} for a wildcard column name).
         * {@code type} is recursive — another {@code TypeAnnotation}.
         *
         * <p>Note: per-column multiplicity (e.g., {@code [1]}, {@code [*]}) is not
         * carried here because {@link Type.Schema} has no per-column multiplicity slot
         * yet. When the Schema gains one, thread the parser text through this record.
         */
        public record Column(String name, TypeAnnotation type) {
            public Column {
                Objects.requireNonNull(name, "Column.name must not be null");
                Objects.requireNonNull(type, "Column.type must not be null");
            }
        }
    }

    /** Wildcard column type: {@code ?} inside a {@link RelationShape}. */
    record Wildcard() implements TypeAnnotation {}

    // ============================================================
    //  Check-time resolver (the ONLY resolver)
    // ============================================================

    /**
     * Resolves this syntactic annotation to the semantic {@link Type} it denotes
     * against the given {@link ModelContext}.
     *
     * <p>Strict — throws on unknown names via {@link Type#resolve(String, ModelContext)}.
     * Never returns a leaky default.
     */
    default Type resolve(ModelContext ctx) {
        Objects.requireNonNull(ctx, "ModelContext must not be null");
        return switch (this) {
            case Named(String path) -> Type.resolve(path, ctx);
            case Wildcard() -> Type.Primitive.ANY;
            case RelationShape(List<RelationShape.Column> cols) -> {
                Map<String, Type> m = new LinkedHashMap<>();
                for (var c : cols) {
                    m.put(c.name(), c.type().resolve(ctx));
                }
                yield new Type.Relation(Type.Schema.withoutPivot(m));
            }
        };
    }
}
