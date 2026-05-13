package com.legend.parser.spec;

import com.legend.parser.Multiplicity;
import com.legend.parser.TypeExpression;

import java.util.List;
import java.util.Objects;

/**
 * Type annotation expression &mdash; the {@code @Type} form. Three
 * structural variants:
 *
 * <ul>
 *   <li>{@link Named}({@code typeName}) &mdash; a named type
 *       reference, possibly qualified ({@code @Integer},
 *       {@code @my::pkg::Foo}), possibly with type arguments
 *       carried verbatim as part of the {@code typeName} text
 *       ({@code @List<Integer>}, {@code @Map<String, Integer>}).
 *       Used most commonly as the second argument to
 *       {@code cast()} ({@code $x->cast(@Integer)}).</li>
 *   <li>{@link RelationShape}({@code columns}) &mdash; an inline
 *       relation type literal ({@code @Relation<(name:String,
 *       age:Integer)>}). Each {@link RelationShape.Column} carries
 *       a name (or wildcard, {@code ?}), a column-type annotation
 *       (which may itself be a {@link Wildcard}), and an optional
 *       multiplicity. Used in {@code cast()} when pivoting or
 *       otherwise changing the column shape of a relation.</li>
 *   <li>{@link Wildcard} &mdash; the {@code ?} placeholder. Only
 *       legal as the type of a {@link RelationShape.Column}; not a
 *       top-level annotation. The parser does not emit it from a
 *       bare {@code @?}.</li>
 * </ul>
 *
 * <h2>Why this is its own ValueSpecification variant</h2>
 *
 * <p>A type annotation in source code <em>is</em> a value position
 * &mdash; you can pass it to functions, store it in let-bindings,
 * etc. It can't be a function call ({@code cast<T|m>} takes the
 * annotation as a parameter, so the annotation must be a value);
 * it isn't a string ({@code @Relation<(...)>} has structured
 * content); it isn't a class instance ({@code Wildcard} doesn't
 * refer to a class). Engine-lite makes it a direct
 * {@link ValueSpecification} variant; engine-pure / legend-engine
 * wraps it in a {@code ClassInstance} envelope for protocol
 * serialisation but the underlying AST shape is the same. We
 * follow engine-lite (no protocol envelope at parse time; we are
 * building an AST, not serialising JSON).
 *
 * <h2>How {@code Named} carries generics</h2>
 *
 * <p>For non-relation generic types ({@code @List<Integer>}), the
 * type-argument list is collected verbatim into the {@code typeName}
 * string via the same token-concatenation mechanism
 * {@link com.legend.parser.SpecParser#parseTypeText() parseTypeText}
 * uses for typed lambda parameters. This loses inter-token
 * whitespace (the documented C.5 quirk: {@code "List< Integer >"}
 * becomes {@code "List<Integer>"} in the {@code typeName}). The
 * type-checker re-parses if it needs to inspect the args.
 *
 * <p>For {@code @Relation<(...)>} the inner parenthesised form is
 * recognised structurally as a {@link RelationShape} rather than
 * collapsed into a string &mdash; engine-lite, engine-pure, and
 * legend-engine protocol all preserve column structure here.
 *
 * <h2>Where this lives in the parser</h2>
 *
 * <p>Entry point is {@link com.legend.parser.SpecParser#parsePrimary}'s
 * {@code AT} case (the {@code @} symbol). Consumers downstream
 * receive a {@code TypeAnnotation} via the normal
 * {@link ValueSpecification} interface and dispatch on the variant
 * to interpret.
 */
public sealed interface TypeAnnotation
        extends ValueSpecification
        permits TypeAnnotation.Named,
                TypeAnnotation.RelationShape,
                TypeAnnotation.Wildcard {

    /**
     * Named type reference: a structured {@link TypeExpression} that
     * captures simple ({@code Integer}), qualified
     * ({@code my::pkg::Foo}), and generic ({@code List<Integer>},
     * {@code Map<String, List<V>>}) forms with full nested structure.
     *
     * <p>Previously this carried a raw {@link String} containing the
     * source-level type text, which lost whitespace and forced
     * downstream consumers (NameResolver, type checker) to re-parse.
     * The structured form is symmetric to lambda parameter types
     * ({@link Variable#type()}) and element-side property/parameter
     * types &mdash; a single AST walk rewrites every name reference
     * in NameResolver.
     *
     * @param type the structured type expression; never {@code null}
     */
    record Named(TypeExpression type) implements TypeAnnotation {
        public Named {
            Objects.requireNonNull(type, "type");
        }
    }

    /**
     * Inline relation type literal. Each column carries a name (or
     * wildcard), a column-type annotation, and an optional
     * multiplicity.
     *
     * <p>An empty column list is structurally legal in the parser
     * (e.g. {@code @Relation<()>}); any rejection lives in the
     * type-checker.
     *
     * @param columns the columns in source order; immutable, never
     *                {@code null}, may be empty
     */
    record RelationShape(List<Column> columns) implements TypeAnnotation {
        public RelationShape {
            Objects.requireNonNull(columns, "columns");
            columns = List.copyOf(columns);
        }

        /**
         * One column in a {@link RelationShape}.
         *
         * <p>The {@code name} field is {@code null} for wildcard
         * column-name slots ({@code ?:Type}); non-null otherwise.
         * Quoted column names ({@code 'My Col'}) are unquoted at
         * parse time so the stored value is the un-quoted, escape-
         * resolved string.
         *
         * <p>The {@code type} can itself be {@link Wildcard} for
         * the {@code name:?} form, matching engine-pure's
         * {@code mayColumnType: QUESTION | type} grammar.
         *
         * <p>The {@code multiplicity} is {@code null} when no
         * {@code [mult]} annotation is present in source. We
         * preserve it (rather than parse-and-discard like
         * engine-lite) because engine-pure's M3 metamodel keeps
         * column multiplicities on the structural type and a future
         * relation-type-checker will need them.
         *
         * @param name         column name; {@code null} for wildcard
         * @param type         column type annotation; never null;
         *                     may be {@link Wildcard}
         * @param multiplicity declared multiplicity, or {@code null}
         *                     when absent
         */
        public record Column(
                String name,
                TypeAnnotation type,
                Multiplicity multiplicity) {
            public Column {
                Objects.requireNonNull(type, "type");
            }
        }
    }

    /**
     * The {@code ?} wildcard. Used only as a column type inside a
     * {@link RelationShape.Column}; the parser does not admit bare
     * {@code @?} at the top level (engine grammar requires a
     * qualified name after {@code @}). Modelled as a record (rather
     * than an enum singleton) to keep the {@link TypeAnnotation}
     * sealed hierarchy uniformly record-shaped.
     */
    record Wildcard() implements TypeAnnotation {
    }
}
