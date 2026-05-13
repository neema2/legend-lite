package com.legend.parser;

import java.util.List;
import java.util.Objects;

/**
 * Structured representation of a Pure type expression. Replaces the
 * pre-flip {@code String type} fields throughout the parser AST so that
 * {@link com.legend.parser.NameResolver} (when added) can rewrite simple
 * names to FQNs by a single tree-walk, and so the downstream type checker
 * receives structured input rather than re-parsing strings.
 *
 * <h2>Variants</h2>
 * <ul>
 *   <li>{@link NameRef} &mdash; bare name reference: {@code Integer},
 *       {@code T}, {@code model::Order}. Most common case; the only one
 *       NameResolver needs to rewrite directly. Name matches engine's
 *       {@code Type.NameRef} (the pre-classification pointer that the
 *       type-checker later refines to {@code ClassType}, {@code EnumType},
 *       {@code Primitive}, or a type-variable binding).</li>
 *   <li>{@link Generic} &mdash; generic application: {@code List<Integer>},
 *       {@code Map<String,Person>}, {@code Relation<T>}. Arguments are
 *       themselves {@code TypeExpression}, recursively structured.</li>
 *   <li>{@link FunctionType} &mdash; {@code {ParamType[mult],...->ResultType[mult]}}.
 *       Carries a list of typed parameters and a typed result. The most
 *       common form inside native function generic arguments
 *       ({@code Function<{T[1]->Boolean[1]}>}).</li>
 *   <li>{@link RelationType} &mdash; {@code (col:Type[mult], ...)} &mdash;
 *       inline relation type literal. Paren-delimited, not braces (braces
 *       in this position are always function types). Column multiplicity
 *       defaults to {@code [1]} when not written; mirrors engine's
 *       {@code Type.RelationTypeVar} shape.</li>
 *   <li>{@link SchemaAlgebra} &mdash; the schema operations used inside
 *       native generic arguments: {@code T+Z} (union), {@code T-Z}
 *       (difference), {@code Z\u2286T} (subset), {@code Z=K} (equal).
 *       Mirrors engine's {@code Type.SchemaAlgebra} exactly, including
 *       operator precedence (equal binds tightest, then +/- chain, then
 *       subset).</li>
 * </ul>
 *
 * <h2>Why these variants and not more</h2>
 *
 * <p>The shapes here match engine's {@code Type.java} structurally:
 * {@code NameRef} matches engine's {@code Type.NameRef};
 * {@code Generic} matches engine's {@code Type.GenericType} (less the
 * post-resolution {@code rawType} classification);
 * {@code FunctionType} matches engine's {@code Type.FunctionType};
 * {@code RelationType} matches engine's {@code Type.RelationTypeVar};
 * {@code SchemaAlgebra} matches engine's {@code Type.SchemaAlgebra}.
 * No verbatim-text fallback &mdash; every form in the corpus has a
 * structured representation.
 *
 * <h2>Equality</h2>
 *
 * <p>Records give us structural equality for free. Two type expressions
 * are equal iff they have the same shape with equal sub-expressions.
 * Whitespace and source-position differences are erased &mdash; that is
 * an improvement over the pre-flip string-equality semantics where
 * {@code "List<T>"} and {@code "List< T >"} were unequal.
 */
public sealed interface TypeExpression {

    /** Bare name reference. Pre-NameResolver may be simple ({@code "Integer"}),
     *  qualified ({@code "model::Person"}), a primitive, or a type-parameter
     *  binder ({@code "T"}). Post-NameResolver every non-binder is FQN.
     *  Name and shape match engine's {@code Type.NameRef}. */
    record NameRef(String name) implements TypeExpression {
        public NameRef {
            Objects.requireNonNull(name, "name");
        }
    }

    /** Generic type application: {@code name<arg, arg, ...>}. The
     *  {@code name} slot is itself a (possibly qualified) identifier;
     *  it is stored flat rather than wrapped in a {@link NameRef} so
     *  that resolving the head and resolving the arguments are obvious
     *  separate steps in any walk. */
    record Generic(String name, List<TypeExpression> arguments)
            implements TypeExpression {
        public Generic {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(arguments, "arguments");
            arguments = List.copyOf(arguments);
        }
    }

    /** Function type: {@code {ParamType[mult], ... -> ResultType[mult]}}.
     *  Zero parameters is legal ({@code {->R[1]}}). The {@code result}
     *  slot is non-null; the absence of a result is not a representable
     *  state. */
    record FunctionType(
            List<TypedParameter> parameters,
            TypedParameter result) implements TypeExpression {
        public FunctionType {
            Objects.requireNonNull(parameters, "parameters");
            Objects.requireNonNull(result, "result");
            parameters = List.copyOf(parameters);
        }
    }

    /** Inline relation type literal: {@code (col:Type[mult], ...)}.
     *  Paren-delimited (not brace-delimited); braces in type position
     *  are always function types. An empty column list is legal at
     *  this layer (rejection, if any, belongs in the type checker).
     *  Mirrors engine's {@code Type.RelationTypeVar}. */
    record RelationType(List<Column> columns) implements TypeExpression {
        public RelationType {
            Objects.requireNonNull(columns, "columns");
            columns = List.copyOf(columns);
        }
    }

    /** Schema-algebra operation on type expressions: {@code T+Z},
     *  {@code T-Z}, {@code Z\u2286T}, {@code Z=K}. Binary; chained
     *  forms (e.g. {@code T-Z+V}) build a left-leaning tree. Precedence
     *  from engine's {@code parseTypeWithOperation}: {@code EQUAL}
     *  binds tightest, then {@code UNION}/{@code DIFFERENCE} chain,
     *  then {@code SUBSET}. */
    record SchemaAlgebra(TypeExpression left, Op op, TypeExpression right)
            implements TypeExpression {
        public SchemaAlgebra {
            Objects.requireNonNull(left, "left");
            Objects.requireNonNull(op, "op");
            Objects.requireNonNull(right, "right");
        }
    }

    /** Schema-algebra operator. Names match engine's
     *  {@code Type.SchemaAlgebra.Op}. */
    enum Op { EQUAL, UNION, DIFFERENCE, SUBSET }

    /** A type with its multiplicity, used inside {@link FunctionType}. */
    record TypedParameter(TypeExpression type, Multiplicity multiplicity) {
        public TypedParameter {
            Objects.requireNonNull(type, "type");
            Objects.requireNonNull(multiplicity, "multiplicity");
        }
    }

    /** A column in a {@link RelationType}: name + type + multiplicity.
     *  Multiplicity is never null &mdash; engine defaults to
     *  {@code [1]} when the source does not declare one, and we match.
     *  The {@code name} slot accepts {@code "?"} for wildcard column
     *  positions used in the rename DSL
     *  ({@code Z=(?:K)\u2286T}); engine stores the literal {@code "?"}
     *  string and we do too. */
    record Column(String name, TypeExpression type, Multiplicity multiplicity) {
        public Column {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(type, "type");
            Objects.requireNonNull(multiplicity, "multiplicity");
        }
    }

    /** Convenience: build a {@link NameRef}. Reads more naturally at
     *  call sites that construct lots of bare references in a row
     *  (tests, fixtures). */
    static TypeExpression nameRef(String name) {
        return new NameRef(name);
    }
}
