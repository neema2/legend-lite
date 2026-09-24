// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.builtin.Pure;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.protocol.TypeExpression;
import com.legend.protocol.spec.TypeAnnotation;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Type-ANNOTATION resolution — {@code @Integer}, {@code @Pair<String,Integer>},
 * {@code @Relation<(a:String[1])>}, {@code {x:T[1]|…}} parameter types — the
 * one place a spelled type becomes a {@link Type}. Carries the ENCLOSING
 * FUNCTION's declared type parameters as a frame (one per body being typed,
 * pushed by {@link Typer#inFunctionScope}): a name in the frame is that type
 * VARIABLE, never a model lookup (census batch 149: {@code cast(@T)} inside a
 * generic body). Split from {@code Typer} for size (CodeShape guard).
 */
final class TypeAnnotations {

    private final ModelContext ctx;
    private final java.util.ArrayDeque<List<String>> typeParameterFrames =
            new java.util.ArrayDeque<>();

    TypeAnnotations(ModelContext ctx) {
        this.ctx = ctx;
    }

    /** Resolve under {@code typeParameters} as the enclosing frame. */
    <R> R inFrame(List<String> typeParameters, java.util.function.Supplier<R> body) {
        typeParameterFrames.push(typeParameters);
        try {
            return body.get();
        } finally {
            typeParameterFrames.pop();
        }
    }

    Type annotationType(TypeAnnotation ta) {
        return switch (ta) {
            case TypeAnnotation.Named n -> namedType(n.type());
            // @Relation<(…)> is a TABLE target — pure's own spelling,
            // kept WRAPPED (Row-vs-Relation: the G-α unwrap is deleted;
            // cast(@Relation<(…)>) yields the wrapped type every
            // relation op emits).
            case TypeAnnotation.RelationShape rs ->
                    Type.relation(relationShapeType(rs));
            // @[m]: a prototype of Any — the multiplicity rides the value's stamp (Typer.typeRef)
            case TypeAnnotation.MultiplicityRef ignored ->
                    new Type.ClassType(com.legend.compiler.element.type.PlatformTypes.ANY);
            case TypeAnnotation.Wildcard ignored -> throw new TypeInferenceException(
                    "the ? wildcard is only legal as a column type inside @Relation<(…)>");
        };
    }

    /**
     * A named type reference used in a value position ({@code @Integer},
     * {@code t:Person[1]|…} branch/parameter declarations). Names are FQN-resolved
     * by NameResolver in the full pipeline; for primitive short names (the prelude)
     * we fall back to the fixed primitive package, so direct query checking
     * ({@code @Integer}) works without an import scope. Package-private: the
     * checkers that read declared types ({@code match} branches, {@code eval}
     * lambda params) resolve through this single point.
     */
    /** Strictness = EMPTY-PRESERVING composition over at least one $this
     * read. The BANNED set is exactly the constructs that produce a
     * NON-EMPTY value from an EMPTY input (conditionals, emptiness
     * tests, reducers over possibly-empty collections); plain property
     * chains, scalar natives and empty-preserving collection ops
     * (filter/map/toOne/first...) propagate emptiness in SQL as null —
     * pure's auto-map result. A literal-only body has no $this read and
     * fails the sawThis requirement (the manufactured-constant case,
     * audit 22a H2). Unknown node kinds are conservatively non-strict. */

    /** bit 0 = saw a $this read; bit 1 = saw a non-strict construct. */

    Type namedType(TypeExpression te) {
        // GENERIC annotations (@Pair<String, Integer>): the base resolves
        // like a NameRef; arguments resolve recursively.
        if (te instanceof TypeExpression.Generic g) {
            Type base = namedType(new TypeExpression.NameRef(g.name()));
            java.util.List<Type> args = g.arguments().stream()
                    .map(this::namedType).toList();
            String fqn = base instanceof Type.ClassType ct ? ct.fqn()
                    : base instanceof Type.GenericType gt ? gt.rawFqn() : null;
            if (fqn == null && args.isEmpty() && !g.typeVariableValues().isEmpty()) {
                // @P(8) over `Primitive P(x:Integer[1]) extends Integer` (cast.pure):
                // the VALUES are the primitive's constraint inputs, not type
                // arguments — the annotated type is the primitive itself
                return base;
            }
            if (fqn == null) {
                throw new TypeInferenceException(
                        "generic annotation over a non-class type: " + g.name());
            }
            // the MULTIPLICITY arguments ride too (@Column<Nil,Z|0..1> —
            // relation::eval's own body): dropping them typed the column's
            // eval at [*] where upstream's result is [0..1]
            return new Type.GenericType(fqn, args, g.multiplicityArguments().stream()
                    .map(Multiplicity::ofArgument).toList());
        }
        // FUNCTION-TYPE annotations (f:Function<{T[1]->R[*]}>[1] spelled
        // structurally — domainManagement/tds postprocessor library
        // params): same conversion TypeClassifier applies to signatures
        if (te instanceof TypeExpression.FunctionType ft) {
            java.util.List<com.legend.compiler.element.type.Type.Param> ps =
                    new java.util.ArrayList<>(ft.parameters().size());
            for (TypeExpression.TypedParameter tp : ft.parameters()) {
                ps.add(new com.legend.compiler.element.type.Type.Param(
                        namedType(tp.type()),
                        com.legend.compiler.element.type.Multiplicity
                                .from(tp.multiplicity())));
            }
            return new Type.FunctionType(ps,
                    new com.legend.compiler.element.type.Type.Param(
                            namedType(ft.result().type()),
                            com.legend.compiler.element.type.Multiplicity
                                    .from(ft.result().multiplicity())));
        }
        // a relation-type literal in argument position (@TDS<(a:String[1])>)
        if (te instanceof TypeExpression.RelationType rt) {
            List<Type.Column> cols = new ArrayList<>(rt.columns().size());
            for (TypeExpression.Column c : rt.columns()) {
                cols.add(new Type.Column(c.name(), namedType(c.type()),
                        Multiplicity.from(c.multiplicity())));
            }
            return new Type.RelationType(cols);
        }
        if (!(te instanceof TypeExpression.NameRef nr)) {
            throw new TypeInferenceException(
                    "unsupported type annotation form: " + te.getClass().getSimpleName());
        }
        String name = nr.name();
        // the enclosing function's own type parameter (@T inside a
        // generic body) — a type VARIABLE, never a model lookup
        if (!typeParameterFrames.isEmpty() && typeParameterFrames.peek().contains(name)) {
            return new Type.TypeVar(name);
        }
        // The legacy TDS surface: a NOMINAL — the value level is the
        // relation carrier (CastChecker treats cast(@TabularDataSet) over
        // a relation as a schema-preserving assertion). The EXACT FQN wins
        // outright; the BARE name is a fallback AFTER user types (audit
        // 22b LOW: a model class named TabularDataSet must not be
        // shadowed — prelude-fallback ordering).
        if (com.legend.compiler.element.type.PlatformTypes.TABULAR_DATA_SET
                .equals(name)) {
            return new Type.GenericType(
                    com.legend.compiler.element.type.PlatformTypes.TABULAR_DATA_SET,
                    List.of());
        }
        return com.legend.compiler.element.type.PlatformTypes.eraseTdsRow(ctx.findType(name)
                .or(() -> "TabularDataSet".equals(name)
                        ? Optional.of((Type) new Type.GenericType(
                                com.legend.compiler.element.type.PlatformTypes
                                        .TABULAR_DATA_SET, List.of()))
                        : Optional.empty())
                .or(() -> name.contains("::")
                        ? Optional.empty()
                        : ctx.findType("meta::pure::metamodel::type::" + name)
                                .or(() -> ctx.findType(Pure.VARIANT_PKG + "::" + name)))
                .orElseThrow(() -> new TypeInferenceException(
                        "unknown type '" + name + "' in @" + name)));
    }

    /** {@code @Relation<(name:Type[m], …)>}: each column resolves recursively; multiplicity defaults to [1]. */
    Type.RelationType relationShapeType(TypeAnnotation.RelationShape rs) {
        List<Type.Column> cols = new ArrayList<>(rs.columns().size());
        for (TypeAnnotation.RelationShape.Column c : rs.columns()) {
            if (c.name() == null || c.type() instanceof TypeAnnotation.Wildcard) {
                throw new TypeInferenceException(
                        "wildcard columns in @Relation<(…)> are not implemented yet");
            }
            Multiplicity m = c.multiplicity() == null
                    ? Multiplicity.Bounded.ONE : Multiplicity.from(c.multiplicity());
            cols.add(new Type.Column(c.name(), annotationType(c.type()), m));
        }
        return new Type.RelationType(cols);
    }

}
