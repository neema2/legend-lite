package com.legend.compiler.element;

import com.legend.builtin.Pure;
import com.legend.compiler.ModelBuilder;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.parser.TypeExpression;
import com.legend.parser.element.ClassDefinition;
import com.legend.parser.element.EnumDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Phase F's shared type kernel (the {@code InferenceKernel} analog): everything
 * about turning <em>names</em> into <em>kinds</em> &mdash; the kind manifest
 * ({@link #findType}: primitive &rarr; class &rarr; enum by FQN existence, no
 * structure materialized) and {@link #classify}, the single
 * {@code TypeExpression → Type} boundary. Every per-kind compiler delegates
 * here; an unknown FQN throws (no fallback, AGENTS.md invariant 4).
 */
final class TypeClassifier {

    private final ModelBuilder model;

    TypeClassifier(ModelBuilder model) {
        this.model = model;
    }

    /** Kind classification by FQN — Knowledge, cheap; builds no structure. */
    Optional<Type> findType(String fqn) {
        Optional<Type.Primitive> prim = Type.Primitive.findByFqn(fqn);
        if (prim.isPresent()) {
            return Optional.of(prim.get());
        }
        // a PRECISE PRIMITIVE is its base primitive in the query path
        // (constraints are instantiation-time; see PrimitiveExtensionDefinition)
        Optional<Type.Primitive> ext = model.findPrimitiveExtension(fqn);
        if (ext.isPresent()) {
            return Optional.of(ext.get());
        }
        if (isClassFqn(fqn)) {
            return Optional.of(new Type.ClassType(fqn));
        }
        if (isEnumFqn(fqn)) {
            return Optional.of(new Type.EnumType(fqn));
        }
        return Optional.empty();
    }

    boolean isClassFqn(String fqn) {
        return model.findClass(fqn).isPresent() || Pure.findNativeClass(fqn).isPresent();
    }

    boolean isEnumFqn(String fqn) {
        return model.findEnum(fqn).isPresent() || Pure.findNativeEnum(fqn).isPresent();
    }

    /** The parser definition behind a class FQN — user model first, then the native catalog. */
    Optional<ClassDefinition> classDef(String fqn) {
        Optional<ClassDefinition> user = model.findClass(fqn);
        return user.isPresent() ? user : Pure.findNativeClass(fqn);
    }

    /** The parser definition behind an enum FQN — user model first, then the native catalog. */
    Optional<EnumDefinition> enumDef(String fqn) {
        Optional<EnumDefinition> user = model.findEnum(fqn);
        return user.isPresent() ? user : Pure.findNativeEnum(fqn);
    }

    /**
     * Classify a parser {@link TypeExpression} into a kinded {@link Type}. A bare
     * name is a {@link Type.TypeVar} if it is one of {@code typeParams}, else it is
     * classified via the kind manifest; an unknown FQN throws.
     */
    Type classify(TypeExpression te, List<String> typeParams) {
        return switch (te) {
            case TypeExpression.NameRef nr -> {
                if (typeParams.contains(nr.name())) {
                    yield new Type.TypeVar(nr.name());
                }
                yield findType(nr.name()).orElseThrow(() -> new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.MODEL, 
                        "Unknown type: '" + nr.name() + "' is not a known primitive, class, or enum"));
            }
            case TypeExpression.Generic g -> {
                List<Type> args = new ArrayList<>(g.arguments().size());
                for (TypeExpression arg : g.arguments()) {
                    args.add(classify(arg, typeParams));
                }
                yield new Type.GenericType(g.name(), args);
            }
            case TypeExpression.FunctionType ft -> {
                List<Type.Param> ps = new ArrayList<>(ft.parameters().size());
                for (TypeExpression.TypedParameter tp : ft.parameters()) {
                    ps.add(new Type.Param(classify(tp.type(), typeParams), multiplicity(tp.multiplicity())));
                }
                Type.Param result = new Type.Param(
                        classify(ft.result().type(), typeParams), multiplicity(ft.result().multiplicity()));
                yield new Type.FunctionType(ps, result);
            }
            case TypeExpression.RelationType rt -> {
                List<Type.Column> cols = new ArrayList<>(rt.columns().size());
                for (TypeExpression.Column c : rt.columns()) {
                    cols.add(new Type.Column(c.name(), classify(c.type(), typeParams), multiplicity(c.multiplicity())));
                }
                yield new Type.RelationType(cols);
            }
            case TypeExpression.SchemaAlgebra sa -> new Type.SchemaAlgebra(
                    classify(sa.left(), typeParams), op(sa.op()), classify(sa.right(), typeParams));
        };
    }

    /** FQN of a superclass / generic head reference. */
    static String headFqn(TypeExpression te) {
        return switch (te) {
            case TypeExpression.NameRef nr -> nr.name();
            case TypeExpression.Generic g -> g.name();
            // EXHAUSTIVE (no default): a head reference is nominal by
            // construction; the structural forms each say why they cannot be.
            case TypeExpression.FunctionType f -> throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.MODEL, 
                    "a function type cannot head a supertype/generic reference");
            case TypeExpression.RelationType r -> throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.MODEL, 
                    "a relation type cannot head a supertype/generic reference");
            case TypeExpression.SchemaAlgebra a -> throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.MODEL, 
                    "a schema-algebra expression cannot head a supertype/generic reference");
        };
    }

    static Multiplicity multiplicity(com.legend.parser.Multiplicity m) {
        return Multiplicity.from(m);
    }

    private static Type.Op op(TypeExpression.Op op) {
        return switch (op) {
            case EQUAL -> Type.Op.EQUAL;
            case UNION -> Type.Op.UNION;
            case DIFFERENCE -> Type.Op.DIFFERENCE;
            case SUBSET -> Type.Op.SUBSET;
        };
    }
}
