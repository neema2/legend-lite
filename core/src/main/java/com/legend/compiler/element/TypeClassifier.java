package com.legend.compiler.element;

import com.legend.builtin.Pure;
import com.legend.compiler.ModelBuilder;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.protocol.TypeExpression;
import com.legend.model.ClassDefinition;
import com.legend.model.EnumDefinition;

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
        // the m3 Enum METACLASS in a value position means "some enumeration
        // value" (EnumValueMapping.enum, Enum.name reads): an enumeration-
        // VALUE type, never a class navigation (InferenceKernel accepts any
        // enumeration or its name carrier against it)
        if (fqn.equals(com.legend.compiler.spec.InferenceKernel.ENUM_METACLASS_FQN)) {
            return Optional.of(new Type.EnumType(fqn));
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

    /**
     * The parser definition behind a class FQN — NATIVE catalog first
     * (the FunctionCompiler platform-owned rule, applied to classes): a
     * corpus re-declaration of a platform class (pureToSQLQuery_union
     * .pure's relation::Union) is the same source this prelude ports,
     * but parsed against parents the corpus doesn't carry, so its
     * inheritance chain dead-ends and subsumption breaks. User classes
     * live outside the native FQN set and resolve as before.
     */
    Optional<ClassDefinition> classDef(String fqn) {
        return model.knowledge().classDef(fqn);
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
                if ("?".equals(nr.name())) {
                    // m3's schema-algebra column-pattern wildcard
                    // ((?:?)⊆T, over.pure signatures) — the established
                    // convention is the anonymous type variable
                    // (InferenceKernel.UNKNOWN_COLUMN_TYPE)
                    yield new Type.TypeVar("?");
                }
                yield com.legend.compiler.element.type.PlatformTypes.eraseTdsRow(
                        findType(nr.name()).orElseThrow(() -> new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.MODEL,
                                "Unknown type: '" + nr.name() + "' is not a known primitive, class, or enum")));
            }
            case TypeExpression.Generic g -> {
                if (g.arguments().isEmpty() && !g.typeVariableValues().isEmpty()) {
                    // `val : P(8)[1]` — a primitive with its constraint VALUES
                    // (Primitive P(x:Integer[1]) extends Integer): the type is P
                    yield classify(new TypeExpression.NameRef(g.name(), g.pos()), typeParams);
                }
                List<Type> args = new ArrayList<>(g.arguments().size());
                for (TypeExpression arg : g.arguments()) {
                    args.add(classify(arg, typeParams));
                }
                // Result<T|m>-style MULTIPLICITY arguments (engine
                // parity, leg 2): a name spells a variable, digits and
                // * spell bounds — dropped before this leg, which
                // erased Result's m and typed every values read [*]
                List<com.legend.compiler.element.type.Multiplicity> margs =
                        new ArrayList<>(g.multiplicityArguments().size());
                for (String ma : g.multiplicityArguments()) {
                    margs.add(com.legend.compiler.element.type.Multiplicity.ofArgument(ma));
                }
                yield new Type.GenericType(g.name(), args, margs);
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

    static Multiplicity multiplicity(com.legend.protocol.Multiplicity m) {
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
