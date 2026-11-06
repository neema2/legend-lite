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
                        findType(nr.name()).orElseThrow(() -> unknownType(nr.name())));
            }
            case TypeExpression.Generic g -> {
                if (g.arguments().isEmpty() && !g.typeVariableValues().isEmpty()) {
                    // `val : P(8)[1]` — a primitive with its constraint VALUES
                    // (Primitive P(x:Integer[1]) extends Integer): the type is P
                    yield classify(new TypeExpression.NameRef(g.name(), g.pos()), typeParams);
                }
                // the head is a type that exists (its arguments' count is a declaration's: requireArguments)
                findType(g.name()).orElseThrow(() -> unknownType(g.name()));
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

    private static com.legend.error.ModelException unknownType(String name) {
        return new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.MODEL,
                "Unknown type: '" + name + "' is not a known primitive, class, or enum");
    }

    /**
     * A DECLARED type (a supertype, a property's, a parameter's, a return): every class it names is given as many
     * type and multiplicity arguments as it declares parameters (legend-pure's GenericTypeValidator), a type
     * parameter in scope naming none. Lite's own synthesized signatures name a mapped class bare and are not
     * declarations.
     */
    void requireArguments(TypeExpression te, List<String> typeParams) {
        switch (te) {
            case TypeExpression.NameRef nr -> {
                if (!typeParams.contains(nr.name())) {
                    requireTypeVariables(nr.name(), List.of());
                }
                // a bare Result is the engine's Result<Any|1..*>, its backward compatibility (DomainParseTreeWalker
                // rewrites a written `Result`; ProtocolReader reads it back bare; 2026-10-21, the independent audit:
                // lite refused it). Resolved, the written `Result` and a bare full path are one name here, so the
                // second is accepted too, where the engine counts its arguments (S95)
                if (!typeParams.contains(nr.name()) && isClassFqn(nr.name())
                        && !com.legend.compiler.element.type.PlatformTypes.RESULT.equals(nr.name())) {
                    requireArity(nr.name(), 0, 0);
                }
            }
            case TypeExpression.Generic g -> {
                requireTypeVariables(g.name(), g.typeVariableValues());
                if (g.arguments().isEmpty() && !g.typeVariableValues().isEmpty()) {
                    return;   // a precise primitive's values (P(8)), no type arguments
                }
                if (isClassFqn(g.name())) {
                    requireArity(g.name(), g.arguments().size(), g.multiplicityArguments().size());
                }
                for (TypeExpression arg : g.arguments()) {
                    requireArguments(arg, typeParams);
                }
            }
            case TypeExpression.FunctionType ft -> {
                for (TypeExpression.TypedParameter tp : ft.parameters()) {
                    requireArguments(tp.type(), typeParams);
                }
                requireArguments(ft.result().type(), typeParams);
            }
            case TypeExpression.RelationType rt -> {
                for (TypeExpression.Column c : rt.columns()) {
                    requireArguments(c.type(), typeParams);
                }
            }
            case TypeExpression.SchemaAlgebra sa -> {
                requireArguments(sa.left(), typeParams);
                requireArguments(sa.right(), typeParams);
            }
        }
    }

    /** A class named with {@code types} type arguments and {@code multiplicities} multiplicity arguments: as many
     *  of each as it declares parameters, or refused in legend-engine's words (CompileContext.newGenericType: "Wrong
     *  type arguments count (N) for type: C<T|m>", the class printed by legend-pure's _Class.print; 2026-10-21, the
     *  independent audit: legend-pure's GenericTypeValidator sentence was used, where the engine has its own). */
    /** One declared type variable of a primitive: its name, its type's name as the engine prints it, and that type
     *  when it is a primitive. */
    private record DeclaredVariable(String name, String typeName, Type.@com.legend.base.Nullable Primitive type) {
    }

    /**
     * A PRIMITIVE's type-variable values against its declaration, as legend-engine's compiler checks every generic
     * type it builds (CompileContext.newGenericType, in its words): as many values as the primitive declares variables
     * ({@code Decimal(1)}, {@code Numeric(10)} and a bare {@code Numeric} are refused), each an instance of its
     * variable's type ({@code Numeric('a', 2)} is refused). A name that is no primitive is not this check's: a class's
     * arguments are {@link #requireArity}'s. Lite took any values and dropped them until 2026-11-05 (DecimalKindsTest).
     */
    private void requireTypeVariables(String fqn, List<com.legend.protocol.spec.ValueSpecification> values) {
        List<DeclaredVariable> declared = new ArrayList<>();
        if (model.primitiveExtensionFqns().contains(fqn)) {
            for (com.legend.protocol.ParameterDefinition tv : model.primitiveTypeVariables(fqn)) {
                String head = headFqn(tv.type());
                Type.Primitive prim = findType(head).orElse(null) instanceof Type.Primitive p ? p : null;
                declared.add(new DeclaredVariable(tv.name(), com.legend.protocol.Protocol.splitFqn(head)[1], prim));
            }
        } else if (findType(fqn).orElse(null) instanceof Type.Primitive) {
            // a platform primitive: an m3 one (by its full path or its reserved bare name) declares none
            for (Type.Primitive.TypeVariable tv : Type.Primitive.typeVariables(fqn).orElseThrow(
                    () -> new IllegalStateException(fqn + " classifies as a platform primitive but is none"))) {
                declared.add(new DeclaredVariable(tv.name(), tv.type().typeName(), tv.type()));
            }
        } else {
            return;
        }
        String printed = com.legend.protocol.Protocol.splitFqn(fqn)[1] + (declared.isEmpty() ? ""
                : declared.stream().map(d -> d.name() + ":" + d.typeName())
                        .collect(java.util.stream.Collectors.joining(",", "(", ")")));
        if (declared.size() != values.size()) {
            throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.MODEL,
                    "Wrong type variables count (" + values.size() + ") for type: " + printed);
        }
        for (int i = 0; i < values.size(); i++) {
            DeclaredVariable d = declared.get(i);
            com.legend.protocol.spec.ValueSpecification v = values.get(i);
            Type.Primitive valueType = switch (v) {
                case com.legend.protocol.spec.CInteger ignored -> Type.Primitive.INTEGER;
                case com.legend.protocol.spec.CFloat ignored -> Type.Primitive.FLOAT;
                case com.legend.protocol.spec.CDecimal ignored -> Type.Primitive.DECIMAL;
                case com.legend.protocol.spec.CString ignored -> Type.Primitive.STRING;
                case com.legend.protocol.spec.CBoolean ignored -> Type.Primitive.BOOLEAN;
                default -> throw new com.legend.error.ModelException(
                        com.legend.error.LegendCompileException.Phase.MODEL,
                        "a type variable's value must be a literal: " + printed + ", variable '" + d.name() + "'");
            };
            boolean instance = d.type() != null && (d.type() == valueType
                    || (d.type() == Type.Primitive.NUMBER && valueType.isNumeric()));
            if (!instance) {
                String name = switch (v) {
                    case com.legend.protocol.spec.CInteger c -> String.valueOf(c.value());
                    case com.legend.protocol.spec.CFloat c -> String.valueOf(c.value());
                    case com.legend.protocol.spec.CDecimal c -> c.value().toPlainString();
                    case com.legend.protocol.spec.CString c -> c.value();
                    case com.legend.protocol.spec.CBoolean c -> String.valueOf(c.value());
                    default -> throw new IllegalStateException("a literal was classified above: " + v);
                };
                throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.MODEL,
                        "Error for type: " + printed + ", '" + name + "' is not an instance of " + d.typeName());
            }
        }
    }

    private void requireArity(String fqn, int types, int multiplicities) {
        ClassDefinition def = classDef(fqn).orElseThrow(() -> new IllegalStateException(
                fqn + " classifies as a class but has no definition"));
        if (def.typeParams().size() != types) {
            throw arityMismatch(def, "type", types);
        }
        if (def.multiplicityParams().size() != multiplicities) {
            throw arityMismatch(def, "multiplicity", multiplicities);
        }
    }

    private static com.legend.error.ModelException arityMismatch(ClassDefinition def, String kind, int got) {
        String params = def.typeParams().isEmpty() && def.multiplicityParams().isEmpty() ? ""
                : "<" + String.join(",", def.typeParams())
                        + (def.multiplicityParams().isEmpty() ? "" : "|" + String.join(",", def.multiplicityParams()))
                        + ">";
        return new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.MODEL,
                "Wrong " + kind + " arguments count (" + got + ") for type: "
                        + com.legend.protocol.Protocol.splitFqn(def.qualifiedName())[1] + params);
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
