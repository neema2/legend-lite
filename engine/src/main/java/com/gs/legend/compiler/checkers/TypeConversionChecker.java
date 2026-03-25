package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.GenericTypeInstance;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;
import com.gs.legend.model.m3.Multiplicity;
import com.gs.legend.plan.GenericType;

import java.util.List;


/**
 * Signature-driven type checker for type conversion functions:
 * {@code cast}, {@code toOne}, {@code toMany}, {@code to}, {@code toVariant}.
 *
 * <p>All functions are validated against their registered signatures in
 * {@link BuiltinFunctionRegistry}. The target type is resolved from the
 * {@code @Type} argument ({@link GenericTypeInstance}).
 *
 * <p>Key design decisions:
 * <ul>
 *   <li>{@code cast<T|m>(source:Any[m], type:T[1]):T[m]} — uses multiplicity
 *       variable {@code m} to preserve the source's multiplicity in the output</li>
 *   <li>{@code toOne<T>(source:T[*]):T[1]} — narrows multiplicity to [1]</li>
 *   <li>{@code toMany<T,V>(source:T[0..1], type:V[0..1]):V[*]} — widens to [*]</li>
 *   <li>{@code to<T,V>(source:T[*], type:V[0..1]):V[0..1]} — nullable conversion</li>
 *   <li>{@code toVariant(source:Any[*]):Variant[1]} — lossy variant conversion</li>
 * </ul>
 *
 * <p>Special handling for {@code cast}:
 * <ul>
 *   <li>Relational cast with declared schema → uses the target Relation type</li>
 *   <li>Relational cast without schema → propagates source type</li>
 *   <li>Scalar cast → uses target type with source multiplicity preserved via |m</li>
 * </ul>
 *
 * <p>Special handling for {@code toVariant}: the current implementation
 * passes through the source type for list detection in downstream compilation,
 * rather than returning the canonical {@code Variant[1]} from the signature.
 * This is a pragmatic concession for SQL codegen compatibility.
 */
public class TypeConversionChecker extends AbstractChecker {

    public TypeConversionChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        String func = TypeInfo.simpleName(af.function());
        List<ValueSpecification> params = af.parameters();

        // Compile all params (source + @Type arg)
        for (var p : params) {
            env.compileExpr(p, ctx);
        }

        // Resolve target type from @Type argument (GenericTypeInstance)
        GenericType targetType = resolveTargetType(params);

        // Look up source info from side table (compiled above)
        TypeInfo sourceInfo = !params.isEmpty() ? env.lookupCompiled(params.get(0)) : null;

        return switch (func) {
            case "cast" -> checkCast(af, params, sourceInfo, targetType);
            case "toOne" -> checkToOne(af, params, sourceInfo);
            case "toMany" -> checkToMany(af, targetType);
            case "to" -> checkTo(af, targetType);
            case "toVariant" -> checkToVariant(af, sourceInfo);
            default -> throw new PureCompileException(
                    "TypeConversionChecker: unknown function '" + func + "'");
        };
    }

    /**
     * cast<T|m>(source:Any[m], type:T[1]):T[m]
     *
     * <p>Preserves source multiplicity via mult var |m.
     * Special cases for relational cast (schema propagation).
     */
    private TypeInfo checkCast(AppliedFunction af, List<ValueSpecification> params,
                               TypeInfo sourceInfo, GenericType targetType) {
        // Resolve overload — cast has a single signature
        NativeFunctionDef def = resolveOverload("cast", params, sourceInfo);

        // Relational cast with declared schema: use target relation type
        if (targetType instanceof GenericType.Relation) {
            return TypeInfo.builder()
                    .expressionType(ExpressionType.one(targetType)).build();
        }

        // Relational cast without explicit schema: propagate source
        if (sourceInfo != null && sourceInfo.isRelational()) {
            return sourceInfo;
        }

        // Scalar cast: resolve multiplicity from source (mult var binding)
        if (targetType == null) {
            throw new PureCompileException(
                    "cast: cannot resolve target type — @Type argument required");
        }

        // Bind mult var 'm' from source expression's actual multiplicity
        Multiplicity sourceMult = (sourceInfo != null && sourceInfo.expressionType() != null)
                ? sourceInfo.expressionType().multiplicity()
                : Multiplicity.ONE;

        // If source is a list type (isList() = true), result is many of target type
        if (sourceInfo != null && sourceInfo.type() != null && sourceInfo.type().isList()) {
            sourceMult = Multiplicity.MANY;
        }

        return TypeInfo.builder()
                .expressionType(new ExpressionType(targetType, sourceMult))
                .build();
    }

    /**
     * toOne<T>(source:T[*]):T[1]
     *
     * <p>Narrows multiplicity to [1]. If source is a list type, unwraps
     * to the element type.
     */
    private TypeInfo checkToOne(AppliedFunction af, List<ValueSpecification> params,
                                TypeInfo sourceInfo) {
        // Resolve overload
        NativeFunctionDef def = resolveOverload("toOne", params, sourceInfo);

        if (sourceInfo == null || sourceInfo.type() == null) {
            throw new PureCompileException(
                    "toOne: cannot resolve source type");
        }

        // If source has list type (e.g., List<Integer>), return element type[1]
        if (sourceInfo.type().isList() && sourceInfo.type().elementType() != null) {
            return TypeInfo.builder()
                    .expressionType(ExpressionType.one(sourceInfo.type().elementType()))
                    .build();
        }

        // Non-list source: toOne asserts exactly one value → T[1]
        // Critical for [0..1] → [1] conversion (e.g., get()->to(@Integer)->toOne())
        return TypeInfo.builder()
                .expressionType(ExpressionType.one(sourceInfo.type()))
                .build();
    }

    /**
     * toMany<T,V>(source:T[0..1], type:V[0..1]):V[*]
     *
     * <p>Always produces multiple values of the target type.
     */
    private TypeInfo checkToMany(AppliedFunction af, GenericType targetType) {
        GenericType elemType = targetType != null ? targetType : GenericType.Primitive.ANY;
        return TypeInfo.builder()
                .expressionType(ExpressionType.many(elemType))
                .build();
    }

    /**
     * to<T,V>(source:T[*], type:V[0..1]):V[0..1]
     *
     * <p>Nullable conversion — returns target type with [0..1] multiplicity.
     */
    private TypeInfo checkTo(AppliedFunction af, GenericType targetType) {
        if (targetType == null) {
            throw new PureCompileException(
                    "to(): cannot resolve target type — @Type argument required");
        }
        return TypeInfo.builder()
                .expressionType(ExpressionType.zeroOrOne(targetType))
                .build();
    }

    /**
     * toVariant(source:Any[*]):Variant[1]
     *
     * <p>Pragmatic concession: passes through source type for list detection
     * in downstream SQL codegen, rather than returning canonical Variant[1].
     */
    private TypeInfo checkToVariant(AppliedFunction af, TypeInfo sourceInfo) {
        if (sourceInfo != null && sourceInfo.type() != null) {
            return sourceInfo;
        }
        throw new PureCompileException(
                "toVariant: cannot resolve source type");
    }

    /**
     * Resolves the target type from a {@code @Type} argument in the param list.
     */
    private GenericType resolveTargetType(List<ValueSpecification> params) {
        for (var p : params) {
            if (p instanceof GenericTypeInstance gti) {
                return gti.resolvedType();
            }
        }
        return null;
    }
}
