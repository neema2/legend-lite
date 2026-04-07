package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.GenericTypeInstance;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;
import com.gs.legend.model.SymbolTable;
import com.gs.legend.plan.GenericType;

import java.util.List;

/**
 * Signature-driven type checker for type conversion functions:
 * {@code toOne}, {@code toMany}, {@code to}, {@code toVariant}.
 *
 * <p>Each function follows the standard checker flow:
 * {@code resolveOverload → unify → resolveOutput}.
 *
 * <p>{@code cast} is handled by {@link CastChecker}.
 */
public class TypeConversionChecker extends AbstractChecker {

    public TypeConversionChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        String func = SymbolTable.extractSimpleName(af.function());
        return switch (func) {
            case "toOne" -> checkToOne(af, source, ctx);
            case "toMany" -> checkToMany(af, source, ctx);
            case "to" -> checkTo(af, source, ctx);
            case "toVariant" -> checkToVariant(af, source);
            default -> throw new PureCompileException(
                    "TypeConversionChecker: unknown function '" + func + "'");
        };
    }

    /**
     * {@code toOne<T>(values:T[*]):T[1]}
     * Narrows multiplicity from [*] to [1]. Type is preserved.
     */
    private TypeInfo checkToOne(AppliedFunction af, TypeInfo source,
                                TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        resolveOverload("toOne", params, source);

        if (params.size() > 1) env.compileExpr(params.get(1), ctx);

        return TypeInfo.builder()
                .expressionType(ExpressionType.one(source.type()))
                .build();
    }

    /**
     * {@code toMany<T,V>(source:T[0..1], type:V[0..1]):V[*]}
     * Widens to [*] with target type V from @Type argument.
     */
    private TypeInfo checkToMany(AppliedFunction af, TypeInfo source,
                                 TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        resolveOverload("toMany", params, source);

        env.compileExpr(params.get(1), ctx);

        GenericType targetType = ((GenericTypeInstance) params.get(1)).resolvedType();

        return TypeInfo.builder()
                .expressionType(ExpressionType.many(targetType))
                .build();
    }

    /**
     * {@code to<T,V>(source:T[0..1], type:V[0..1]):V[0..1]}
     * Nullable conversion with target type V from @Type argument.
     */
    private TypeInfo checkTo(AppliedFunction af, TypeInfo source,
                             TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        resolveOverload("to", params, source);

        env.compileExpr(params.get(1), ctx);

        GenericType targetType = ((GenericTypeInstance) params.get(1)).resolvedType();

        return TypeInfo.builder()
                .expressionType(ExpressionType.zeroOrOne(targetType))
                .build();
    }

    /**
     * {@code toVariant(source:Any[*]):Variant[1]}
     * Converts to Variant (JSON) with multiplicity [1].
     */
    private TypeInfo checkToVariant(AppliedFunction af, TypeInfo source) {
        resolveOverload("toVariant", af.parameters(), source);
        return TypeInfo.builder()
                .expressionType(ExpressionType.one(GenericType.Primitive.JSON))
                .build();
    }
}
