package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.TypeAnnotation;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.TypedCast;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.model.SymbolTable;
import com.gs.legend.model.m3.Type;

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

    public TypedCast check(AppliedFunction af, TypedSpec source,
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
     * {@code toOne<T>(values:T[*]):T[1]} — narrow [*] to [1], preserve type.
     * {@code targetType} on the cast is {@code null}: only multiplicity changes.
     */
    private TypedCast checkToOne(AppliedFunction af, TypedSpec source,
                                 TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        resolveOverload("toOne", params, source);
        // @Type arg (param[1]) is consumed structurally; compileExpr throws on
        // {@link TypeAnnotation} since the HIR refactor.
        return new TypedCast(source, null, ExpressionType.one(source.type()));
    }

    /**
     * {@code toMany<T,V>(source:T[0..1], type:V[0..1]):V[*]} — widen to [*] with
     * target type V from the {@code @Type} argument.
     */
    private TypedCast checkToMany(AppliedFunction af, TypedSpec source,
                                  TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        resolveOverload("toMany", params, source);
        Type targetType = ((TypeAnnotation) params.get(1)).resolve(env.modelContext());
        return new TypedCast(source, targetType, ExpressionType.many(targetType));
    }

    /**
     * {@code to<T,V>(source:T[0..1], type:V[0..1]):V[0..1]} — nullable conversion
     * with target type V.
     */
    private TypedCast checkTo(AppliedFunction af, TypedSpec source,
                              TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        resolveOverload("to", params, source);
        Type targetType = ((TypeAnnotation) params.get(1)).resolve(env.modelContext());
        return new TypedCast(source, targetType, ExpressionType.zeroOrOne(targetType));
    }

    /**
     * {@code toVariant(source:Any[*]):Variant[1]} — return type flows from the
     * native signature; {@code targetType} on the cast is set from the resolved
     * output type so downstream consumers have the Variant type in hand.
     */
    private TypedCast checkToVariant(AppliedFunction af, TypedSpec source) {
        NativeFunctionDef def = resolveOverload("toVariant", af.parameters(), source);
        var bindings = unify(def, source.expressionType());
        ExpressionType out = resolveOutput(def, bindings, "toVariant()");
        return new TypedCast(source, out.type(), out);
    }
}
