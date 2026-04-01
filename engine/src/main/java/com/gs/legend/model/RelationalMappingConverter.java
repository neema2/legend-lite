package com.gs.legend.model;

import com.gs.legend.ast.*;
import com.gs.legend.model.def.RelationalOperation;

import java.util.List;

/**
 * Converts {@link RelationalOperation} AST (from database filter definitions)
 * into {@link ValueSpecification} AST (for the compiler pipeline).
 *
 * <p>This is a model-layer transformation: it converts the relational definition
 * format into the Pure expression format that TypeChecker can compile.
 * Column references become {@code $row.COLUMN} property accesses.
 *
 * <p>Used by {@link com.gs.legend.compiler.MappingNormalizer} to convert
 * {@code ~filter} conditions into compilable expressions.
 */
public final class RelationalMappingConverter {

    private RelationalMappingConverter() {}

    /**
     * Converts a RelationalOperation to a ValueSpecification.
     *
     * @param op The relational operation to convert
     * @return The equivalent ValueSpecification
     * @throws IllegalArgumentException if the operation type is not supported
     */
    public static ValueSpecification convert(RelationalOperation op) {
        return switch (op) {
            case RelationalOperation.ColumnRef ref ->
                    new AppliedProperty(ref.column(), List.of(new Variable("row")));

            case RelationalOperation.Literal lit -> convertLiteral(lit);

            case RelationalOperation.Comparison cmp ->
                    new AppliedFunction(comparisonOp(cmp.op()),
                            List.of(convert(cmp.left()), convert(cmp.right())));

            case RelationalOperation.BooleanOp bool ->
                    new AppliedFunction(bool.op(),
                            List.of(convert(bool.left()), convert(bool.right())));

            case RelationalOperation.IsNull isNull ->
                    new AppliedFunction("isEmpty", List.of(convert(isNull.operand())));

            case RelationalOperation.IsNotNull isNotNull ->
                    new AppliedFunction("isNotEmpty", List.of(convert(isNotNull.operand())));

            case RelationalOperation.FunctionCall func ->
                    new AppliedFunction(func.name(),
                            func.args().stream().map(RelationalMappingConverter::convert).toList());

            case RelationalOperation.Group grp -> convert(grp.inner());

            default -> throw new IllegalArgumentException(
                    "Unsupported RelationalOperation in filter context: " + op.getClass().getSimpleName());
        };
    }

    private static ValueSpecification convertLiteral(RelationalOperation.Literal lit) {
        Object value = lit.value();
        if (value instanceof String s) return new CString(s);
        if (value instanceof Long l) return new CInteger(l);
        if (value instanceof Integer i) return new CInteger(i.longValue());
        if (value instanceof Double d) return new CFloat(d);
        throw new IllegalArgumentException("Unsupported literal type: " + value.getClass());
    }

    private static String comparisonOp(String op) {
        return switch (op) {
            case "=" -> "equal";
            case "!=" , "<>" -> "notEqual";
            case ">" -> "greaterThan";
            case "<" -> "lessThan";
            case ">=" -> "greaterThanEqual";
            case "<=" -> "lessThanEqual";
            default -> throw new IllegalArgumentException("Unsupported comparison operator: " + op);
        };
    }
}
